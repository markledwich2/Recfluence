using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Elasticsearch.Net;
using Humanizer;
using Mutuo.Etl.Db;
using Mutuo.Etl.Pipe;
using Nest;
using Polly;
using Polly.Retry;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using Policy = Polly.Policy;

namespace YtReader.Search {
  public class YtSearch {
    readonly ConnectionProvider         Db;
    readonly ElasticClient Es;
    readonly ILogger       Log;

    public YtSearch(ConnectionProvider db, ElasticClient es, ILogger log) {
      Db = db;
      Es = es;
      Log = log;
    }

    [Pipe]
    public async Task SyncToElastic(bool fullLoad = false, long? limit = null) {
      await BasicSync<EsChannel>("select * from channel_latest", fullLoad, limit);
      await BasicSync<EsVideo>("select * from video_latest", fullLoad, limit);
      await SyncCaptions(fullLoad, limit);
    }

    async Task BasicSync<T>(string selectSql, bool fullLoad, long? limit, string[] conditions = null) where T : class, IHasUpdated {
      var lastUpdate = await Es.MaxDateField<T>(m => m.Field(p => p.updated));
      var sql = CreateSql(selectSql, fullLoad, lastUpdate, limit, conditions);
      await UpdateIndex<T>(fullLoad);
      await BatchToEs<T>(sql);
    }
    
    async Task SyncCaptions(bool fullLoad, long? limit, string[] conditions = null) {
      await UpdateIndex<EsCaption>(fullLoad);
      var lastUpdate = await Es.MaxDateField<EsCaption>(m => m.Field(p => p.updated));
      var sql = CreateSql("select * from caption", fullLoad, lastUpdate, limit, conditions);
      using var conn = await Db.OpenLoggedConnection(Log);
      var allItems = Query<DbCaption>(sql, conn).SelectMany(c => {
        // instead of searching across title, description, captions. We create new caption records for each part
        var caps = (c.caption_group == 0) ? new[] {
            c,
            CreatePart(c, CaptionPart.Title),
            CreatePart(c, CaptionPart.Description),
            CreatePart(c, CaptionPart.Keywords)
          } : new[] {c};
        foreach (var newCap in caps) {
          // even tho we use EsCaption  in the NEST api, it will still serialize and store instance properties. Remove the extra ones
          newCap.keywords = null;
          newCap.description = null;
        }
        return caps;
      });

      await BatchToEs<EsCaption>(allItems);
      
      DbCaption CreatePart(DbCaption c, CaptionPart part) {
        var partCaption = c.JsonClone();
        partCaption.caption_id = $"{c.video_id}|{part.ToString().ToLowerInvariant()}";
        partCaption.caption = part switch {
          CaptionPart.Title => c.video_title,
          CaptionPart.Description => c.description,
          CaptionPart.Keywords => c.keywords,
          _ => throw new NotImplementedException($"can't create part `{part}`")
        };
        partCaption.part = part;
        return partCaption;
      }
    }

    (string sql, object param) CreateSql(string selectSql, bool fullLoad, DateTime? lastUpdate, long? limit, string[] conditions = null) {
      conditions ??= new string[] { };

      var param = lastUpdate == null || fullLoad ? null : new {max_updated = lastUpdate};
      if (param?.max_updated != null)
        conditions = conditions.Concat(" updated > :max_updated").ToArray();
      var sqlWhere = conditions.IsEmpty() ? "" : $" where {conditions.NotNull().Join(" and ")}";
      var sql = $"{selectSql}{sqlWhere}{SqlLimit(limit)}";
      return (sql, param);
    }

    async Task UpdateIndex<T>(bool fullLoad) where T : class {
      var index = Es.GetIndexFor<T>() ?? throw new InvalidOperationException("The ElasticClient must have default indexes created for types used");
      var exists = await Es.Indices.ExistsAsync(index);

      if (fullLoad && exists.Exists) {
        await Es.Indices.DeleteAsync(index);
        Log.Information("Deleted index {Index} (full load)", index);
      }

      if (fullLoad || !exists.Exists) {
        await Es.Indices.CreateAsync(index, c => c.Map<T>(m => m.AutoMap()));
        Log.Information("Created ElasticSearch Index {Index}", index);
      }
    }

    static string SqlLimit(long? limit) => limit == null ? "" : $" limit {limit}";

    async Task BatchToEs<T>((string sql, object param) sql) where T : class {
      using (var conn = await Db.OpenLoggedConnection(Log)) {
        var allItems = Query<T>(sql, conn);
        await BatchToEs(allItems);
      }
    }

    async Task BatchToEs<T>(IEnumerable<T> allItems) where T : class {
      var batchedItems = allItems.Batch(10000).WithIndex();
      var esPolicy = EsExtensions.EsPolicy(Log);
      var docCount = 0;
      await batchedItems.BlockAction(async b => {
        var (item, i) = b;
        var res = await esPolicy.ExecuteAsync(() => Es.IndexManyAsync(item));
        Interlocked.Add(ref docCount, res.Items.Count);
        if (res.ItemsWithErrors.Any())
          Log.Information("Indexed {Success}/{Total} documents to {Index} (batch {Batch}). Top 5 Error items: {@ItemsWithErrors}",
            res.Items.Count - res.ItemsWithErrors.Count(), item.Count, Es.GetIndexFor<T>(), i,
            res.ItemsWithErrors.Select(r => r.Error).Take(5));
        else
          Log.Information("Indexed {Success}/{Total} documents to {Index} (batch {Batch})",
            res.Items.Count, item.Count, Es.GetIndexFor<T>(), i);
      }, 3, 8);

      Log.Information("Completed indexed {Documents} documents to {Index}", docCount, Es.GetIndexFor<T>());
    }

    IEnumerable<T> Query<T>((string sql, object param) sql, LoggedConnection conn) where T : class => 
      conn.QueryBlocking<T>(nameof(SyncToElastic), sql.sql, sql.param, buffered: false);
  }

  public static class EsExtensions {
    public static ElasticClient Client(this ElasticCfg cfg, string defaultIndex) => new ElasticClient(new ConnectionSettings(
        cfg.CloudId,
        new BasicAuthenticationCredentials(cfg.Creds.Name, cfg.Creds.Secret))
      .DefaultIndex(defaultIndex));

    public static AsyncRetryPolicy<BulkResponse> EsPolicy(ILogger log) => Policy
      .HandleResult<BulkResponse>(r => r.ItemsWithErrors.Any(i => i.Status == 403))
      .RetryAsync(3, async (r, i) => {
        var delay = i.ExponentialBackoff(1.Seconds());
        log?.Debug("Retryable error indexing captions: Retrying in {Duration}, attempt {Attempt}/{Total}",
          delay, i, 3);
        await Task.Delay(delay);
      });

    public static async Task<DateTime?> MaxDateField<T>(this ElasticClient es, Func<MaxAggregationDescriptor<T>, IMaxAggregation> selector)
      where T : class, IHasUpdated {
      var res = await es.SearchAsync<T>(c => c
        .Aggregations(a => a.Max("max", selector))
      );
      var val = res.Aggregations.Max("max")?.ValueAsString?.ParseDate();
      return val;
    }

    public static string GetIndexFor<T>(this ElasticClient es) => es.ConnectionSettings.DefaultIndices.TryGetValue(typeof(T), out var i) ? i : null;
  }

  public static class EsIndex {
    public const string Video   = "video";
    public const string Caption = "caption2";
    public const string Channel = "channel";
  }

  public interface IHasUpdated {
    DateTime updated { get; }
  }

  public abstract class VideoCaptionCommon : IHasUpdated {
    [Keyword] public string video_id      { get; set; }
    [Keyword] public string channel_id    { get; set; }
    public           string video_title   { get; set; }
    public           string channel_title { get; set; }
    [Keyword] public string thumb_high    { get; set; }

    public DateTime upload_date { get; set; }
    public DateTime updated     { get; set; }

    public long views { get; set; }

    [Keyword] public string ideology { get; set; }
    [Keyword] public string lr       { get; set; }
  }

  [ElasticsearchType(IdProperty = nameof(video_id))]
  [Table(EsIndex.Video)]
  public class EsVideo : VideoCaptionCommon {
    public string description { get; set; }
    public string KeyWords    { get; set; }

    public           long    likes            { get; set; }
    public           long    dislikes         { get; set; }
    [Keyword] public string  error_type       { get; set; }
    [Keyword] public string  copyright_holder { get; set; }
    public           double? pcd_ads          { get; set; }

    [Keyword] public string media   { get; set; }
    [Keyword] public string country { get; set; }
  }

  [ElasticsearchType(IdProperty = nameof(caption_id))]
  [Table(EsIndex.Caption)]
  public class EsCaption : VideoCaptionCommon {
    [Keyword] public string      caption_id     { get; set; }
    public           long        offset_seconds { get; set; }
    public           long        caption_group  { get; set; }
    public           string      caption        { get; set; }
    [Keyword, StringEnum] public           CaptionPart part           { get; set; }
  }

  /// <summary>Extra types on EsCaption that we don't want to store</summary>
  public class DbCaption : EsCaption {
    public string description { get; set; }
    public string keywords { get; set; }
  }

  [ElasticsearchType(IdProperty = nameof(channel_id))]
  [Table(EsIndex.Channel)]
  public class EsChannel : IHasUpdated {
    [Keyword] public string   channel_id                            { get; set; }
    public           string   channel_title                         { get; set; }
    [Keyword] public string   main_channel_id                       { get; set; }
    public           string   channel_decription                    { get; set; }
    [Keyword] public string   logo_url                              { get; set; }
    public           double   relevance                             { get; set; }
    public           long     subs                                  { get; set; }
    public           double   channel_views                         { get; set; }
    [Keyword] public string   country                               { get; set; }
    public           DateTime updated                               { get; set; }
    [Keyword] public string   status_msg                            { get; set; }
    public           double   channel_video_views                   { get; set; }
    public           DateTime from_date                             { get; set; }
    public           DateTime to_date                               { get; set; }
    public           long     day_range                             { get; set; }
    public           double   channel_lifetime_daily_views          { get; set; }
    public           double   avg_minutes                           { get; set; }
    public           double   channel_lifetime_daily_views_relevant { get; set; }
    public           string   main_channel_title                    { get; set; }
    public           long     age                                   { get; set; }
    public           string   tags                                  { get; set; }
    [Keyword] public string   lr                                    { get; set; }
    [Keyword] public string   ideology                              { get; set; }
    [Keyword] public string   media                                 { get; set; }
    [Keyword] public string   manoel                                { get; set; }
    [Keyword] public string   ain                                   { get; set; }
  }

  public enum CaptionPart {
    Caption,
    Title,
    Description,
    Keywords
  }
}