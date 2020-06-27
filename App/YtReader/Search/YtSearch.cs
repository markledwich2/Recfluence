using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Autofac.Util;
using Elasticsearch.Net;
using Humanizer;
using Mutuo.Etl.Db;
using Mutuo.Etl.Pipe;
using Nest;
using Polly;
using Polly.Retry;
using Semver;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using static YtReader.Search.SearchIndex;
using Policy = Polly.Policy;

namespace YtReader.Search {
  public enum SearchIndex {
    Channel,
    Caption,
    Video
  }

  public class YtSearch {
    readonly SnowflakeConnectionProvider Db;
    readonly ElasticClient               Es;

    public YtSearch(SnowflakeConnectionProvider db, ElasticClient es) {
      Db = db;
      Es = es;
    }

    [Pipe]
    public async Task SyncToElastic(ILogger log, bool fullLoad = false, long? limit = null,
      (SearchIndex index, string condition)[] conditions = null, CancellationToken cancel = default) {
      string[] Conditions(SearchIndex index) => conditions?.Where(c => c.index == index).Select(c => c.condition).ToArray();

      await BasicSync<EsChannel>(log, "select * from channel_latest", fullLoad, limit, Conditions(Channel), cancel);
      await BasicSync<EsVideo>(log, "select * from video_latest", fullLoad, limit, Conditions(Video), cancel);
      await SyncCaptions(log, fullLoad, limit, Conditions(Caption), cancel);
    }

    async Task BasicSync<T>(ILogger log, string selectSql, bool fullLoad, long? limit, string[] conditions = null, CancellationToken cancel = default)
      where T : class, IHasUpdated {
      var lastUpdate = await Es.MaxDateField<T>(m => m.Field(p => p.updated));
      var sql = CreateSql(selectSql, fullLoad, lastUpdate, limit, conditions);
      await UpdateIndex<T>(log, fullLoad);
      await BatchToEs<T>(log, sql, cancel);
    }

    async Task SyncCaptions(ILogger log, bool fullLoad, long? limit, string[] conditions, CancellationToken cancel) {
      await UpdateIndex<EsCaption>(log, fullLoad);
      var lastUpdate = await Es.MaxDateField<EsCaption>(m => m.Field(p => p.updated));
      var sql = CreateSql("select * from caption", fullLoad, lastUpdate, limit, conditions);
      using var conn = await OpenConnection(log);
      var allItems = Query<DbCaption>(sql, conn)
        .SelectMany(c => {
          // instead of searching across title, description, captions. We create new caption records for each part
          var caps = c.caption_group == 0
            ? new[] {
              c,
              CreatePart(c, CaptionPart.Title),
              CreatePart(c, CaptionPart.Description),
              CreatePart(c, CaptionPart.Keywords)
            }
            : new[] {c};
          foreach (var newCap in caps) {
            // even tho we use EsCaption  in the NEST api, it will still serialize and store instance properties. Remove the extra ones
            newCap.keywords = null;
            newCap.description = null;
          }
          return caps;
        });

      await BatchToEs<EsCaption>(log, allItems, EsExtensions.EsPolicy(log), cancel);

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

    async Task<LoggedConnection> OpenConnection(ILogger log) {
      var conn = await Db.OpenConnection(log);
      await conn.SetSessionParams((SfParam.ClientPrefetchThreads, 2));
      return conn;
    }

    (string sql, object param) CreateSql(string selectSql, bool fullLoad, DateTime? lastUpdate, long? limit, string[] conditions = null) {
      conditions ??= new string[] { };

      var param = lastUpdate == null || fullLoad ? null : new {max_updated = lastUpdate};
      if (param?.max_updated != null && conditions.IsEmpty())
        conditions = conditions.Concat("updated > :max_updated").ToArray();
      var sqlWhere = conditions.IsEmpty() ? "" : $" where {conditions.NotNull().Join(" and ")}";
      var sql = $"{selectSql}{sqlWhere}{SqlLimit(limit)}";
      return (sql, param);
    }

    async Task UpdateIndex<T>(ILogger log, bool fullLoad) where T : class {
      var index = Es.GetIndexFor<T>() ?? throw new InvalidOperationException("The ElasticClient must have default indexes created for types used");
      var exists = await Es.Indices.ExistsAsync(index);

      if (!exists.Exists) {
        await Es.Indices.CreateAsync(index, c => c.Map<T>(m => m.AutoMap()));
        log.Information("Created ElasticSearch Index {Index}", index);
      }
      else if (fullLoad) {
        await Es.MapAsync<T>(c => c.AutoMap());
        log.Information("Updated index mapping {Index}", index);
      }
    }

    static string SqlLimit(long? limit) => limit == null ? "" : $" limit {limit}";

    async Task BatchToEs<T>(ILogger log, (string sql, object param) sql, CancellationToken cancel) where T : class {
      var esPolicy = EsExtensions.EsPolicy(log);
      using var conn = await OpenConnection(log);
      await BatchToEs(log, Query<T>(sql, conn), esPolicy, cancel);
    }

    async Task BatchToEs<T>(ILogger log, IEnumerable<T> enumerable, AsyncRetryPolicy<BulkResponse> esPolicy, CancellationToken cancel) where T : class =>
      await enumerable
        .Batch(3_000).WithIndex()
        .BlockFunc(b => BatchToEs(b.item, b.index, esPolicy, log),
          parallel: 2, // 2 parallel, we don't get much improvements because its just one server/hard disk on the other end
          capacity: 2,
          cancel: cancel);

    async Task<int> BatchToEs<T>(IReadOnlyCollection<T> items, int i, AsyncRetryPolicy<BulkResponse> esPolicy, ILogger log) where T : class {
      var res = await esPolicy.ExecuteAsync(() => Es.IndexManyAsync(items));
      if (res.ItemsWithErrors.Any())
        log.Information("Indexed {Success}/{Total} documents to {Index} (batch {Batch}). Top 5 Error items: {@ItemsWithErrors}",
          res.Items.Count - res.ItemsWithErrors.Count(), items.Count, Es.GetIndexFor<T>(), i,
          res.ItemsWithErrors.Select(r => r.Error).Take(5));
      else
        log.Information("Indexed {Success}/{Total} documents to {Index} (batch {Batch})",
          res.Items.Count, items.Count, Es.GetIndexFor<T>(), i);

      return items.Count;
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

    public static ConnectionSettings ElasticConnectionSettings(this ElasticCfg cfg) {
      var esMappngTypes = typeof(EsIndex).Assembly.GetLoadableTypes()
        .Select(t => (t, es: t.GetCustomAttribute<ElasticsearchTypeAttribute>(), table: t.GetCustomAttribute<TableAttribute>()))
        .Where(t => t.es != null)
        .ToArray();
      if (esMappngTypes.Any(t => t.table == null))
        throw new InvalidOperationException("All document types must have a mapping to and index. Add a Table(\"Index name\") attribute.");
      var clrMap = esMappngTypes.Select(t => new ClrTypeMapping(t.t) {
        IdPropertyName = t.es.IdProperty,
        IndexName = IndexName(cfg, t.table.Name)
      });
      var cs = new ConnectionSettings(
        cfg.CloudId,
        new BasicAuthenticationCredentials(cfg.Creds.Name, cfg.Creds.Secret)
      ).DefaultMappingFor(clrMap);
      return cs;
    }

    public static string IndexPrefix(SemVersion version) => version.Prerelease;
    static string IndexName(ElasticCfg cfg, string table) => cfg.IndexPrefix.HasValue() ? $"{cfg.IndexPrefix}-{table}" : table;
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
    [Keyword] public              string      caption_id     { get; set; }
    public                        long        offset_seconds { get; set; }
    public                        long        caption_group  { get; set; }
    public                        string      caption        { get; set; }
    [Keyword] [StringEnum] public CaptionPart part           { get; set; }
  }

  /// <summary>Extra types on EsCaption that we don't want to store</summary>
  public class DbCaption : EsCaption {
    public string description { get; set; }
    public string keywords    { get; set; }
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