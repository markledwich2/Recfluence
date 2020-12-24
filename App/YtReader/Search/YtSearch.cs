using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
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
using Snowflake.Data.Client;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using static YtReader.Search.IndexType;
using Policy = Polly.Policy;

// ReSharper disable InconsistentNaming

namespace YtReader.Search {
  public class YtSearch {
    readonly SnowflakeConnectionProvider Db;
    readonly ElasticClient               Es;
    readonly SearchCfg                   Cfg;

    public YtSearch(SnowflakeConnectionProvider db, ElasticClient es, SearchCfg cfg) {
      Db = db;
      Es = es;
      Cfg = cfg;
    }

    [Pipe]
    public async Task SyncToElastic(ILogger log, bool fullLoad = false,
      string[] indexes = null,
      (string index, string condition)[] conditions = null, CancellationToken cancel = default) {
      string[] Conditions(IndexType index) => conditions?.Where(c => c.index == index.BaseName()).Select(c => c.condition).ToArray() ?? new string[] { };
      bool ShouldRun(IndexType index) => indexes == null || indexes.Any(i => string.Equals(i, index.BaseName(), StringComparison.OrdinalIgnoreCase));

      async Task Sync<TDb, TEs>(IndexType type, string sql, Func<TDb, TEs> map, params string[] extraConditions)
        where TDb : class where TEs : class, IHasUpdated {
        TEs Map(TDb o) {
          try {
            return map(o);
          }
          catch (Exception ex) {
            log.Warning(ex, "Search - Error when mapping db row: {row}", o);
            throw;
          }
        }

        if (ShouldRun(type))
          await CoreSync<TDb, TEs>(log, sql, fullLoad, Map, Conditions(type).Concat(extraConditions).ToArray(), cancel);
      }

      await Sync<dynamic, EsChannel>(Channel, @"select * from channel_latest", MapChannel, "source='recfluence'");

      await Sync(ChannelTitle, "select channel_id, channel_title, description, updated from channel_latest",
        (EsChannelTitle c) => c);

      await Sync<dynamic, EsVideo>(Video, @"select l.*, c.lr, c.tags, timediff(seconds, '0'::time, duration) as duration_secs
from video_latest l
inner join channel_accepted c on l.channel_id = c.channel_id", MapVideo);

      await Sync(Caption, "select * from caption_es", (DbEsCaption c) => MapCaption(c));
    }

    async Task CoreSync<TDb, TEs>(ILogger log, string selectSql, bool fullLoad, Func<TDb, TEs> map, string[] conditions = null,
      CancellationToken cancel = default)
      where TEs : class, IHasUpdated where TDb : class {
      var lastUpdate = await Es.MaxDateField<TEs>(m => m.Field(p => p.updated));
      var sql = CreateSql(selectSql, fullLoad, lastUpdate, conditions);

      var alias = Es.GetIndexFor<TEs>() ?? throw new InvalidOperationException("The ElasticClient must have default indexes created for types used");
      var existingIndex = (await Es.GetIndicesPointingToAliasAsync(alias)).FirstOrDefault();
      var existingIndexCheck = (await Es.Indices.GetAsync(alias)).Indices.FirstOrDefault();
      if (existingIndexCheck.Key == alias) throw new InvalidOperationException($"Existing index with the alias {alias}. Not supported for update");
      // full load into random indexes and use aliases. This is to avoid downtime on full loads
      var newIndex = fullLoad || existingIndex == null || lastUpdate == null ? $"{alias}-{ShortGuid.Create(5).ToLower()}" : null;

      if (newIndex != null) {
        await Es.Indices.CreateAsync(newIndex, c => c.Map<TEs>(m => m.AutoMap()));
        log.Information("Search - Created new ElasticSearch Index {Index} ({Alias})", newIndex, alias);
      }

      using var conn = await OpenConnection(log);
      var rows = Query<TDb>(sql, conn).Select(map);
      var (docs, dur) = await BatchToEs(newIndex ?? existingIndex, log, rows, EsExtensions.EsPolicy(log), cancel).WithDuration();
      if (newIndex != null) {
        if (existingIndex != null) {
          (await Es.Indices.BulkAliasAsync(b =>
              b.Remove(r => r.Index(existingIndex).Alias(alias))
                .Add(a => a.Index(newIndex).Alias(alias))))
            .EnsureValid("switching index aliases");
          (await Es.Indices.DeleteAsync(existingIndex, ct: cancel)).EnsureValid("deleting index");
          log.Information("Search - switched {Old} with {New} index ({Alias})", existingIndex, newIndex, alias);
        }
        else {
          (await Es.Indices.PutAliasAsync(new PutAliasDescriptor(newIndex, alias))).EnsureValid("creating alias");
          log.Debug("Search - {New} index now has alias ({Alias})", newIndex, alias);
        }
      }
      log.Information("Search - completed indexing {Docs} docs to {Alias} in {Duration}", docs, alias, dur.HumanizeShort());
    }

    static EsVideo MapVideo(dynamic v) => new() {
      channel_id = v.CHANNEL_ID,
      channel_title = v.CHANNEL_TITLE,
      description = v.DESCRIPTION,
      dislikes = v.DISLIKES,
      error_type = v.ERROR_TYPE,
      likes = v.LIKES,
      lr = v.LR,
      tags = v.TAGS == null ? new string[] { } : JsonExtensions.ToObject<string[]>(v.TAGS),
      updated = v.UPDATED,
      upload_date = v.UPLOAD_DATE,
      video_id = v.VIDEO_ID,
      video_title = v.VIDEO_TITLE,
      views = v.VIEWS,
      keywords = v.KEYWORDS == null ? new string[] { } : JsonExtensions.ToObject<string[]>(v.KEYWORDS),
      duration_secs = v.DURATION_SECS
    };

    static EsChannel MapChannel(dynamic c) => new() {
      channel_id = c.CHANNEL_ID,
      channel_title = c.CHANNEL_TITLE,
      age = c.AGE,
      avg_minutes = c.AVG_MINUTES,
      channel_lifetime_daily_views = c.CHANNEL_LIFETIME_DAILY_VIEWS,
      channel_lifetime_daily_views_relevant = c.CHANNEL_LIFETIME_DAILY_VIEWS_RELEVANT,
      channel_video_views = c.CHANNEL_VIDEO_VIEWS,
      channel_views = c.CHANNEL_VIEWS,
      country = c.COUNTRY,
      day_range = c.DAY_RANGE,
      description = c.DESCRIPTION,
      from_date = c.FROM_DATE,
      ideology = c.IDEOLOGY,
      logo_url = c.LOGO_URL,
      lr = c.LR,
      main_channel_id = c.MAIN_CHANNEL_ID,
      main_channel_title = c.MAIN_CHANNEL_TITLE,
      media = c.MEDIA,
      relevance = c.RELEVANCE,
      reviews_human = c.REVIEWS_HUMAN,
      status_msg = c.STATUS_MSG,
      subs = c.SUBS,
      tags = c.TAGS == null ? new string[] { } : JsonExtensions.ToObject<string[]>(c.TAGS),
      to_date = c.TO_DATE,
      updated = c.UPDATED,
    };

    static EsCaption MapCaption(DbEsCaption c) => new() {
      caption_id = c.caption_id,
      caption = c.caption,
      channel_id = c.caption_id,
      channel_title = c.channel_title,
      lr = c.lr,
      offset_seconds = c.offset_seconds,
      part = c.part,
      tags = c.tags.ToObject<string[]>(),
      updated = c.updated,
      upload_date = c.upload_date,
      video_id = c.video_id,
      video_title = c.video_title,
      views = c.views,
    };

    async Task<ILoggedConnection<SnowflakeDbConnection>> OpenConnection(ILogger log) {
      var conn = await Db.OpenConnection(log);
      await conn.SetSessionParams(
        (SfParam.ClientPrefetchThreads, 2),
        (SfParam.Timezone, "GMT"));
      return conn;
    }

    (string sql, object param) CreateSql(string selectSql, bool fullLoad, DateTime? lastUpdate, string[] conditions = null) {
      conditions ??= new string[] { };

      var param = lastUpdate == null || fullLoad ? null : new {max_updated = lastUpdate};
      if (param?.max_updated != null)
        conditions = conditions.Concat("updated > :max_updated").ToArray();
      var sqlWhere = conditions.IsEmpty() ? "" : $" where {conditions.NotNull().Join(" and ")}";
      var sql = $@"with q as ({selectSql})
select * from q{sqlWhere}
order by updated"; // always order by updated so that if sync fails, we can resume where we left of safely.
      return (sql, param);
    }

    async Task<int> BatchToEs<T>(string indexName, ILogger log, IEnumerable<T> enumerable, AsyncRetryPolicy<BulkResponse> esPolicy, CancellationToken cancel)
      where T : class => (await enumerable
      .Batch(Cfg.BatchSize).WithIndex()
      .BlockFunc(b => BatchToEs(indexName, b.item, b.index, esPolicy, log),
        parallel: Cfg.Parallel, // 2 parallel, we don't get much improvements because its just one server/hard disk on the other end
        capacity: Cfg.Parallel,
        cancel: cancel)).Sum();

    async Task<int> BatchToEs<T>(string indexName, IReadOnlyCollection<T> items, int i, AsyncRetryPolicy<BulkResponse> esPolicy, ILogger log) where T : class {
      var res = await esPolicy.ExecuteAsync(() => Es.IndexManyAsync(items, indexName));

      if (!res.IsValid) {
        log.Error(
          "Error indexing. Indexed {Success}/{Total} documents to {Index} (batch {Batch}). Server error: {@ServerError} {@OriginalException}. Top 5 item errors: {@ItemsWithErrors}",
          res.Items.Count - res.ItemsWithErrors.Count(), items.Count, Es.GetIndexFor<T>(), i,
          res.ServerError, res.OriginalException, res.ItemsWithErrors.Select(r => $"{r.Error} ({r.Status})").Take(5));
        throw new InvalidOperationException("Error indexing documents. Best to stop now so the documents are contiguous.");
      }

      log.Information("Indexed {Success}/{Total} documents to {Index} (batch {Batch})",
        res.Items.Count, items.Count, Es.GetIndexFor<T>(), i);

      return items.Count;
    }

    IEnumerable<T> Query<T>((string sql, object param) sql, ILoggedConnection<IDbConnection> conn) where T : class =>
      conn.QueryBlocking<T>(nameof(SyncToElastic), sql.sql, sql.param, buffered: false);
  }

  public static class EsExtensions {
    public static ElasticClient Client(this ElasticCfg cfg, string defaultIndex) => new ElasticClient(new ConnectionSettings(
        cfg.CloudId,
        new BasicAuthenticationCredentials(cfg.Creds.Name, cfg.Creds.Secret))
      .DefaultIndex(defaultIndex));

    public static AsyncRetryPolicy<BulkResponse> EsPolicy(ILogger log) => Policy
      .HandleResult<BulkResponse>(r =>
        r.ItemsWithErrors.Any(i => i.Status == 429) || r.ServerError?.Status == 429 || r.OriginalException is OperationCanceledException)
      .RetryAsync(retryCount: 3, async (r, i) => {
        var delay = i.ExponentialBackoff(5.Seconds());
        log?.Information("Retryable error indexing captions: Retrying in {Duration}, attempt {Attempt}/{Total}",
          delay, i, 3);
        await Task.Delay(delay);
      });

    public static async Task<DateTime?> MaxDateField<T>(this ElasticClient es, Func<MaxAggregationDescriptor<T>, IMaxAggregation> selector)
      where T : class, IHasUpdated {
      var res = await es.SearchAsync<T>(c => c
        .Aggregations(a => a.Max("max", selector))
      );
      var val = res.Aggregations.Max("max")?.ValueAsString?.ParseDate(style: DateTimeStyles.RoundtripKind);
      return val;
    }

    public static string GetIndexFor<T>(this ElasticClient es) => es.ConnectionSettings.DefaultIndices.TryGetValue(typeof(T), out var i) ? i : null;

    public static void EnsureValid(this ResponseBase res, string verb) {
      if (!res.IsValid)
        throw new InvalidOperationException($"error when {verb}", res.OriginalException);
    }
  }

  public enum IndexType {
    [EnumMember(Value = "video")]         Video,
    [EnumMember(Value = "caption")]       Caption,
    [EnumMember(Value = "channel")]       Channel,
    [EnumMember(Value = "channel_title")] ChannelTitle
  }

  public static class EsIndex {
    public const string Version = "2";
    public static string BaseName(this IndexType type) => type.EnumString();
    public static string IndexName(this IndexType type) => $"{type.BaseName()}-{Version}";

    public static ConnectionSettings ElasticConnectionSettings(this ElasticCfg cfg) {
      var esMappngTypes = typeof(EsIndex).Assembly.GetLoadableTypes()
        .Select(t => (t, es: t.GetCustomAttribute<ElasticsearchTypeAttribute>(), table: t.GetCustomAttribute<YtEsTableAttribute>()))
        .Where(t => t.es != null)
        .ToArray();
      if (esMappngTypes.Any(t => t.table == null))
        throw new InvalidOperationException("All document types must have a mapping to and index. Add a Table(\"Index name\") attribute.");
      var clrMap = esMappngTypes.Select(t => new ClrTypeMapping(t.t) {
        IdPropertyName = t.es.IdProperty,
        IndexName = IndexName(cfg, t.table.Index)
      });
      var cs = new ConnectionSettings(
        cfg.CloudId,
        new BasicAuthenticationCredentials(cfg.Creds.Name, cfg.Creds.Secret)
      ).DefaultMappingFor(clrMap);
      return cs;
    }

    public static string IndexPrefix(SemVersion version) => version.Prerelease;
    static string IndexName(ElasticCfg cfg, IndexType table) => new[] {cfg.IndexPrefix, table.IndexName()}.Where(p => p.HasValue()).Join("-");
  }

  public interface IHasUpdated {
    DateTime updated { get; }
  }

  public class DbEsCaption {
    public                     string      caption_id     { get; set; }
    public                     string      video_id       { get; set; }
    public                     string      channel_id     { get; set; }
    public                     string      video_title    { get; set; }
    public                     string      channel_title  { get; set; }
    public                     DateTime    upload_date    { get; set; }
    [Date(Format = "")] public DateTime    updated        { get; set; }
    public                     long        views          { get; set; }
    public                     string      lr             { get; set; }
    public                     string      tags           { get; set; }
    public                     long        offset_seconds { get; set; }
    public                     string      caption        { get; set; }
    public                     CaptionPart part           { get; set; }
  }

  public abstract class VideoCaptionCommon : IHasUpdated {
    [Keyword] public string   video_id      { get; set; }
    [Keyword] public string   channel_id    { get; set; }
    public           string   video_title   { get; set; }
    public           string   channel_title { get; set; }
    public           DateTime upload_date   { get; set; }
    public           DateTime updated       { get; set; }
    public           long?    views         { get; set; }
    [Keyword] public string   lr            { get; set; }
    [Keyword] public string[] tags          { get; set; }
  }

  [ElasticsearchType(IdProperty = nameof(video_id))]
  [YtEsTableAttribute(Video)]
  public class EsVideo : VideoCaptionCommon {
    public           string   description   { get; set; }
    public           string[] keywords      { get; set; }
    public           long?    likes         { get; set; }
    public           long?    dislikes      { get; set; }
    [Keyword] public string   error_type    { get; set; }
    public           long?    duration_secs { get; set; }
  }

  [ElasticsearchType(IdProperty = nameof(caption_id))]
  [YtEsTableAttribute(Caption)]
  public class EsCaption : VideoCaptionCommon {
    [Keyword] public              string      caption_id     { get; set; }
    public                        long        offset_seconds { get; set; }
    public                        string      caption        { get; set; }
    [Keyword] [StringEnum] public CaptionPart part           { get; set; }
  }

  [ElasticsearchType(IdProperty = nameof(channel_id))]
  [YtEsTableAttribute(Channel)]
  public class EsChannel : EsChannelTitle, IHasUpdated {
    [Keyword] public string    main_channel_id                       { get; set; }
    [Keyword] public string    logo_url                              { get; set; }
    public           double?   relevance                             { get; set; }
    public           long?     subs                                  { get; set; }
    public           long?     channel_views                         { get; set; }
    [Keyword] public string    country                               { get; set; }
    [Keyword] public string    status_msg                            { get; set; }
    public           decimal?  channel_video_views                   { get; set; }
    public           DateTime? from_date                             { get; set; }
    public           DateTime? to_date                               { get; set; }
    public           long?     day_range                             { get; set; }
    public           decimal?  channel_lifetime_daily_views          { get; set; }
    public           decimal?  avg_minutes                           { get; set; }
    public           decimal?  channel_lifetime_daily_views_relevant { get; set; }
    public           string    main_channel_title                    { get; set; }
    public           long?     age                                   { get; set; }
    [Keyword] public string[]  tags                                  { get; set; }
    [Keyword] public string    lr                                    { get; set; }
    [Keyword] public string    ideology                              { get; set; }
    [Keyword] public string    media                                 { get; set; }
    public           long?     reviews_human                         { get; set; }
  }

  [ElasticsearchType(IdProperty = nameof(channel_id))]
  [YtEsTableAttribute(ChannelTitle)]
  public class EsChannelTitle : IHasUpdated {
    [Keyword] public string   channel_id    { get; set; }
    public           string   channel_title { get; set; }
    public           string   description   { get; set; }
    public           DateTime updated       { get; set; }
  }

  public class YtEsTableAttribute : Attribute {
    public YtEsTableAttribute(IndexType index) => Index = index;
    public IndexType Index { get; set; }
  }

  public enum CaptionPart {
    Caption,
    Title,
    Description,
    Keywords
  }
}