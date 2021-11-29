using Humanizer.Bytes;
using Mutuo.Etl.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using YtReader.Db;
using IndexExpression = System.Linq.Expressions.Expression<System.Func<YtReader.Store.WorkCfg>>;

namespace YtReader.Store; 

record WorkCfg(string Name, IndexCol[] Cols, string Sql, ByteSize? Size = null, string Version = null,
  NullValueHandling NullHandling = NullValueHandling.Include, string[] Tags = null);

public class YtIndexResults {
  public static string                      IndexVersion = "v2";
  readonly      BlobIndex                   BlobIndex;
  readonly      SnowflakeConnectionProvider Sf;

  public YtIndexResults(BlobStores stores, SnowflakeConnectionProvider sf) {
    Sf = sf;
    BlobIndex = new(stores.Store(DataStoreType.Results));
  }

  public async Task Run(string[] names, string[] tags, ILogger log, CancellationToken cancel = default) {
    var toRun = new[] {
        Narrative2Channels,
        Narrative2Videos,
        Narrative2Captions,
        UsFeed,
        UsRecs,
        UsWatch,
        VideoRemoved,
        VideoRemovedCaption,
        ChannelStatsById,
        ChannelStatsByPeriod,
        TopVideos(20_000),
        TopChannelVideos(50)
      }
      .Select(t => t with {Name = t.Name.Underscore()}) // we are building this for javascript land. So snake case everything
      .Where(t => names?.Contains(t.Name) != false && tags?.Intersect(t.Tags.NotNull()).Any() != false).ToArray();

    var (res, indexDuration) = await toRun.BlockMapList(async t => {
      var work = await IndexWork(log, t);

      return await BlobIndex.SaveIndexedJsonl(work, log, cancel);
    }, parallel: 4, cancel: cancel).WithDuration();

    log.Information("Completed writing indexes files {Indexes} in {Duration}. Starting commit.",
      res.Select(i => i.IndexFilesPath), indexDuration.HumanizeShort());

    if (cancel.IsCancellationRequested) return;

    await res.BlockDo(r => BlobIndex.CommitIndexJson(r, log), parallel: 10, cancel: cancel);
    log.Information("Committed indexes {Indexes}", res.Select(i => i.IndexPath));
  }

  async Task<BlobIndexWork> IndexWork(ILogger log, WorkCfg work, Action<JObject> onProcessed = null) {
    using var con = await Sf.Open(log);

    var reader = await con.ExecuteReader(work.Name, work.Sql);

    async IAsyncEnumerable<JObject> GetRows() {
      while (await reader.ReadAsync())
        yield return reader.ToSnowflakeJObject().ToCamelCase();
    }

    var path = SPath.Relative("index", work.Name, work.Version ?? IndexVersion);
    return new(path, work.Cols, GetRows(), work.Size ?? 200.Kilobytes(), work.NullHandling, onProcessed);
  }

  #region Channels & Videos

  static readonly IndexCol[] PeriodCols = new[] {"period"}.Select(c => Col(c, distinct: true)).ToArray();

  static IndexCol Col(string dbName, bool inIndex = true, bool distinct = false, bool minMax = false) {
    var meta = new ColMeta?[] {distinct ? ColMeta.Distinct : null, minMax ? ColMeta.MinMax : null}.NotNull().ToArray();
    return new() {
      Name = dbName.ToCamelCase(),
      DbName = dbName,
      InIndex = inIndex,
      ExtraMeta = meta
    };
  }

  /// <summary>Top videos for all channels for a given time period</summary>
  WorkCfg TopVideos(int topPerPeriod) => new(nameof(TopVideos), PeriodCols, TopVideoResSql(topPerPeriod, PeriodCols));

  /// <summary>Top videos from a channel & time period</summary>
  WorkCfg TopChannelVideos(int topPerChannel) {
    var cols = new[] {Col("channel_id")}.Concat(PeriodCols).ToArray();
    return new(nameof(TopChannelVideos), cols, TopVideoResSql(topPerChannel, cols), 300.Kilobytes());
  }

  string TopVideoResSql(int rank, IndexCol[] cols) {
    var indexColString = cols.DbNames().Join(",");
    return $@"with video_ex as (
  select video_id, video_title, upload_date, views as video_views, duration from video_latest
)
select t.video_id
     , video_title
     , channel_id
     , upload_date
     , timediff(seconds, '0'::time, v.duration) as duration_secs
     , concat(period_type, '|', period_value) period
     , views as period_views
     , video_views
     , watch_hours
     , rank() over (partition by {indexColString} order by period_views desc) rank
from ttube_top_videos t
left join video_ex v on v.video_id = t.video_id
  qualify rank<{rank}
order by {indexColString}, rank";
  }

  /// <summary>Aggregate stats for a channel at a given time period</summary>
  WorkCfg ChannelStatsByPeriod => new(nameof(ChannelStatsByPeriod), PeriodCols, ChannelStatsSql(PeriodCols), 100.Kilobytes());

  static readonly IndexCol[] ByChannelCols = {Col("channel_id")};

  /// <summary>Aggregate stats for a channel given a channel</summary>
  WorkCfg ChannelStatsById => new(nameof(ChannelStatsById), ByChannelCols, ChannelStatsSql(ByChannelCols), 50.Kilobytes());

  static string ChannelStatsSql(IndexCol[] orderCols) =>
    $@"with by_channel as (
  select t.channel_id
       , concat(t.period_type, '|', t.period_value) period
       , sum(views) views
       , sum(watch_hours) watch_hours
  from ttube_top_videos t
  group by t.channel_id, t.period_type, t.period_value
)
select t.*
  , r.latest_refresh
  , r.videos
from by_channel t
       left join ttube_refresh_stats r on r.channel_id=t.channel_id and concat(r.period_type, '|', r.period_value)=t.period
order by {orderCols.DbNames().Join(",")}";

  WorkCfg VideoRemoved =
    new(nameof(VideoRemoved), new[] {Col("last_seen", minMax: true), Col("error_type", inIndex: false, distinct: true)}, @"
select e.*
     , exists(select s.video_id from caption s where e.video_id=s.video_id) has_captions
from video_error e
join channel_accepted c on e.channel_id = c.channel_id
where e.platform = 'YouTube'
order by last_seen", 200.Kilobytes());

  WorkCfg VideoRemovedCaption = new(nameof(VideoRemovedCaption), new[] {Col("video_id")}, @"
select e.video_id, s.caption, s.offset_seconds
from video_error e
join caption s on e.video_id = s.video_id
join video_latest v on v.video_id = s.video_id
join channel_accepted c on c.channel_id = v.channel_id  
where v.platform = 'YouTube'
order by video_id, offset_seconds", 100.Kilobytes());

  #endregion

  #region Narrative

  const string Narrative2Version = "v2.3";

  static readonly IndexCol[] NarrativeChannelsCols = {Col("narrative", distinct: true)};

  readonly WorkCfg Narrative2Channels = new(nameof(Narrative2Channels), NarrativeChannelsCols, $@"
with by_channel as (
  select n.narrative, v.channel_id, sum(v.views) views
  from video_narrative2 n
         left join video_latest v on v.video_id=n.video_id
  group by 1,2
),
s as (
  select n.*
          , cl.channel_title
         , arrayExclude(cl.tags, array_construct('MissingLinkMedia', 'OrganizedReligion', 'Educational', 'Black', 'LGBT')) tags
         , cl.lr
         , logo_url
         , subs
         , substr(cl.description, 0, 301) description
          , cl.platform
  from by_channel n
           left join channel_latest cl on n.channel_id=cl.channel_id
)
select * from s order by {NarrativeChannelsCols.DbNames().Join(",")}",
    Tags: new[] {"narrative2"}, Version: Narrative2Version);

  static readonly IndexCol[] NarrativeVideoCols = {Col("narrative", distinct: true), Col("upload_date", minMax: true)};

  readonly WorkCfg Narrative2Videos = new(nameof(Narrative2Videos), NarrativeVideoCols, $@"
with s as (
  select n.narrative
       , n.mentions
       , n.keywords
       , n.tags
       , n.tags_meta
       , n.video_id
       , v.video_title
       , v.channel_id
       , v.views::int video_views
       , case narrative
           when '2020 Election Fraud' then
             case
               when array_contains('manual'::variant,n.tags_meta) then 1
               when array_contains('support'::variant,n.tags) then iff(v.upload_date<'2020-12-09',0.84/0.96,0.68/0.97)
               when array_contains('dispute'::variant,n.tags) then iff(v.upload_date<'2020-12-09',0.84/0.94,0.80/0.97)
               else 1
             end
           else null
         end*v.views::int video_views_adjusted
       , v.upload_date::date upload_date
       , ve.error_type
       , timediff(seconds,'0'::time,v.duration) duration_secs
       --, n.captions
       , ve.last_seen
      , e.thumb
  from video_narrative2 n
         left join video_latest v on n.video_id=v.video_id
         left join video_extra e on e.video_id=v.video_id
         left join video_error ve on ve.video_id=n.video_id
)
select *
from s
order by {NarrativeVideoCols.DbNames().Join(",")}, video_views desc",
    200.Kilobytes(), // bigish because some charts need to load alot of these
    Narrative2Version,
    NullValueHandling.Ignore,
    new[] {"narrative2"});

  static readonly IndexCol[] Narrative2CaptionCols = {Col("narrative"), Col("upload_date")};

  WorkCfg Narrative2Captions = new(nameof(Narrative2Captions), Narrative2CaptionCols, @$"
select narrative, v.upload_date::date upload_date, n.video_id, v.channel_id, n.captions
from video_narrative2 n
left join video_latest v on v.video_id = n.video_id
order by {Narrative2CaptionCols.DbNames().Join(",")}",
    50.Kilobytes(), // small because the UI loads these on demand
    Narrative2Version,
    Tags: new[] {"narrative2"});

  #endregion

  #region Recs

  static readonly IndexCol[] UsRecCols = {
    Col("label", distinct: true),
    Col("from_channel_id", distinct: true)
  };

  WorkCfg UsRecs = new(nameof(UsRecs), UsRecCols, @$"
with video_date_accounts as (
  with g as (
    select from_video_id, day, count(distinct account) accounts_total --, count(*) over (partition by from_video_id) days_viewed
    from (
      select from_video_id, updated::date day, account
      from us_rec
      group by 1, 2, 3
      having max(rank)>5 -- at least x videos per account
    )
    group by 1, 2
    having accounts_total>=12 -- at least x accounts watched the same vid
  )
  select * --, row_number() over (partition by from_video_id order by days_viewed desc nulls last) days_viewed_rank
  from g
)
  , full_account_recs as (
  select r.account
       , r.updated::date day
       , coalesce(m.label, 'Other') label
       , r.from_video_id
       , r.to_video_id
       , r.from_channel_id
       , r.from_channel_title
       , r.from_video_title
       , r.to_video_title
       , r.to_channel_id
       , r.to_channel_title
       , d.accounts_total
  from us_rec r
         left join us_test_manual m on m.video_id=r.from_video_id
         inner join video_date_accounts d on d.from_video_id=r.from_video_id and d.day=r.updated::date
  where account<>'Black'
)
  , sets as (
  select from_video_id
       , to_video_id
       , day
       , label
       , array_agg(distinct account) accounts
       , any_value(from_channel_id) from_channel_id
       , any_value(from_video_title) from_video_title
       , any_value(to_video_title) to_video_title
       , any_value(to_channel_id) to_channel_id
       , any_value(to_channel_title) to_channel_title
  from full_account_recs r
  group by 1, 2, 3, 4
)
select *
from sets
order by {UsRecCols.DbNames().Join(",")}", 100.Kilobytes(), Tags: new[] {"us"});

  static readonly IndexCol[] VideoSeenCols = {Col("part"), Col("account", distinct: true)};

  static string GetVideoSeen(string table, bool titleInSeen = false) =>
    $@"
with s1 as (
  select w.account
       , w.video_id
       --, any_value(w.video_title) as video_title
       , any_value({(titleInSeen ? "w" : "vl")}.video_title) as video_title
       , any_value(vl.channel_id) as channel_id
       , any_value(vl.channel_title) as channel_title
       , min(w.updated) first_seen
       , max(w.updated) last_seen
       , count(*) as seen_total
  from {table} w
         left join video_latest vl on w.video_id=vl.video_id
  where account<>'Black'
  group by 1, 2
)
select *
     , iff(row_number() over (partition by account order by seen_total desc)<100, 'featured', null) part
      , percent_rank() over (partition by account order by seen_total) percentile
from s1
order by {VideoSeenCols.DbNames().Join(",")}, percentile desc";

  WorkCfg UsWatch = new(nameof(UsWatch), VideoSeenCols, GetVideoSeen("us_watch"), 100.Kilobytes(), Tags: new[] {"us"});
  WorkCfg UsFeed  = new(nameof(UsFeed), VideoSeenCols, GetVideoSeen("us_feed", titleInSeen: true), 100.Kilobytes(), Tags: new[] {"us"});

  #endregion
}