using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Humanizer;
using Humanizer.Bytes;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using IndexExpression = System.Linq.Expressions.Expression<System.Func<Serilog.ILogger, System.Threading.Tasks.Task<Mutuo.Etl.Blob.BlobIndexWork>>>;

namespace YtReader.Store {
  public class YtIndexResults {
    readonly SnowflakeConnectionProvider Sf;
    readonly BlobIndex                   BlobIndex;
    readonly ISimpleFileStore            Store;

    public YtIndexResults(YtStores stores, SnowflakeConnectionProvider sf) {
      Sf = sf;
      Store = stores.Store(DataStoreType.Results);
      BlobIndex = new BlobIndex(Store);
    }

    static (string Name, Func<ILogger, Task<BlobIndexWork>> Run) IndexTask(IndexExpression expression) {
      var run = expression.Compile();
      var m = expression.Body as MethodCallExpression ?? throw new InvalidOperationException("expected an expression that calls a method");
      var attribute = m.Method.GetCustomAttribute<GraphTaskAttribute>();
      return (attribute?.Name ?? m.Method.Name, run);
    }

    public async Task Run(IReadOnlyCollection<string> include, ILogger log, CancellationToken cancel = default) {
      var toRun = new IndexExpression[] {
        l => TopVideos(l, 20_000),
        l => TopChannelVideos(l, 50),
        l => ChannelStatsByPeriod(l),
        l => ChannelStatsById(l),
        l => VideosRemoved(l)
      }.Select(IndexTask).Where(t => include == null || include.Contains(t.Name));
      var (res, indexDuration) = await toRun.BlockFunc(async r => {
        var work = await r!.Run(log);
        return await BlobIndex.SaveIndexedJsonl(work, log, cancel);
      }, parallel: 4, cancel: cancel).WithDuration();
      log.Information("Completed writing indexes files {Indexes} in {Duration}. Starting commit.",
        res.Select(i => i.IndexFilesPath), indexDuration.HumanizeShort());
      if (cancel.IsCancellationRequested) return;
      await res.BlockAction(r => BlobIndex.CommitIndexJson(r, log), parallel: 10, cancel: cancel);
      log.Information("Committed indexes {Indexes}", res.Select(i => i.IndexPath));
    }

    public static string IndexVersion = "v2";

    async Task<BlobIndexWork> IndexWork(ILogger log, string name, IndexCol[] cols, string sql,
      ByteSize size, Action<JObject> onProcessed = null) {
      using var con = await Sf.OpenConnection(log);
      var rows = con.QueryBlocking<dynamic>(name, sql)
        .Select(d => (JObject) JObject.FromObject(d))
        .Select(j => j.ToCamelCase()).GetEnumerator();
      var path = StringPath.Relative("index", name, IndexVersion);
      return new BlobIndexWork(path, cols, rows, size, onProcessed);
    }

    const           string     TopVideosName = "top_videos";
    static readonly IndexCol[] PeriodCols    = new[] {"period"}.Select(c => CreateIndexCol(c, writeDistinct: true)).ToArray();

    static IndexCol CreateIndexCol(string dbName, bool inIndex = true, bool writeDistinct = false) => new IndexCol {
      Name = dbName.ToCamelCase(),
      DbName = dbName,
      InIndex = inIndex,
      WriteDistinct = writeDistinct
    };

    /// <summary>Top videos for all channels for a given time period</summary>
    [GraphTask(Name = TopVideosName)]
    Task<BlobIndexWork> TopVideos(ILogger log, int topPerPeriod) =>
      IndexWork(log, TopVideosName, PeriodCols, TopVideoResSql(rank: topPerPeriod, PeriodCols), 200.Kilobytes());

    const string TopChannelVideosName = "top_channel_videos";

    /// <summary>Top videos from a channel & time period</summary>
    [GraphTask(Name = TopChannelVideosName)]
    Task<BlobIndexWork> TopChannelVideos(ILogger log, int topPerChannel) {
      var cols = new[] {CreateIndexCol("channel_id")}.Concat(PeriodCols).ToArray();
      return IndexWork(log, TopChannelVideosName, cols, TopVideoResSql(topPerChannel, cols), 300.Kilobytes());
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

    const string ChannelStatsByPeriodName = "channel_stats_by_period";

    /// <summary>Aggregate stats for a channel at a given time period</summary>
    [GraphTask(Name = ChannelStatsByPeriodName)]
    Task<BlobIndexWork> ChannelStatsByPeriod(ILogger log) =>
      IndexWork(log, ChannelStatsByPeriodName, PeriodCols, ChannelStatsSql(PeriodCols), 100.Kilobytes());

    static readonly IndexCol[] ByChannelCols        = {CreateIndexCol("channel_id")};
    const           string     ChannelStatsByIdName = "channel_stats_by_id";

    /// <summary>Aggregate stats for a channel given a channel</summary>
    [GraphTask(Name = ChannelStatsByIdName)]
    Task<BlobIndexWork> ChannelStatsById(ILogger log) =>
      IndexWork(log, ChannelStatsByIdName, ByChannelCols, ChannelStatsSql(ByChannelCols), 50.Kilobytes());

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

    [GraphTask(Name = "video_removed")]
    Task<BlobIndexWork> VideosRemoved(ILogger log) =>
      IndexWork(
        log, "video_removed",
        new[] {CreateIndexCol("last_seen"), CreateIndexCol("error_type", inIndex: false, writeDistinct: true)},
        @"with
  video_errors as (
  select e.video_id
       , e.channel_id
       , e.channel_title
       , e.video_title
       , e.error_type
       , e.copyright_holder
       , l.updated last_seen
       , timediff(seconds, '0'::time, l.duration) duration_secs
       , l.views video_views
       , e.updated as error_updated
  from video_extra e
         inner join video_latest l on l.video_id=e.video_id
         inner join channel_accepted c on e.channel_id=c.channel_id
  where error_type is not null
    and error_type not in ('Restricted','Not available in USA','Paywall','Device','Unknown')
)
select * from video_errors
order by video_views desc", 100.Kilobytes());
  }
}