using System;
using System.Collections.Generic;
using System.Linq;
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

    public async Task Run(IReadOnlyCollection<string> include, ILogger log, CancellationToken cancel = default) {
      var taskGraph = TaskGraph.FromMethods(
        (l,c) => TopVideos(l), 
        (l,c) => TopChannelVideos(l), 
        (l,c) => ChannelStatsByPeriod(l), 
        (l,c) => ChannelStatsById(l));
      
      taskGraph.IgnoreNotIncluded(include);
      var (res, dur) = await taskGraph.Run(parallel: 4, log, cancel).WithDuration();
      var errors = res.Where(r => r.Error).ToArray();
      if (errors.Any())
        Log.Error("Index - failed in {Duration}: {@TaskResults}", dur.HumanizeShort(), res.Join("\n"));
      else
        Log.Information("Index - completed in {Duration}: {TaskResults}", dur.HumanizeShort(), res.Join("\n"));
    }

    async Task<(BlobIndexMeta index, StringPath path)> UpdateBlobIndex(ILogger log, string name, string[] indexCols, string sql,
      ByteSize size, Action<JObject> onProcessed = null) {
      using var con = await Sf.OpenConnection(log);
      var rows = con.QueryBlocking<dynamic>(name, sql)
        .Select(d => (JObject) JObject.FromObject(d))
        .Select(j => j.ToCamelCase()).GetEnumerator();

      var camelIndex = CamelIndex(indexCols);
      var path = StringPath.Relative("index", name);
      var index = await BlobIndex.SaveIndexedJsonl(path, rows, camelIndex, size, log, onProcessed);
      return (index, path);
    }

    static string[] CamelIndex(string[] indexCols) => indexCols.Select(i => i.ToCamelCase()).ToArray();

    const           string   TopVideosName   = "top_videos";
    static readonly string[] PeriodCols = {"period_type", "period_value"};

    
    /// <summary>
    /// Top videos for all channels for a given time period
    /// </summary>
    [GraphTask(Name = TopVideosName)]
    async Task TopVideos(ILogger log) {
      var uniqPeriods = new Dictionary<string, JObject>(); // store unique periods to show in UI dynamically before loading data
      var (_, path) = await UpdateBlobIndex(log, TopVideosName, PeriodCols, TopVideoResSql(rank: 20_000, PeriodCols),
        100.Kilobytes(), r => RecordPeriods(r, uniqPeriods));
      await SavePeriods(path, uniqPeriods, log);
    }

    const string TopChannelVideosName = "top_channel_videos";

    /// <summary>
    /// Top videos from a channel & time period
    /// </summary>
    [GraphTask(Name = TopChannelVideosName)]
    async Task TopChannelVideos(ILogger log) {
      var cols = new[] {"channel_id"}.Concat(PeriodCols).ToArray();
      var uniqPeriods = new Dictionary<string, JObject>(); // store unique periods to show in UI dynamically before loading data
      var (_, path) = await UpdateBlobIndex(log, TopChannelVideosName, cols, TopVideoResSql(rank: 50, cols),
        200.Kilobytes(), r => RecordPeriods(r, uniqPeriods));
      await SavePeriods(path, uniqPeriods, log);
    }

    string TopVideoResSql(int rank, string[] index) =>
      $@"select video_id
     , channel_id
     , period_type
     , period_value::string period_value
     , views
     , watch_hours
     , rank() over (partition by {index.Join(",")} order by views desc) rank
from ttube_top_videos
  qualify rank<{rank}
order by {index.Join(",")}, rank";


    const string ChannelStatsByPeriodName = "channel_stats_by_period";
    /// <summary>
    /// Aggregate stats for a channel at a given time period
    /// </summary>
    [GraphTask(Name = ChannelStatsByPeriodName)]
    async Task ChannelStatsByPeriod(ILogger log) {
      await UpdateBlobIndex(log, ChannelStatsByPeriodName, PeriodCols, ChannelStatsSql(PeriodCols), 100.Kilobytes());
    }
    
    static readonly string[] ByChannelCols        = { "channel_id" };
    const           string   ChannelStatsByIdName = "channel_stats_by_id";
    /// <summary>
    /// Aggregate stats for a channel given a channel
    /// </summary>
    [GraphTask(Name = ChannelStatsByIdName)]
    async Task ChannelStatsById(ILogger log) {
      await UpdateBlobIndex(log, ChannelStatsByIdName, ByChannelCols, ChannelStatsSql(ByChannelCols), 50.Kilobytes());
    }

    static string ChannelStatsSql(string[] orderCols) =>
      $@"with by_channel as (
  select t.channel_id
       , t.period_type
       , t.period_value::string period_value
       , sum(views) views
       , sum(watch_hours) watch_hours

  from ttube_top_videos t
  group by t.channel_id, t.period_type, t.period_value
)
select t.*
  , r.latest_refresh
  , r.videos
from by_channel t
       left join ttube_refresh_stats r on r.channel_id=t.channel_id and r.period_type=t.period_type and r.period_value=t.period_value
order by {orderCols.Join(",")}";

    async Task SavePeriods(StringPath path, IDictionary<string, JObject> uniqPeriods, ILogger log) {
      var stream = await uniqPeriods.Values.ToJsonlGzStream();
      await Store.Save(path.Add("periods.jsonl.gz"), stream, log);
    }

    static readonly string[] JVideoPeriodCols = CamelIndex(PeriodCols);

    static void RecordPeriods(JObject r, IDictionary<string, JObject> uniqPeriods) {
      var s = r.JStringValues(JVideoPeriodCols).Join("|");
      if (!uniqPeriods.ContainsKey(s))
        uniqPeriods[s] = r.JCloneProps(JVideoPeriodCols);
    }
  }
}