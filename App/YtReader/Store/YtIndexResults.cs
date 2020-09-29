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
      var taskGraph = TaskGraph.FromMethods(c => TopVideos(log), c => TopChannelVideos(log), c => ChannelStats(log));
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
    static readonly string[] VideoPeriodCols = {"period_type", "period_value"};

    [GraphTask(Name = TopVideosName)]
    async Task TopVideos(ILogger log) {
      var uniqPeriods = new Dictionary<string, JObject>(); // store unique periods to show in UI dynamically before loading data
      var (_, path) = await UpdateBlobIndex(log, TopVideosName, VideoPeriodCols, TopVideoResSql(1000, VideoPeriodCols),
        50.Kilobytes(), r => RecordPeriods(r, uniqPeriods));
      await SavePeriods(path, uniqPeriods, log);
    }

    const string TopChannelVideosName = "top_channel_videos";

    [GraphTask(Name = TopChannelVideosName)]
    async Task TopChannelVideos(ILogger log) {
      var cols = new[] {"channel_id"}.Concat(VideoPeriodCols).ToArray();
      var uniqPeriods = new Dictionary<string, JObject>(); // store unique periods to show in UI dynamically before loading data
      var (_, path) = await UpdateBlobIndex(log, TopChannelVideosName, cols, TopVideoResSql(50, cols),
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


    const string ChannelStatsName = "channel_stats";

    [GraphTask(Name = ChannelStatsName)]
    async Task ChannelStats(ILogger log) {
      var sql = $@"select channel_id
     , period_type
     , period_value::string period_value
     , sum(views) views
     , sum(watch_hours) watch_hours
from ttube_top_videos
group by channel_id, period_type, period_value
order by {VideoPeriodCols.Join(",")}";
      
      var uniqPeriods = new Dictionary<string, JObject>(); // store unique periods to show in UI dynamically before loading data
      var (_, path) = await UpdateBlobIndex(log, ChannelStatsName, VideoPeriodCols, sql,
        100.Kilobytes(), r => RecordPeriods(r, uniqPeriods));
      await SavePeriods(path, uniqPeriods, log);
    }

    async Task SavePeriods(StringPath path, IDictionary<string, JObject> uniqPeriods, ILogger log) {
      var stream = await uniqPeriods.Values.ToJsonlGzStream();
      await Store.Save(path.Add("periods.jsonl.gz"), stream, log);
    }

    static readonly string[] JVideoPeriodCols = CamelIndex(VideoPeriodCols);

    static void RecordPeriods(JObject r, IDictionary<string, JObject> uniqPeriods) {
      var s = r.JStringValues(JVideoPeriodCols).Join("|");
      if (!uniqPeriods.ContainsKey(s))
        uniqPeriods[s] = r.JCloneProps(JVideoPeriodCols);
    }
  }
}