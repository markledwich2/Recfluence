using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Dapper;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json.Linq;
using Serilog;
using Serilog.Core;
using SysExtensions.Collections;
using SysExtensions.Threading;
using YtReader.SimpleCollect;
using static Mutuo.Etl.Pipe.PipeArg;
using static YtReader.Yt.CollectFromType;
using static YtReader.Yt.UpdateChannelType;
using static YtReader.Yt.CollectListPart;
using static YtReader.Yt.CollectPart;
using static YtReader.Yt.ExtraPart;
using static YtReader.Yt.YtCollectDb;

// ReSharper disable InconsistentNaming

namespace YtReader.Yt {
  public record CollectListOptions {
    public CollectListPart[]                    Parts         { get; init; }
    public ExtraPart[]                          ExtraParts    { get; set; }
    public (CollectFromType Type, string Value) CollectFrom   { get; init; }
    public string[]                             LimitChannels { get; init; }
    public TimeSpan                             StaleAgo      { get; init; }
    public JObject                              Args          { get; set; }
  }

  public enum CollectFromType {
    VideoPath,
    View,
    ChannelPath,
    Named
  }

  public enum CollectListPart {
    /// <summary>Channels explicitly listed</summary>
    [EnumMember(Value = "channel")] LChannel,
    /// <summary>Videos explicitly listed</summary>
    [EnumMember(Value = "video")] LVideo,

    /// <summary>Channels found from refreshing videos</summary>
    [EnumMember(Value = "discovered-channel")] [CollectPart(Explicit = true)]
    LDiscoveredChannel,

    /// <summary>Process videos in the channels found from the list (via video's or channels) where the most recent video_extra
    ///   is UpdateBefore</summary>
    [EnumMember(Value = "channel-video")] [CollectPart(Explicit = true)]
    LChannelVideo
  }

  public record VideoListStats(string video_id, string channel_id, DateTime? extra_updated, bool caption_exists, bool comment_exists);

  public record YtCollectList(YtCollector Col, IPipeCtx PipeCtx, AppCfg Cfg) {
    /// <summary>Collect extra & parts from an imported list of channels and or videos. Use instead of update to process
    ///   arbitrary lists and ad-hoc fixes</summary>
    public async Task Run(CollectListOptions opts, ILogger log, CancellationToken cancel = default) {
      var parts = opts.Parts;
      var extraParts = opts.ExtraParts;

      log.Information("YtCollect - Special Collect from {CollectFrom} started", opts.CollectFrom);

      // sometimes updates fail. When re-running this, we should refresh channels that are missing videos or have a portion of captions not attempted
      // NOTE: core warehouse table must be updated (not just staging tables) to take into account previously successful loads.
      var vidChanSelect = VideoChannelSelect(opts);
      var videosProcessed = Array.Empty<VideoProcessResult>();
      if (parts.ShouldRun(LVideo)) {
        IReadOnlyCollection<VideoListStats> videos;
        using (var db = await Col.Db(log)) // videos sans extra update
          videos = await VideoStats(db, vidChanSelect, opts.LimitChannels);
        videosProcessed = await videos
          .Process(PipeCtx, b => ProcessVideos(b, extraParts, Inject<ILogger>(), Inject<CancellationToken>()), log: log, cancel: cancel)
          .Then(r => r.Select(p => p.OutState).SelectMany().ToArray());
      }

      IReadOnlyCollection<string> channelIds = Array.Empty<string>();
      if (parts.ShouldRunAny(LChannelVideo, LChannel)) {
        // channels explicitly listed in the query
        using (var db = await Col.Db(log))
          channelIds = await ChannelIds(db, vidChanSelect, opts.LimitChannels);

        // channels found from processing videos
        if (parts.ShouldRun(LDiscoveredChannel))
          channelIds = channelIds.Concat(videosProcessed.Select(e => e.ChannelId)).NotNull().Distinct().ToArray();

        if (channelIds.Any()) {
          var channels = await Col.PlanAndUpdateChannelStats(new[] {PChannel, PChannelVideos}, channelIds, log, cancel)
            .Then(chans => chans
              .Where(c => c.LastVideoUpdate.OlderThanOrNull(opts.StaleAgo)) // filter to channels we haven't updated video's in recently
              .Select(c => c with {Update = c.Update == Discover ? Standard : c.Update})); // revert to standard update for channels detected as discover

          if (parts.ShouldRun(LChannelVideo)) {
            DateTime? fromDate = new DateTime(year: 2020, month: 1, day: 1);
            await channels.Randomize() // randomize to even the load
              .Process(PipeCtx,
                b => Col.ProcessChannels(b, extraParts, Inject<ILogger>(), Inject<CancellationToken>(), fromDate), log: log, cancel: cancel)
              .WithDuration();
          }
        }
      }
      log.Information("YtCollect - ProcessVideos complete - {Videos} videos and {Channels} channels processed",
        videosProcessed.Length, channelIds.Count);
    }

    public record VideoProcessResult(string VideoId, string ChannelId);

    [Pipe]
    public async Task<VideoProcessResult[]> ProcessVideos(IReadOnlyCollection<VideoListStats> videos, ExtraPart[] parts, ILogger log,
      CancellationToken cancel) {
      log ??= Logger.None;
      const int batchSize = 1000;
      var plans = new VideoExtraPlans();
      if (parts.ShouldRun(EExtra))
        plans.SetPart(videos.Select(v => v.video_id), EExtra);
      if (parts.ShouldRun(ECaption))
        plans.SetPart(videos.Where(v => v.channel_id != null && !v.caption_exists && v.video_id != null).Select(v => v.video_id), ECaption);
      if (parts.ShouldRun(EComment))
        plans.SetPart(videos.Where(v => !v.comment_exists).Select(v => v.video_id), EComment);

      var planBatches = plans.Batch(batchSize).ToArray();
      var extra = await planBatches
        .BlockMap(async (planBatch, i) => {
          var (e, _, _, _) = await Col.SaveExtraAndParts(c: null, parts, log, new(planBatch));
          log.Information("ProcessVideos - saved extra {Videos}/{TotalBatches} ", i * batchSize + e.Length, planBatches.Length);
          return e;
        }, Cfg.Collect.ParallelChannels, cancel: cancel)
        .SelectManyList();
      return extra.Select(e => new VideoProcessResult(e.VideoId, e.ChannelId)).ToArray();
    }

    static (string Sql, JObject Args) VideoChannelSelect(CollectListOptions opts) {
      var (type, value) = opts.CollectFrom;
      var select = type switch {
        ChannelPath => ($"select null video_id, $1::string channel_id from @public.yt_data/{value} (file_format => tsv)", null),
        VideoPath =>
          ($"select $1::string video_id, $2::string channel_id  from @public.yt_data/{value} (file_format => tsv)", null),
        View => ($"select video_id, channel_id from {value}", null),
        Named => CollectListSql.NamedQuery(value, opts.Args),
        _ => throw new($"CollectFrom {opts.CollectFrom} not supported")
      };
      return select;
    }

    static Task<IReadOnlyCollection<string>> ChannelIds(YtCollectDbCtx db, (string Sql, JObject Args) select, string[] channels = null) =>
      db.Db.Query<string>("distinct channels", $@"select distinct channel_id from ({select.Sql}) 
where channel_id is not null
{(channels.None() ? "" : $"and channel_id in ({SqlList(channels)})")}
", ToDapperArgs(select.Args));

    /// <summary>Find videos fromt he given select that are missing one of the required parts</summary>
    static Task<IReadOnlyCollection<VideoListStats>> VideoStats(YtCollectDbCtx db, (string Sql, dynamic Args) select,
      string[] channels = null) => db.Db.Query<VideoListStats>("collect list videos", @$"
with raw_vids as ({select.Sql})
, s as (
  select v.video_id
       , coalesce(v.channel_id, e.channel_id) channel_id
       , e.updated extra_updated
       , exists(select * from caption s where s.video_id=v.video_id) caption_exists
       , exists(select * from comment t where t.video_id=v.video_id) comment_exists
  from raw_vids v
         left join video_extra e on v.video_id=e.video_id
  where v.video_id is not null
)
select * from s
{(channels.None() ? "" : $"where channel_id in ({SqlList(channels)})")}
", ToDapperArgs(select.Args));

    static DynamicParameters ToDapperArgs(JObject args) {
      if (args == null) return null;
      var kvp = args.Properties().Select(p => new KeyValuePair<string, object>(p.Name, ((JValue) p.Value).Value));
      var p = new DynamicParameters(kvp);
      return p;
    }
  }
}