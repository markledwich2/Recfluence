using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YtReader {
  public class YtDataUpdater {
    public YtDataUpdater(YtStore store, AppCfg cfg, ILogger log) {
      Yt = store;
      Cfg = cfg;
      Log = log;
    }

    AppCfg Cfg { get; }
    ILogger Log { get; }
    YtStore Yt { get; }

    public async Task UpdateData() {
      Log.Information("Starting incremental data update");

      var channelCfg = await Cfg.LoadChannelConfig();
      var seeds = channelCfg.Seeds.Randomize().ToList();
      var res = await seeds.BlockTransform(UpdateChannel, Cfg.Parallel, progressUpdate:
        p => Log.Verbose("Channel update progress {Channels}/{Total} {Speed}",
          p.Results.Count, seeds.Count, p.Speed("channels").Humanize())).WithDuration(); // sufficiently parallel inside

      Log.Information("Completed updates successfully in {Duration}. {Channels} channels, {Videos} videos.",
        res.Duration.Humanize(), res.Result.Count, res.Result.Sum(r => r.videos.Count));
    }

    async Task<(ChannelStored channel, ICollection<VideoStored> videos)> UpdateChannel(SeedChannel seed) {
      Log.Information("Updating {Channel} with new data", seed.Title);
      var channel = await Yt.GetAndUpdateChannel(seed.Id);
      var log = Log.ForContext("Channel", channel.Latest.Title);

      async Task<(VideoStored video, RecommendedVideoStored recommended)> UpdateVideo(ChannelVideoListItem fromV) {
        var video = await Yt.GetAndUpdateVideo(fromV.VideoId);
        if (video == null) return (null, null);
        var allRecommended = await Yt.GetAndUpdateRecommendedVideos(fromV);
        return (video, allRecommended);
      }

      var channelVideos = await Yt.GetAndUpdateChannelVideos(channel.Latest);
      var updateResults = await channelVideos.Vids.Where(v => !Yt.VideoDead(v))
        .BlockTransform(UpdateVideo, Cfg.Parallel, null,
          p => log.Verbose("{Channel} {Videos}/{Total} channel video's visited. {Speed}",
            channel.ChannelTitle, p.Results.Count, channelVideos.Vids.Count, p.NewItems.Count.Speed("videos", p.Elapsed).Humanize()));

      updateResults = updateResults.Where(r => r.video != null).ToList();
      log.Information("Completed {Channel} update", channel.ChannelTitle, updateResults.Count);

      return (channel, updateResults.Select(r => r.video).ToList());
    }

    public async Task RefreshMissingVideos() {
      var channelCfg = await Cfg.LoadChannelConfig();
      await channelCfg.Seeds.BlockAction(async c => {
        var channelVids = (await Yt.ChannelVideosCollection.Get(c.Id))?.Vids;
        if (channelVids == null) {
          Log.Error("{Channel}' has not video's stored", c.Title);
          return;
        }
        var missingVids = (await channelVids.Select(v => v).NotNull()
            .BlockTransform(async v => await Yt.Videos.Get(v.VideoId) == null ? v.VideoId : null, Cfg.Parallel))
          .NotNull().ToList();
        if (missingVids.Count > 0) {
          var videosUpdated = await missingVids.BlockTransform(async v => (Id: v, Vid: await Yt.GetAndUpdateVideo(v)));
          
          Log.Information("'{Channel}' Missing video's fixed: [{Fixed}] , [{Broken}]",
            c.Title,
            videosUpdated.Where(v => v.Vid != null).Join("|", v => v.Id),
            videosUpdated.Where(v => v.Vid == null).Join("|", v => v.Id));
        }
      });
    }
  }
}