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
      var (channel, isNew) = await Yt.GetAndUpdateChannel(seed.Id);
      var log = Log.ForContext("Channel", channel.Latest.Title);

      async Task<(VideoStored video, RecommendedVideoStored recommended)> UpdateVideo(ChannelVideoListItem fromV) {
        var vu = await Yt.GetAndUpdateVideo(fromV.VideoId);
        if (vu.Video == null) return (null, null);
        await Yt.GetAndUpdateVideoCaptions(seed.Id, vu.Video.VideoId, Log);
        var allRecommended = await Yt.GetAndUpdateRecommendedVideos(fromV);
        return (vu.Video, allRecommended);
      }

      var channelVideos = await Yt.GetAndUpdateChannelVideos(channel.Latest);
      var updateResults = await channelVideos.Vids.Where(v => isNew || !Yt.VideoDead(v))
        .BlockTransform(UpdateVideo, Cfg.Parallel, null,
          p => log.Verbose("{Channel} {Videos}/{Total} channel video's visited. {Speed}",
            channel.ChannelTitle, p.Results.Count, channelVideos.Vids.Count, p.NewItems.Count.Speed("videos", p.Elapsed).Humanize()));

      updateResults = updateResults.Where(r => r.video != null).ToList();
      log.Information("Completed {Channel} update of {Videos} videos", 
        channel.ChannelTitle, updateResults.Count);

      return (channel, updateResults.Select(r => r.video).ToList());
    }

    public async Task RefreshMissingVideos() {
      var channelCfg = await Cfg.LoadChannelConfig();
      await channelCfg.Seeds.Randomize().BlockAction(async c => {
        
        Log.Information("Started fixing '{Channel}'", c.Title);
        
        var channelVids = (await Yt.ChannelVideosCollection.Get(c.Id))?.Vids;
        if (channelVids == null) {
          Log.Error("{Channel}' has not video's stored", c.Title);
          return;
        }
        var missingVids = (await channelVids.Select(v => v).NotNull()
            .BlockTransform(async v => await Yt.Videos.Get(v.VideoId) == null ? v.VideoId : null, Cfg.ParallelCollect))
          .NotNull().ToList();
        if (missingVids.Count > 0) {
          var videosUpdated = await missingVids.BlockTransform(async v => (Id: v, Video: await Yt.GetAndUpdateVideo(v)), Cfg.Parallel);
          
          Log.Information("'{Channel}' Missing video's fixed [{Fixed}], broken [{Broken}]",
            c.Title,
            videosUpdated.Where(v => v.Video.Video != null).Join("|", v => v.Id),
            videosUpdated.Where(v => v.Video.Video == null).Join("|", v => v.Id));
        }

        var recommends = await channelVids.BlockTransform(async v => 
          (From:v, Recs:await Yt.RecommendedVideosCollection.Get(v.VideoId)), Cfg.ParallelCollect);
        var missinRecs = await recommends.Where(r => r.Recs == null).BlockTransform(async r => 
          (From:r.From, Recs: await Yt.GetAndUpdateRecommendedVideos(r.From)), Cfg.Parallel);
        Log.Information("'{Channel}' Missing video's recommendations [{Fixed}], broken [{Broken}]",
          c.Title,
          missinRecs.Where(v => v.Recs != null).Join("|", v => v.From.VideoId),
          missinRecs.Where(v => v.Recs == null).Join("|", v => v.From.VideoId));
      });
    }
  }
}