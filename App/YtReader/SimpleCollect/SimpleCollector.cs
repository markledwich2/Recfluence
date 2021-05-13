using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Mutuo.Etl.Pipe;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.BitChute;
using YtReader.Db;
using YtReader.Rumble;
using YtReader.Store;
using YtReader.Yt;
using static YtReader.SimpleCollect.StandardCollectPart;
using static YtReader.Store.ChannelSourceType;
using ChanById = SysExtensions.Collections.IKeyedCollection<string, YtReader.Store.Channel>;
using static Mutuo.Etl.Pipe.PipeArg;
using static Newtonsoft.Json.Linq.MergeNullValueHandling;
using static YtReader.Yt.ExtraPart;

namespace YtReader.SimpleCollect {
  /// <summary>Collector for sources that are simple enough to use the same basic planning and execution of a scrape</summary>
  public record SimpleCollector(SnowflakeConnectionProvider Sf, RumbleWeb Rumble, BcWeb BitChute, YtStore Store, IPipeCtx PipeCtx) {
    public IScraper Scraper(Platform platform) => platform switch {
      Platform.Rumble => Rumble,
      Platform.BitChute => BitChute,
      _ => throw new($"Platform {platform} not supported")
    };

    public async ValueTask<SimpleCollectPlan> PlanSimpleCollect(SimpleCollectOptions options, ILogger log, CancellationToken cancel) {
      var plan = new SimpleCollectPlan(options);
      var platform = options.Platform;
      var parts = options.Parts;
      var scraper = Scraper(platform);

      if (plan.ExplicitVideos != null)
        return plan;

      using var dbCtx = await DbCtx(platform, log);

      plan = plan with {Existing = (await dbCtx.ExistingChannels()).ById()};

      void WithUp(string desc, IReadOnlyCollection<Channel> channels) => plan = plan.WithForUpdate(desc, channels, log);

      if (plan.Parts.ShouldRun(ExistingChannel)) // ad existing channels limit to explicit
        WithUp("existing", plan.Existing.Where(c => c.ForUpdate(plan.ExplicitChannels)).ToArray());

      // add explicit channel, no need to lookup existing, because that will already be in the list
      WithUp("explicit",
        plan.ExplicitChannels.NotNull().Select(c => scraper.NewChan(c) with {DiscoverSource = new(Manual, c)}).ToArray());

      if (!parts.ShouldRunAny(Discover, DiscoverFromVideo) || plan.ExplicitChannels?.Any() == true) return plan;
      var discovered = await dbCtx.DiscoverChannelsAndVideos();
      if (parts.ShouldRun(Discover))
        WithUp("discovered", discovered.Where(d => d.LinkType == LinkType.Channel)
          .Select(selector: l => scraper.NewChan(l.LinkId) with {DiscoverSource = l.ToDiscoverSource()}).ToArray());

      if (!parts.ShouldRun(DiscoverFromVideo)) return plan;

      plan = plan with {
        VideosToCrawl = discovered.Where(d => d.LinkType == LinkType.Video)
          .Select(l => new DiscoverSource(VideoLink, l.LinkId, l.FromPlatform)).ToArray()
      };
      log.Information("Collect {Platform} - planned {Videos} videos for crawl", platform, plan.VideosToCrawl.Count);
      return plan;
    }

    /// <summary>Scrape videos from the plan, save to the store, and append new channels to the plan</summary>
    public async ValueTask<SimpleCollectPlan> CrawlVideoLinksToFindNewChannels(SimpleCollectPlan plan, ILogger log) {
      var scraper = Scraper(plan.Platform);
      var crawledVideos = await plan.VideosToCrawl.BlockMap(async (discover, i) => {
        var video = await scraper.Video(discover.LinkId, log)
          .Swallow(e => log.Warning(e, "Collect {Platform} - error crawling video {Video}: {Error}", plan.Platform, discover.LinkId, e.Message));
        log.Debug("Collect {Platform} - crawled video {VideoId} {Vid}/{Total}", plan.Platform, video?.VideoId, i, plan.VideosToCrawl.Count);
        return (discover, video);
      }, scraper.CollectParallel).ToListAsync();

      await Store.VideoExtra.Append(crawledVideos.Select(r => r.video).NotNull().ToArray());
      log.Information("Collect {Platform} - saved {Videos} videos", plan.Platform, crawledVideos.Count);

      var crawledChannels = crawledVideos.Where(v => v.video?.ChannelId.HasValue() == true)
        .Select(v => new Channel(plan.Platform, v.video.ChannelId, v.video.ChannelSourceId) {
          ChannelTitle = v.video.ChannelTitle,
          DiscoverSource = v.discover
        }).Distinct().ToArray();

      return plan.WithForUpdate("video crawled channels", crawledChannels, log);
    }

    public async ValueTask<SimpleCollectPlan> CollectChannelAndVideos(SimpleCollectPlan plan, ILogger log, CancellationToken cancel) {
      var scraper = Scraper(plan.Platform);
      if (plan.ExplicitVideos != null) {
        var videos = await plan.ExplicitVideos.BlockMap(v => scraper.Video(v, log)).ToListAsync();
        await Store.Videos.Append(videos);
      }

      await plan.ToUpdate.Process(PipeCtx,
        b => SimpleCollectChannels(b, plan.Platform, plan.Options, Inject<ILogger>(), Inject<CancellationToken>()), 
        log: log, cancel: cancel);
      
      return plan;
    }

    public async Task<CollectDbCtx> DbCtx(Platform platform, ILogger log) => new(await Sf.Open(log), platform);

    [Pipe]
    async Task<int> SimpleCollectChannels(IReadOnlyCollection<Channel> channels, Platform platform, SimpleCollectOptions options, ILogger log, CancellationToken cancel) {
      using var dbCtx = await DbCtx(platform, log);
      //var channelById = channels.ToKeyedCollection(c => c.c.ChannelId);
      var videoPlanById = await dbCtx.VideoPlans(channels.ToArray())
        .Then(p => p.GroupBy(v => v.ChannelId).ToDictionary(v => v.Key, v => v.ToArray()));
      var channelPlans = channels.Select(c => {
        var videPlansSource = videoPlanById.TryGet(c.ChannelId);
        var videoPlans = new VideoExtraPlans();
        if (videPlansSource == null) return new {videoPlans, c};
        foreach (var v in videPlansSource) {
          videoPlans.SetForUpdate(v);
          if (v.ExtraUpdated == null)
            videoPlans.SetPart(v.VideoId, EExtra);
        }
        return new { videoPlans, c };
      }).ToArray();

      async Task ProcessChannel(Channel c, VideoExtraPlans plan, int i) {
        var scraper = Scraper(platform);
        var ((freshChan, getVideos), ex) = await Def.Fun(() => scraper.ChannelAndVideos(c.SourceId, log)).Try();
        if (ex != null) {
          log.Warning(ex, "Collect {Platform} - Unable to load channel {c}: {Message}", platform, c, ex.Message);
          return; // don't save exceptional case. Don't know if it is our problem or thiers
        }

        var chan = c.JsonMerge(freshChan, new() {
          MergeNullValueHandling = Ignore,
          MergeArrayHandling = MergeArrayHandling.Union
        }); // merge in new values from the collect. Using merge array because we want alt SourceId's to accumulate

        await Store.Channels.Append(chan, log);
        log.Information("Collect {Platform} - saved channel {Channel}",  platform, c.ToString());

        if (options.Parts.ShouldRun(StandardCollectPart.Video) && getVideos != null
          && chan.ForUpdate()) { // check in case we discovered a channel but it doesn't have enough subs
          var (videos, vEx) = await getVideos.SelectManyList().Try();
          if (vEx != null) {
            log.Warning(vEx, "Collect {Platform} - Unable to load videos for channel {Channel}: {Message}", platform, chan.ToString(), vEx.Message);
            return;
          }
          await Store.Videos.Append(videos);
          log.Debug("Collect {Platform} - saved {Videos} videos for {Channel}", platform, videos.Count, chan.ToString());

          if (options.Parts.ShouldRun(Extra))
            foreach (var v in videos) {
              // if this video doesn't exist in the plan, update extra
              if (plan.ContainsVideo(v.VideoId)) continue;
              var forUpdate = new VideoForUpdate {
                ChannelId = v.ChannelId,
                VideoId = v.VideoId,
                Updated = v.Updated,
                UploadDate = v.UploadDate,
                SourceId = v.SourceId
              };
              plan.SetForUpdate(forUpdate);
              plan.SetPart(v.VideoId, EExtra);
            }
        }

        if (options.Parts.ShouldRun(Extra)) {
          var extras = await plan.WithPart(EExtra).BlockMap(v => scraper.Video(v.ForUpdate.SourceId, log)
              .Swallow(e => log.Error(e, "Collect {Platform} - error crawling video {Video}: {Error}", platform, v.VideoId, e.Message))
            , scraper.CollectParallel, cancel: cancel
          ).NotNull().ToListAsync();
          await Store.VideoExtra.Append(extras, log);
          log.Debug("Collect {Platform} - saved {Videos} video extras for {Channel}", platform, extras.Count, chan.ToString());
        }
      }

      await channelPlans.BlockDo(async (p,i) => {
        await ProcessChannel(p.c, p.videoPlans, i);
        log.Information("Collect {Platform} - completed processing channel {Channel} {Num}/{BatchTotal}", 
          platform, p.c.ToString(), i + 1, channelPlans.Length);
      }, parallel: 4, cancel:cancel);

      return channelPlans.Length;
    }
  }
}