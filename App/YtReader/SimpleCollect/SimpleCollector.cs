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
using static System.Array;
using static YtReader.SimpleCollect.StandardCollectPart;
using static YtReader.Store.ChannelSourceType;
using ChanById = SysExtensions.Collections.IKeyedCollection<string, YtReader.Store.Channel>;
using static Mutuo.Etl.Pipe.PipeArg;
using static Newtonsoft.Json.Linq.MergeNullValueHandling;
using static YtReader.Yt.ExtraPart;

namespace YtReader.SimpleCollect {
  /// <summary>Collector for sources that are simple enough to use the same basic planning and execution of a scrape</summary>
  public record SimpleCollector(SnowflakeConnectionProvider Sf, RumbleWeb Rumble, BitChuteScraper BitChute, YtStore Store, IPipeCtx PipeCtx,
    YtCollectCfg CollectCfg) : ICollector {
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

      if (plan.Parts.ShouldRun(StandardCollectPart.Channel)) // ad existing channels limit to explicit
        WithUp("existing", plan.Existing.Where(c => c.ForUpdate(plan.ExplicitChannels)).ToArray());

      // add explicit channel, no need to lookup existing, because that will already be in the list
      WithUp("explicit",
        plan.ExplicitChannels.NotNull().Select(c => scraper.NewChan(c) with {DiscoverSource = new(Manual, c)}).ToArray());

      if (!parts.ShouldRunAny(DiscoverLink, DiscoverVideo) || plan.ExplicitChannels?.Any() == true) return plan;
      var discovered = await dbCtx.DiscoverChannelsAndVideos();
      if (parts.ShouldRun(DiscoverLink))
        WithUp("discovered", discovered.Where(d => d.LinkType == LinkType.Channel)
          .Select(selector: l => scraper.NewChan(l.LinkId) with {DiscoverSource = l.ToDiscoverSource()}).ToArray());

      if (!parts.ShouldRun(DiscoverVideo)) return plan;

      plan = plan with {
        VideosToCrawl = discovered.Where(d => d.LinkType == LinkType.Video)
          .Select(l => new DiscoverSource(VideoLink, l.LinkId, l.FromPlatform)).ToArray()
      };
      log.Information("Collect {Platform} - planned {Videos} videos for crawl", platform, plan.VideosToCrawl.Count);
      return plan;
    }

    public async ValueTask<SimpleCollectPlan> Discover(SimpleCollectPlan plan, ILogger log, CancellationToken cancel = default) {
      if (plan.Parts.ShouldRun(DiscoverVideo))
        plan = await CrawlVideoLinks(plan, log);
      if (plan.Parts.ShouldRun(DiscoverHome)) {
        var platform = plan.Platform;
        var chans = await Scraper(plan.Platform).HomeVideos(log).BlockMap(async b => {
            var videos = b.NotNull().Select(v => (discover: new DiscoverSource(Home), video: v)).ToArray();
            await Store.Videos.Append(videos.Select(v => v.video).ToArray(), log);
            //log.Information("Collect {Platform} - crawled {Videos} home videos", platform, videos.Length);
            return CrawledChannels(platform, videos);
          }, cancel: cancel)
          .SelectMany().Distinct().ToListAsync();
        plan = plan.WithForUpdate("video crawled channels", chans, log);
      }
      return plan;
    }

    /// <summary>Scrape videos from the plan, save to the store, and append new channels to the plan</summary>
    public async ValueTask<SimpleCollectPlan> CrawlVideoLinks(SimpleCollectPlan plan, ILogger log) {
      var scraper = Scraper(plan.Platform);
      var crawledVideos = await plan.VideosToCrawl.BlockMap(async (discover, i) => {
        var video = await scraper.VideoAndExtra(discover.LinkId, log)
          .Swallow(e => log.Warning(e, "Collect {Platform} - error crawling video {Video}: {Error}", plan.Platform, discover.LinkId, e.Message));
        log.Debug("Collect {Platform} - crawled video {VideoId} {Vid}/{Total}", plan.Platform, video.Video?.VideoId, i, plan.VideosToCrawl.Count);
        return (discover, video);
      }, scraper.CollectParallel).ToListAsync();

      await Store.VideoExtra.Append(crawledVideos.Select(r => r.video.Video).NotNull().ToArray());
      await Store.Comments.Append(crawledVideos.SelectMany(r => r.video.Comments).NotNull().ToArray());
      log.Information("Collect {Platform} - saved {Videos} videos", plan.Platform, crawledVideos.Count);
      var crawledChannels = CrawledChannels(plan.Platform, crawledVideos.Select(v => (v.discover, v.video.Video)));
      return plan.WithForUpdate("video crawled channels", crawledChannels, log);
    }

    static Channel[] CrawledChannels<T>(Platform platform, IEnumerable<(DiscoverSource discover, T video)> crawledVideos) where T : Video {
      var crawledChannels = crawledVideos.Where(v => v.video?.ChannelId.HasValue() == true)
        .Select(v => new Channel(platform, v.video.ChannelId, v.video.ChannelSourceId) {
          ChannelTitle = v.video.ChannelTitle,
          DiscoverSource = v.discover
        }).Distinct().ToArray();
      return crawledChannels;
    }

    public async ValueTask<SimpleCollectPlan> CollectChannelAndVideos(SimpleCollectPlan plan, ILogger log, CancellationToken cancel) {
      var scraper = Scraper(plan.Platform);
      if (plan.ExplicitVideos != null) {
        var videos = await plan.ExplicitVideos.BlockMap(v => scraper.VideoAndExtra(v, log)).ToListAsync();
        await Store.Videos.Append(videos.Select(v => v.Video).NotNull().ToArray());
        await Store.Comments.Append(videos.SelectMany(v => v.Comments).NotNull().ToArray());
      }

      await plan.ToUpdate.Process(PipeCtx,
        b => SimpleCollectChannels(b, plan.Options, Inject<ILogger>(), Inject<CancellationToken>()),
        log: log, cancel: cancel);

      return plan;
    }

    public async Task<CollectDbCtx> DbCtx(Platform platform, ILogger log) => new(await Sf.Open(log), platform);

    [Pipe]
    public async Task<int> SimpleCollectChannels(IReadOnlyCollection<Channel> channels, SimpleCollectOptions options, ILogger log,
      CancellationToken cancel) {
      var platform = options.Platform;
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
        return new {videoPlans, c};
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
        log.Information("Collect {Platform} - saved channel {Channel}", platform, c.ToString());

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
                SourceId = v.SourceId,
                Platform = v.Platform
              };
              plan.SetForUpdate(forUpdate);
              plan.SetPart(v.VideoId, EExtra);
            }
        }
      }

      await channelPlans.BlockDo(async (p, i) => {
        await ProcessChannel(p.c, p.videoPlans, i);
        log.Information("Collect {Platform} - completed processing channel {Channel} {Num}/{BatchTotal}",
          platform, p.c.ToString(), i + 1, channelPlans.Length);
      }, parallel: 4, cancel: cancel);

      return channelPlans.Length;
    }

    public async Task<(VideoExtra[] Extras, Rec[] Recs, VideoComment[] Comments, VideoCaption[] Captions)> SaveExtraAndParts(
      Platform platform, Channel c, ExtraPart[] parts,
      ILogger log, VideoExtraPlans planedExtras) {
      var scraper = Scraper(platform);
      if (c != null && c.Platform != platform) throw new($"platform '{c.Platform}' of channel `{c.ChannelId}` doesn't match provided '{platform}'");
      var extras = await planedExtras.WithPart(EExtra).BlockMap(v => {
          return scraper.VideoAndExtra(v.ForUpdate.SourceId, log)
            .Swallow(e => log.Error(e, "Collect {Platform} - error crawling video {Video}: {Error}", platform, v.VideoId, e.Message));
        }, CollectCfg.WebParallel
      ).NotNull().ToArrayAsync();
      var vids = extras.Select(r => r.Video).NotNull().ToArray();
      await Store.VideoExtra.Append(vids);
      var comments = extras.SelectMany(r => r.Comments).NotNull().ToArray();
      await Store.Comments.Append(comments);
      log.Debug("Collect {Platform} - saved {Videos} video extras for {Channel}", platform, extras.Length, c?.ToString() ?? "");
      return (vids, Empty<Rec>(), comments, Empty<VideoCaption>());
    }
  }
}