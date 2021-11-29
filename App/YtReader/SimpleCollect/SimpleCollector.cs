using System;
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
using YtReader.Db;
using YtReader.Store;
using YtReader.Yt;
using static System.Array;
using static YtReader.SimpleCollect.StandardCollectPart;
using static YtReader.Store.DiscoverSourceType;
using ChanById = SysExtensions.Collections.IKeyedCollection<string, YtReader.Store.Channel>;
using static Mutuo.Etl.Pipe.PipeArg;
using static Newtonsoft.Json.Linq.MergeNullValueHandling;
using static YtReader.Yt.ExtraPart;

namespace YtReader.SimpleCollect; 

/// <summary>Collector for sources that are simple enough to use the same basic planning and execution of a scrape</summary>
public record SimpleCollector(SnowflakeConnectionProvider Sf, YtStore Store, IPipeCtx PipeCtx,
  YtCollectCfg YtCollectCfg) : ICollector {
  public async Task<(VideoExtra[] Extras, Rec[] Recs, VideoComment[] Comments, VideoCaption[] Captions)> SaveExtraAndParts(
    Platform platform, Channel c, ExtraPart[] parts, ILogger log, VideoExtraPlans planedExtras) {
    var scraper = Scraper(platform);
    if (c != null && c.Platform != platform) throw new($"platform '{c.Platform}' of channel `{c.ChannelId}` doesn't match provided '{platform}'");
    var extras = await planedExtras.WithPart(EExtra).BlockMap(v => {
        return scraper.VideoAndExtra(v.ForUpdate.SourceId, parts, log, c)
          .Swallow(e => log.Error(e, "Collect {Platform} - error crawling video {Video}: {Error}", platform, v.VideoId, e.Message));
      }, scraper.CollectCfg.WebParallel
    ).NotNull().ToArrayAsync();
    var vids = extras.Select(r => r.Video).NotNull().ToArray();
    if (parts.ShouldRun(EExtra))
      await Store.VideoExtra.Append(vids);
    var comments = extras.SelectMany(r => r.Comments.NotNull()).NotNull().ToArray();
    if (parts.ShouldRun(EComment))
      await Store.Comments.Append(comments);
    log.Debug("Collect {Platform} - saved {Videos} video extras for {Channel}", platform, extras.Length, c?.ToString() ?? "");
    return (vids, Empty<Rec>(), comments, Empty<VideoCaption>());
  }

  public async Task Collect(SimpleCollectOptions options, ILogger log, CancellationToken cancel) {
    log = log.ForContext("Function", nameof(Collect));
    if (options.Mode == SimpleCollectMode.Dedupe) {
      await DedupeChannels(options, log);
      return;
    }
    var plan = await PlanSimpleCollect(options, log, cancel);
    plan = await Discover(plan, log, cancel);
    if (plan.Parts.ShouldRunAny(StandardCollectPart.Channel, ChannelVideo, Extra))
      await plan.ChannelPlans.Pipe(PipeCtx,
        b => SimpleCollectChannels(b, options.Platform, options.Parts, Inject<ILogger>(), Inject<CancellationToken>()),
        log: log, cancel: cancel);
  }

  async Task DedupeChannels(SimpleCollectOptions options, ILogger log) {
    using var dbCtx = await DbCtx(options.Platform, log);
    var chans = await dbCtx.ChannelUpdateStats(options.ExplicitChannels).Then(b => b.Select(c => c.Channel).KeyBy(c => c.SourceId));
    var dupes = chans
      .SelectMany(c => c.SourceIdAlts.NotNull().Select(a => (dupe: chans[a], chan: c)).Where(d => d.dupe != null))
      .Select(d => d.dupe with {Status = ChannelStatus.Dupe, StatusMessage = $"dupe of canon channel {d.chan.SourceId}"}).ToArray();
    // update all dupes in db with a new status
    await Store.Channels.Append(dupes);
  }

  public IScraper Scraper(Platform platform) => platform switch {
    _ => throw new($"Platform {platform} not supported")
  };

  public async ValueTask<SimpleCollectPlan> PlanSimpleCollect(SimpleCollectOptions options, ILogger log, CancellationToken cancel) {
    var plan = new SimpleCollectPlan(options);
    var platform = options.Platform;
    var scraper = Scraper(platform);
    using (var dbCtx = await DbCtx(platform, log))
      plan = plan.WithAddedChannels("existing", await dbCtx.ChannelUpdateStats(options.ExplicitChannels), log);
    return plan.WithAddedChannels("explicit", plan.ExplicitChannels.NotNull()
      .Select(c => (scraper.NewChan(c) with {DiscoverSource = new(Manual, c)}).AsPlan()), log);
  }

  public async ValueTask<SimpleCollectPlan> Discover(SimpleCollectPlan plan, ILogger log, CancellationToken cancel = default) {
    var platform = plan.Platform;
    if (plan.Parts.ShouldRun(DiscoverHome)) {
      var chans = await Scraper(platform).HomeVideos(log, cancel).NotNull().SelectMany()
        .Select(v => v with {DiscoverSource = new(Home)})
        .Batch(1000)
        .BlockMap(async b => {
          var videos = b.ToArray();
          await Store.Videos.Append(videos, log);
          return CrawledChannels(platform, videos);
        }, cancel: cancel)
        .SelectMany().Distinct().ToListAsync(cancellationToken: cancel);
      plan = plan.WithAddedChannels("video crawled channels", chans, log);
    }
    return plan;
  }

  /// <summary>Scrape videos from the plan, save to the store, and append new channels to the plan</summary>
  public async ValueTask<SimpleCollectPlan> CrawlVideoLinks(SimpleCollectPlan plan, ExtraPart[] parts, ILogger log) {
    var scraper = Scraper(plan.Platform);
    var crawledVideos = await plan.VideosToCrawl.BlockMap(async (discover, i) => {
      var video = await scraper.VideoAndExtra(discover.LinkId, parts, log)
        .Swallow(e => log.Warning(e, "Collect {Platform} - error crawling video {Video}: {Error}", plan.Platform, discover.LinkId, e.Message));
      log.Debug("Collect {Platform} - crawled video {VideoId} {Vid}/{Total}", plan.Platform, video.Video?.VideoId, i, plan.VideosToCrawl.Count);
      return (discover, video);
    }, scraper.CollectParallel).ToListAsync();

    await Store.VideoExtra.Append(crawledVideos.Select(r => r.video.Video).NotNull().ToArray());
    await Store.Comments.Append(crawledVideos.SelectMany(r => r.video.Comments).NotNull().ToArray());
    log.Information("Collect {Platform} - saved {Videos} videos", plan.Platform, crawledVideos.Count);
    var crawledChannels = CrawledChannels(plan.Platform, crawledVideos.Select(v => v.video.Video));
    return plan.WithAddedChannels("video crawled channels", crawledChannels, log);
  }

  static Channel[] CrawledChannels<T>(Platform platform, IEnumerable<T> videos) where T : Video {
    var crawledChannels = videos.Where(v => v?.ChannelId.HasValue() == true)
      .Select(v => new Channel(platform, v.ChannelId, v.ChannelSourceId) {
        ChannelTitle = v.ChannelTitle,
        DiscoverSource = v.DiscoverSource
      }).Distinct().ToArray();
    return crawledChannels;
  }

  public async Task<CollectDbCtx> DbCtx(Platform platform, ILogger log) => new(await Sf.Open(log), platform, Scraper(platform).CollectCfg);

  [Pipe]
  public async Task<int> SimpleCollectChannels(IReadOnlyCollection<ChannelUpdatePlan> channels, Platform platform, StandardCollectPart[] parts, ILogger log,
    CancellationToken cancel) {
    using var dbCtx = await DbCtx(platform, log);
    //var channelById = channels.ToKeyedCollection(c => c.c.ChannelId);
    var videoPlanById = await dbCtx.VideosForUpdate(channels.Select(c => c.Channel).ToArray())
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

    await channelPlans.BlockDo(async (p, i) => {
      await ProcessChannel(p.c, p.videoPlans, i);
      log.Information("Collect {Platform} - completed processing channel {Channel} {Num}/{BatchTotal}",
        platform, p.c.Channel.ToString(), i + 1, channelPlans.Length);
    }, parallel: 4, cancel: cancel);
    return channelPlans.Length;

    async Task ProcessChannel(ChannelUpdatePlan c, VideoExtraPlans plans, int i) {
      var scraper = Scraper(platform);
      var ((freshChan, getVideos), ex) = await Def.Fun(() => scraper.ChannelAndVideos(c.Channel.SourceId, log)).Try();
      if (ex != null) {
        log.Warning(ex, "Collect {Platform} - Unable to load channel {c}: {Message}", platform, c, ex.Message);
        return; // don't save exceptional case. Don't know if it is our problem or thiers
      }

      var chan = c.Channel.JsonMerge(freshChan, new() {
        MergeNullValueHandling = Ignore,
        MergeArrayHandling = MergeArrayHandling.Union
      }); // merge in new values from the collect. Using merge array because we want alt SourceId's to accumulate

      if (parts.ShouldRun(StandardCollectPart.Channel))
        await Store.Channels.Append(chan, log);
      log.Information("Collect {Platform} - saved channel {Channel}", platform, c.ToString());

      if (parts.ShouldRun(ChannelVideo) && getVideos != null
          && chan.ForUpdate()) { // check in case we discovered a channel but it doesn't have enough subs
        var videosFrom = c.VideosFrom ??
          DateTime.Now - (c.LastVideoUpdate == null ? YtCollectCfg.RefreshVideosWithinNew : YtCollectCfg.RefreshVideosWithinDaily);
        var (videos, vEx) = await getVideos
          .TakeWhileInclusive(b => videosFrom < b.Min(v => v.UploadDate))
          .SelectManyList().Try();
        if (vEx != null) {
          log.Warning(vEx, "Collect {Platform} - Unable to load videos for channel {Channel}: {Message}", platform, chan.ToString(), vEx.Message);
          return;
        }
        await Store.Videos.Append(videos);
        log.Debug("Collect {Platform} - saved {Videos} videos for {Channel}", platform, videos.Count, chan.ToString());

        if (parts.ShouldRun(Extra))
          foreach (var v in videos) {
            var plan = plans[v.VideoId];
            if (plan == null) {
              var forUpdate = new VideoForUpdate {
                ChannelId = v.ChannelId,
                VideoId = v.VideoId,
                Updated = v.Updated,
                UploadDate = v.UploadDate,
                SourceId = v.SourceId,
                Platform = v.Platform
              };
              plans.SetForUpdate(forUpdate);
              plans.SetPart(v.VideoId, EExtra, EComment);
            }
            else {
              if (plan.ForUpdate.HasComment == false) plan.SetPart(EComment);
              if (plan.ForUpdate.ExtraUpdated == null) plan.SetPart(EExtra);
            }
          }
      }

      if (parts.ShouldRun(Extra))
        await SaveExtraAndParts(platform, c.Channel, parts: null, log, plans);
    }
  }
}