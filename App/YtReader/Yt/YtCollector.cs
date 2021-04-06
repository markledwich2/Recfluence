using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Humanizer;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Mutuo.Etl.Pipe;
using Serilog;
using Serilog.Core;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using static Mutuo.Etl.Pipe.PipeArg;
using static YtReader.Yt.UpdateChannelType;
using static YtReader.Yt.CollectPart;
using static YtReader.Yt.ExtraPart;
using static YtReader.Yt.YtCollectEx;

// ReSharper disable InconsistentNaming

namespace YtReader.Yt {
  public class YtCollector {
    readonly YtClient                    Api;
    readonly AppCfg                      Cfg;
    readonly SnowflakeConnectionProvider Sf;
    readonly IPipeCtx                    PipeCtx;
    readonly YtWeb                       Scraper;
    readonly YtStore                     DbStore;

    public YtCollector(BlobStores stores, AppCfg cfg, SnowflakeConnectionProvider sf, IPipeCtx pipeCtx, YtWeb ytWeb,
      YtClient api, ILogger log) {
      DbStore = new(stores.Store(DataStoreType.DbStage), log);
      Cfg = cfg;
      Sf = sf;
      PipeCtx = pipeCtx;
      Scraper = ytWeb;
      Api = api;
    }

    YtCollectCfg RCfg => Cfg.Collect;

    [Pipe]
    public async Task Collect(ILogger log, CollectOptions options, CancellationToken cancel = default) {
      options ??= new();
      if (options.CollectFrom.Type != CollectFromType.None) {
        await CollectFromVideoPathOrView(options, log, cancel);
      }
      else {
        var channels = await PlanAndUpdateChannelStats(options.Parts, options.LimitChannels, log, cancel);
        if (cancel.IsCancellationRequested)
          return;
        var (result, dur) = await channels
          .Randomize() // randomize to even the load
          .Process(PipeCtx,
            b => ProcessChannels(b, options.Parts, Inject<ILogger>(), Inject<CancellationToken>(), null), log: log, cancel: cancel)
          .WithDuration();

        var allChannelResults = result.Where(r => r.OutState != null).SelectMany(r => r.OutState.Channels).ToArray();
        log.Information("Collect - {Pipe} Complete - {Success}/{Total} channels updated in {Duration}",
          nameof(ProcessChannels), allChannelResults.Count(c => c.Success), allChannelResults.Length, dur.HumanizeShort());
      }
    }

    #region FromPathOrView

    /// <summary>Collect extra and recommendations from an imported list of videos. Used for arbitrary lists.</summary>
    async Task CollectFromVideoPathOrView(CollectOptions options, ILogger log, CancellationToken cancel = default) {
      var parts = options.Parts ?? new[] {PChannel, PStats, PExtra, PCaption};
      log.Information("YtCollect - Special Collect from {CollectFrom} started", options.CollectFrom);


      var refreshPeriod = 7.Days();
      IReadOnlyCollection<YtCollectDb.DiscoverRow> videos;
      IReadOnlyCollection<string> channelsForUpdate;
      using (var db = await Db(log)) {
        // sometimes updates fail. When re-running this, we should refresh channels that are missing videos or have a portion of captions not attempted
        // NOTE: core warehouse table must be updated (not just staging tables) to take into account previously successful loads.
        channelsForUpdate = !parts.ShouldRun(PChannel)
          ? Array.Empty<string>()
          : await db.StaleOrCaptionlessChannels(options, refreshPeriod);

        // videos sans extra update
        videos = await db.MissingVideos(options);
      }

      // process all videos
      var extras = parts.ShouldRun(PExtra)
        ? videos.Count > 200 ? await videos
          .Process(PipeCtx, b => ProcessVideos(b, parts, Inject<ILogger>(), Inject<CancellationToken>()), log: log, cancel: cancel)
          .Then(res => res.SelectMany(r => r.OutState))
        : await ProcessVideos(videos, parts, log, cancel)
        : Array.Empty<VideoExtra>();

      var channelIds = Array.Empty<string>();
      if (parts.ShouldRun(PChannel)) {
        // then process all unique channels we don't already have recent data for
        channelIds = extras.Select(e => e.ChannelId)
          .Concat(videos.Select(v => v.channel_id))
          .Concat(channelsForUpdate)
          .NotNull().Distinct().ToArray();
        if (channelIds.Any()) {
          var channels = await PlanAndUpdateChannelStats(parts, channelIds, log, cancel)
            .Then(chans => chans
              .Where(c => c.LastVideoUpdate.OlderThanOrNull(refreshPeriod)) // filter to channels we haven't updated video's in recently
              .Select(c => c with {Update = c.Update == Discover ? Standard : c.Update})); // revert to standard update for channels detected as discover

          DateTime? fromDate = new DateTime(year: 2020, month: 01, day: 01);
          await channels
            .Randomize() // randomize to even the load
            .Process(PipeCtx,
              b => ProcessChannels(b, parts, Inject<ILogger>(), Inject<CancellationToken>(), fromDate), log: log, cancel: cancel)
            .WithDuration();
        }
      }
      log.Information("YtCollect - ProcessVideos complete - {Videos} videos and {Channels} channels processed",
        videos.Count, channelIds.Length);
    }

    [Pipe]
    public async Task<VideoExtra[]> ProcessVideos(IReadOnlyCollection<YtCollectDb.DiscoverRow> videos, CollectPart[] parts, ILogger log,
      CancellationToken cancel) {
      log ??= Logger.None;
      const int batchSize = 1000;
      var plans = new VideoExtraPlans();
      if (parts.ShouldRun(PExtra))
        plans.SetPart(videos.Select(v => v.video_id), EExtra);
      if (parts.ShouldRun(PCaption))
        plans.SetPart(videos.Where(v => v.channel_id != null && !v.caption_exists && v.video_id != null).Select(v => v.video_id), ECaptions);
      var extra = await plans.Batch(batchSize)
        .BlockTrans(async (p, i) => {
          var (e, _, _, _) = await SaveExtraAndParts(c: null, new[] {PExtra, PCaption}, log, new(p));
          log.Information("ProcessVideos - saved extra {Videos}/{TotalBatches} ", i * batchSize + e.Length, p.Count);
          return e;
        }, Cfg.Collect.ParallelChannels, cancel: cancel)
        .SelectManyList();
      return extra.ToArray();
    }

    #endregion

    /// <summary>Update channel data from the YouTube API and determine what type of update should be performed on each channel</summary>
    /// <returns></returns>
    async Task<IReadOnlyCollection<ChannelUpdatePlan>> PlanAndUpdateChannelStats(CollectPart[] parts, string[] limitChannels,
      ILogger log,
      CancellationToken cancel) {
      var store = DbStore.Channels;
      var explicitChannels = limitChannels.HasItems() ? limitChannels.ToHashSet() : Cfg.LimitedToSeedChannels?.ToHashSet() ?? new HashSet<string>();
      log.Information("Collect - Starting channels update. Limited to ({Included}). Parts ({Parts})",
        explicitChannels.Any() ? explicitChannels.Join("|") : "All", parts == null ? "All" : parts.Join("|"));

      var toDiscover = new List<ChannelUpdatePlan>();
      var explicitMissing = new List<ChannelUpdatePlan>();

      IKeyedCollection<string, ChannelUpdatePlan> existingChannels;
      using (var db = await Db(log)) {
        // retrieve previous channel state to update with new classification (algos and human) and stats form the API
        existingChannels = await db.ExistingChannels(explicitChannels);

        if (limitChannels != null) // discover channels specified in limit if they aren't in our dataset
          explicitMissing.AddRange(limitChannels.Where(c => !existingChannels.ContainsKey(c))
            .Select(c => new ChannelUpdatePlan {Channel = new() {ChannelId = c}, Update = Discover}));

        if (parts.ShouldRun(PDiscover))
          toDiscover.AddRange(await db.ChannelsToDiscover());
      }

      // perform full update on channels with a last full update older than 90 days (max X at a time because of quota limit).
      var fullUpdate = existingChannels
        .Where(c => c.Channel.Updated == default || c.Channel.Updated - c.Channel.LastFullUpdate > 90.Days())
        .Randomize().Take(200)
        .Select(c => c.Channel.ChannelId).ToHashSet();

      var channels = existingChannels
        .Select(c => {
          var full = fullUpdate.Contains(c.Channel.ChannelId);
          return c with {
            Update = full ? Full : Standard,
            VideosFrom = full ? null : c.VideosFrom // don't limit from date when on a full update
          };
        })
        .Concat(explicitMissing)
        .Concat(toDiscover).ToArray();

      if (!parts.ShouldRun(PChannel)) return channels;

      var (updatedChannels, duration) = await channels
        .Where(c => c.Update != None && c.Channel.Updated.Age() > RCfg.RefreshChannelDetailDebounce)
        .BlockFunc(async c => await UpdateChannelDetail(c, log), Cfg.DefaultParallel, cancel: cancel)
        .WithDuration();
      if (cancel.IsCancellationRequested) return updatedChannels;

      if (updatedChannels.Any())
        await store.Append(updatedChannels.Select(c => c.Channel).ToArray(), log);

      log.Information("Collect - Updated stats {Channels}/{AllChannels} channels. {Discovered} discovered, {Full} full {Duration}",
        updatedChannels.Count, channels.Length, updatedChannels.Count(c => c.Update == Discover), updatedChannels.Count(c => c.Update == Full),
        duration.HumanizeShort());

      var updatedIds = updatedChannels.Select(c => c.Channel.ChannelId).ToHashSet();
      var res = updatedChannels.Concat(channels.Where(c => !updatedIds.Contains(c.Channel.ChannelId))).ToArray();
      return res;
    }

    async Task<ChannelUpdatePlan> UpdateChannelDetail(ChannelUpdatePlan plan, ILogger log) {
      var channel = plan.Channel;
      var channelLog = log.ForContext("Channel", channel.ChannelId).ForContext("ChannelId", channel.ChannelId);
      var full = plan.Update == Full;
      var c = channel.JsonClone();
      try {
        c.Platform = Platform.YouTube;
        c.Updated = DateTime.UtcNow;
        var d = await Api.ChannelData(c.ChannelId, full); // to save quota - full update only when missing features channels
        if (d != null) {
          c.ChannelTitle = d.Title;
          c.Description = d.Description;
          c.LogoUrl = d.Thumbnails?.Default__?.Url;
          c.Subs = d.Stats?.SubCount;
          c.ChannelViews = d.Stats?.ViewCount;
          c.Country = d.Country;
          c.FeaturedChannelIds = d.FeaturedChannelIds ?? c.FeaturedChannelIds;
          c.Keywords = d.Keywords ?? c.Keywords;
          c.Subscriptions = d.Subscriptions ?? c.Subscriptions;
          c.DefaultLanguage = d.DefaultLanguage ?? c.DefaultLanguage;
          c.Status = ChannelStatus.Alive;
          if (full)
            c.LastFullUpdate = c.Updated;
        }
        else {
          c.Status = ChannelStatus.Dead;
        }
        channelLog.Information("Collect - {Channel} - channel details ({Update})", c.ChannelTitle, plan.Update.EnumString());
      }
      catch (Exception ex) {
        channelLog.Error(ex, "Collect - {Channel} - Error when updating details for channel : {Error}", c.ChannelTitle, ex.Message);
      }
      return plan with {Channel = c};
    }

    async Task<YtCollectDbCtx> Db(ILogger log) => new(RCfg, await Sf.Open(log), log);

    [Pipe]
    public async Task<ProcessChannelResults> ProcessChannels(IReadOnlyCollection<ChannelUpdatePlan> channels,
      CollectPart[] parts, ILogger log = null, CancellationToken cancel = default, DateTime? videoFromExplicit = null) {
      log ??= Logger.None;
      var workSw = Stopwatch.StartNew();

      var results = await channels.Batch(RCfg.ChannelBatchSize).BlockTrans(async (planBatch, batchNo) => {
        var channelBatch = planBatch.Select(p => p.Channel).ToArray();

        // to save on db round trips we batch our plan for updates
        var channelPlans = new Dictionary<string, VideoExtraPlans>();
        VideoExtraPlans VideoPlans(string channelId) => channelPlans.GetOrAdd(channelId, () => new());
        void SetPlanPart(string channelId, string videoId, ExtraPart part) => VideoPlans(channelId).SetPart(videoId, part);

        if (parts.ShouldRunAny(PExtra, PCaption)) {
          using var db = await Db(log);

          if (parts.ShouldRun(PExtra))
            foreach (var u in await db.VideosForUpdate(channelBatch))
              VideoPlans(u.ChannelId).SetForUpdate(u);

          if (parts.ShouldRun(PCaption))
            foreach (var (c, v) in await db.MissingCaptions(channelBatch))
              SetPlanPart(c, v, ECaptions);

          if (parts.ShouldRun(PComments))
            foreach (var (c, v) in await db.MissingComments(channelBatch))
              SetPlanPart(c, v, EComments);
        }

        var channelResults = await planBatch
          .Select(p => new {ChannelPlan = p, VideoPlans = channelPlans.TryGet(p.Channel.ChannelId) ?? new VideoExtraPlans()})
          .BlockTrans(async (plan, i) => {
            var c = plan.ChannelPlan.Channel;
            var sw = Stopwatch.StartNew();
            var cLog = log
              .ForContext("ChannelId", c.ChannelId)
              .ForContext("Channel", c.ChannelTitle);
            try {
              await using var conn = new Defer<ILoggedConnection<IDbConnection>>(async () => await Sf.Open(cLog));
              await UpdateAllInChannel(cLog, plan.ChannelPlan, parts, plan.VideoPlans, videoFromExplicit);
              var progress = i + batchNo * RCfg.ChannelBatchSize + 1;
              cLog.Information("Collect - {Channel} - Completed videos/recs/captions in {Duration}. Progress: channel {Count}/{BatchTotal}",
                c.ChannelTitle, sw.Elapsed.HumanizeShort(), progress, channels.Count);
              return (c, Success: true);
            }
            catch (Exception ex) {
              ex.ThrowIfUnrecoverable();
              cLog.Error(ex, "Collect - Error updating channel {Channel}: {Error}", c.ChannelTitle, ex.Message);
              return (c, Success: false);
            }
          }, RCfg.ParallelChannels, cancel: cancel).ToListAsync();

        return channelResults;
      }).SelectManyList();

      var res = new ProcessChannelResults {
        Channels = results.Select(r => new ProcessChannelResult {ChannelId = r.c.ChannelId, Success = r.Success}).ToArray(),
        Duration = workSw.Elapsed
      };

      log.Information(
        "Collect - {Pipe} complete - {ChannelsComplete} channel videos/captions/recs, {ChannelsFailed} failed {Duration}",
        nameof(ProcessChannels), results.Count(c => c.Success), results.Count(c => !c.Success), res.Duration);

      return res;
    }

    async Task UpdateAllInChannel(ILogger log, ChannelUpdatePlan plan, CollectPart[] parts, VideoExtraPlans plans, DateTime? videosFromExplicit = null) {
      var c = plan.Channel;

      void NotUpdatingLog(string reason) => log.Information("Collect - {Channel} - Not updating videos/recs/captions because: {Reason} ",
        c.ChannelTitle, reason);

      if (c.Status == ChannelStatus.Dead) {
        NotUpdatingLog("it's dead");
        return;
      }
      if (c.Subs < RCfg.MinChannelSubs && c.ChannelViews < RCfg.MinChannelViews) {
        NotUpdatingLog($"too small - subs ({c.Subs}), channel views ({c.ChannelViews})");
        return;
      }
      if (c.StatusMessage.HasValue()) {
        NotUpdatingLog($"status msg ({c.StatusMessage})");
        return;
      }

      var discover = plan.Update == Discover; // no need to check this against parts, that is done when planning the update
      var full = plan.Update == Full;

      var videoItems = new List<YtVideoItem>();
      if (parts.ShouldRunAny(PStats, PRecs, PCaption) || discover && parts.ShouldRun(PDiscover)) {
        log.Information("Collect - {Channel} - Starting channel update of videos/recs/captions", c.ChannelTitle);

        // get the oldest date for videos to store updated statistics for. This overlaps so that we have a history of video stats.
        var videosFrom = videosFromExplicit ?? plan.VideosFrom ?? DateTime.UtcNow - RCfg.RefreshVideosWithinNew;
        var videoItemsLimit = discover ? RCfg.DiscoverChannelVids : full ? RCfg.MaxChannelFullVideos : RCfg.MaxChannelDailyVideos;
        var vidsEnum = ChannelVidItems(c, videosFrom, plans, log);
        videoItems = await vidsEnum.Take(videoItemsLimit).ToListAsync();

        if (parts.ShouldRun(PStats))
          await SaveVids(c, videoItems, DbStore.Videos, log);
      }

      if (parts.ShouldRunAny(PRecs, PExtra, PCaption, PComments)) {
        if (parts.ShouldRun(PRecs) && !discover)
          plans.SetPart(VideoToUpdateRecs(plan, videoItems), ERecs);

        if (parts.ShouldRun(PExtra) && plans.Any()) {
          // add videos that look seem missing given the current update
          var videoItemsKey = videoItems.ToKeyedCollection(v => v.Id);
          var oldestUpdate = videoItems.Min(v => v.UploadDate);

          var suspectMissingFresh = plans
            .Where(v => v.ForUpdate?.UploadDate > oldestUpdate // newer than our oldest update
                        && !videoItemsKey.ContainsKey(v.VideoId) // missing from this run
                        && v.ForUpdate?.ExtraUpdated.OlderThanOrNull(7.Days()) == true) // haven't tried updating extra for more than 7d
            .Select(v => v.VideoId).ToArray();
          log.Debug("Collect {Channel} - Video-extra for {Videos} video's because they look  missing", c.ChannelTitle, suspectMissingFresh.Length);
          plans.SetPart(suspectMissingFresh, EExtra);

          // videos that we just refreshed but don't have any extra yet. limit per run to not slow down a particular update too much
          var missingExtra = videoItems
            .Where(v => plans[v.Id]?.ForUpdate?.ExtraUpdated == null)
            .OrderByDescending(v => v.UploadDate)
            .Select(v => v.Id).ToArray();
          log.Debug("Collect {Channel} - Video-extra for {Videos} video's new video's or ones without any extra yet", c.ChannelTitle, missingExtra.Length);
          plans.SetPart(missingExtra, EExtra);

          // videos that we know are missing extra updated
          foreach (var p in plans.Where(p => p.ForUpdate != null && p.ForUpdate.ExtraUpdated == null && !p.Parts.Contains(EExtra)))
            p.Parts = p.Parts.Concat(EExtra).ToArray();

          // captions for all new videos from the vids list. missing captions have already been planned for existing videos.
          plans.SetPart(videoItems.Where(v => plan.LastCaptionUpdate == null || v.UploadDate > plan.LastCaptionUpdate).Select(v => v.Id), ECaptions);

          // take a random sample of comments for new videos since last comment record (NOTE: this is in addition to missing comments have separately been planned)
          plans.SetPart(videoItems
            .Where(v => plan.LastCommentUpdate == null || v.UploadDate > plan.LastCommentUpdate)
            .Randomize().Take(RCfg.MaxChannelComments).Select(v => v.Id), EComments);
        }
        await SaveExtraAndParts(c, parts, log, plans);
      }
    }

    /// <summary>Returns a list of videos that should have recommendations updated.</summary>
    IEnumerable<string> VideoToUpdateRecs(ChannelUpdatePlan plan, List<YtVideoItem> vids) {
      static bool ChannelInTodaysRecsUpdate(ChannelUpdatePlan plan, int cycleDays) =>
        plan.LastRecUpdate == null ||
        DateTime.UtcNow - plan.LastRecUpdate > 8.Days() ||
        plan.Channel.ChannelId.GetHashCode().Abs() % cycleDays == (DateTime.Today - DateTime.UnixEpoch).TotalDays.RoundToInt() % cycleDays;

      var c = plan.Channel;
      var vidsDesc = vids.OrderByDescending(v => v.UploadDate).ToList();
      var inThisWeeksRecUpdate = ChannelInTodaysRecsUpdate(plan, cycleDays: 7);
      var toUpdate = new List<YtVideoItem>();
      if (plan.LastRecUpdate == null) {
        Log.Debug("Collect - {Channel} - first rec update, collecting max", c.ChannelTitle);
        toUpdate.AddRange(vidsDesc.Take(RCfg.RefreshRecsMax));
      }
      else if (inThisWeeksRecUpdate) {
        Log.Debug("Collect - {Channel} - performing weekly recs update", c.ChannelTitle);
        toUpdate.AddRange(vidsDesc.Where(v => v.UploadDate?.YoungerThan(RCfg.RefreshRecsWithin) == true)
          .Take(RCfg.RefreshRecsMax));
        var deficit = RCfg.RefreshRecsMin - toUpdate.Count;
        if (deficit > 0)
          toUpdate.AddRange(vidsDesc.Where(v => toUpdate.All(u => u.Id != v.Id))
            .Take(deficit)); // if we don't have new videos, refresh the min amount by adding videos 
      }
      else {
        Log.Debug("Collect - {Channel} - skipping rec update because it's not this channels day", c.ChannelTitle);
      }

      return toUpdate.Select(v => v.Id);
    }

    static async Task SaveVids(Channel c, IReadOnlyCollection<YtVideoItem> vids, JsonlStore<Video> vidStore, ILogger log) {
      var vidsStored = ToVidsStored(c, vids);
      if (vidsStored.Any())
        await vidStore.Append(vidsStored, log);
      log.Information("Collect - {Channel} - Recorded {VideoCount} videos", c.ChannelTitle, vids.Count);
    }

    async IAsyncEnumerable<YtVideoItem> ChannelVidItems(Channel c, DateTime uploadFrom, VideoExtraPlans plans, ILogger log) {
      long vidCount = 0;
      await foreach (var vids in Scraper.ChannelVideos(c.ChannelId, log)) {
        vidCount += vids.Count;
        log.Debug("YtCollect - read {Videos} videos for channel {Channel}", vidCount, c.ChannelTitle);
        foreach (var v in vids) {
          var u = plans[v.Id]?.ForUpdate;
          yield return u?.UploadDate == null ? v : v with {UploadDate = u.UploadDate}; // not really needed. but prefer to fix inaccurate dates when we can
        }
        if (vids.Any(v => v.UploadDate < uploadFrom)) // return all vids on a page because its free. Stop on page with something older than uploadFrom
          yield break;
      }
    }

    public IAsyncEnumerable<ExtraAndParts> GetExtras(VideoExtraPlans extras, ILogger log, string channelId = null, string channelTitle = null) =>
      extras.Where(e => e.Parts.Any())
        .BlockTrans(async v => await Scraper.GetExtra(log, v.VideoId, v.Parts, channelId, channelTitle), RCfg.WebParallel);

    /// <summary>Saves recs for all of the given vids</summary>
    async Task<(VideoExtra[] Extras, Rec[] Recs, VideoComment[] Comments, VideoCaption[] Captions)> SaveExtraAndParts(Channel c, CollectPart[] parts,
      ILogger log, VideoExtraPlans planedExtras) {
      var extrasAndParts = await GetExtras(planedExtras, log, c?.ChannelId, c?.ChannelTitle).ToListAsync();
      foreach (var e in extrasAndParts) {
        e.Extra.ChannelId ??= c?.ChannelId; // if the video has an error, it may not have picked up the channel
        e.Extra.ChannelTitle ??= c?.ChannelTitle;
      }

      var s = extrasAndParts.Split();
      var updated = DateTime.UtcNow;
      if (parts.ShouldRun(PExtra)) await DbStore.VideoExtra.Append(s.Extras, log);
      if (parts.ShouldRun(PRecs)) await DbStore.Recs.Append(ToRecStored(extrasAndParts, updated), log);
      if (parts.ShouldRun(PComments)) await DbStore.Comments.Append(s.Comments, log);
      if (parts.ShouldRun(PCaption)) await DbStore.Captions.Append(s.Captions, log);

      log.Information("Collect - {Channel} - Recorded {WebExtras} web-extras, {Recs} recs, {Comments} comments, {Captions} captions",
        c?.ChannelTitle ?? "(unknown)", s.Extras.Length, s.Recs.Length, s.Comments.Length, s.Captions.Length);

      return s;
    }
  }
}