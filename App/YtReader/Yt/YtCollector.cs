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
using YtReader.SimpleCollect;
using YtReader.Store;
using static Mutuo.Etl.Pipe.PipeArg;
using static YtReader.Yt.UpdateChannelType;
using static YtReader.Yt.CollectPart;
using static YtReader.Yt.ExtraPart;
using static YtReader.Yt.YtCollectEx;

// ReSharper disable InconsistentNaming

namespace YtReader.Yt {
  public interface ICollector {
    Task<(VideoExtra[] Extras, Rec[] Recs, VideoComment[] Comments, VideoCaption[] Captions)> SaveExtraAndParts(Platform platform, Channel c, ExtraPart[] parts,
      ILogger log, VideoExtraPlans planedExtras);
  }

  public class YtCollector : ICollector {
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

      await Task.WhenAll(
        options.Parts.ShouldRunAny(PUser) ? ProcessAllMissingUserChannels(log, cancel) : Task.CompletedTask,
        options.Parts.ShouldRunAny(PChannel, PChannelVideos, PDiscover)
          ? ProcessAllChannels(log, options, cancel)
          : Task.CompletedTask
      );
    }

    async Task<bool> ProcessAllChannels(ILogger log, CollectOptions options, CancellationToken cancel) {
      var channels = await PlanAndUpdateChannelStats(options.Parts, options.LimitChannels, log, cancel);
      if (cancel.IsCancellationRequested)
        return true;
      var (result, dur) = await channels
        .Randomize() // randomize to even the load
        .Process(PipeCtx,
          b => ProcessChannels(b, options.ExtraParts, Inject<ILogger>(), Inject<CancellationToken>(), null), log: log, cancel: cancel)
        .WithDuration();

      var allChannelResults = result.Where(r => r.OutState != null).SelectMany(r => r.OutState.Channels).ToArray();
      log.Information("Collect - {Pipe} Complete - {Success}/{Total} channels updated in {Duration}",
        nameof(ProcessChannels), allChannelResults.Count(c => c.Success), allChannelResults.Length, dur.HumanizeShort());
      return false;
    }

    async Task ProcessAllMissingUserChannels(ILogger log, CancellationToken cancel = default) {
      var sw = Stopwatch.StartNew();
      using var db = await Db(log);
      var missing = await db.MissingUsers();
      var total = await missing.Process(PipeCtx,
          b => CollectUserChannels(b, Inject<ILogger>(), Inject<CancellationToken>()), log: log, cancel: cancel)
        .Then(r => r.Sum(i => i.OutState));
      log.Information("Collect - completed scraping all user channels {Total} in {Duration}", total, sw.Elapsed.HumanizeShort());
    }

    [Pipe]
    public async Task<int> CollectUserChannels(IReadOnlyCollection<string> channelIds, ILogger log, CancellationToken cancel) {
      log.Information("Collect - started scraping user channels {Total}", channelIds.Count);
      var batchTotal = channelIds.Count / RCfg.UserBatchSize;
      var start = Stopwatch.StartNew();
      var total = await channelIds.Batch(RCfg.ChannelBatchSize).BlockMap(async (ids, i) => {
          var userChannels = await ids.BlockMap(async c => {
              var u = await Scraper.Channel(log, c).Swallow(ex => log.Warning(ex, "error loading channel {Channel}", c));
              if (u == null) return null;
              var subs = await u.Subscriptions().SelectManyList().Swallow(ex => log.Warning(ex, "error loading subscriptions for user {User}", u.Id));
              return new User {
                UserId = u.Id,
                Platform = Platform.YouTube,
                Name = u.Title,
                ProfileUrl = u.LogoUrl,
                Updated = DateTime.UtcNow,
                Subscriptions = subs,
                SubscriberCount = u.Subs,
              };
            }, RCfg.WebParallel, cancel: cancel)
            .NotNull().ToListAsync();
          log.Debug("Collect - scraped {Users} users. Batch {Batch}/{Total}", userChannels.Count, i, batchTotal);
          await DbStore.Users.Append(userChannels);
          return userChannels.Count;
        }, RCfg.ParallelChannels, cancel: cancel) // mimic parallel settings from channel processing e.g. x4 outer, x6 inner
        .SumAsync();
      log.Information("Collect - completed scraping user channels {Success}/{Total} in {Duration}",
        total, channelIds.Count, start.Elapsed.HumanizeShort());
      return total;
    }

    /// <summary>Update channel data from the YouTube API and determine what type of update should be performed on each channel</summary>
    /// <returns></returns>
    public async Task<IReadOnlyCollection<ChannelUpdatePlan>> PlanAndUpdateChannelStats(CollectPart[] parts, IReadOnlyCollection<string> limitChannels,
      ILogger log, CancellationToken cancel) {
      var store = DbStore.Channels;
      var explicitChannels = limitChannels.HasItems() ? limitChannels.ToHashSet() : Cfg.LimitedToSeedChannels?.ToHashSet() ?? new HashSet<string>();
      log.Information("Collect - Starting channels update. Limited to ({Included}). Parts ({Parts})",
        explicitChannels.Any() ? explicitChannels.Join("|") : "All", parts == null ? "All" : parts.Join("|"));

      var toDiscover = new List<ChannelUpdatePlan>();
      var explicitMissing = new List<ChannelUpdatePlan>();

      IKeyedCollection<string, ChannelUpdatePlan> existingChannels;
      using (var db = await Db(log)) {
        // retrieve previous channel state to update with new classification (algos and human) and stats form the API
        existingChannels = await db.ChannelUpdateStats(explicitChannels).Then(r => r.KeyBy(c => c.Channel.ChannelId));
        if (limitChannels != null) // discover channels specified in limit if they aren't in our dataset
          explicitMissing.AddRange(limitChannels.Where(c => !existingChannels.ContainsKey(c))
            .Select(c => new ChannelUpdatePlan {Channel = new() {ChannelId = c}, Update = Discover}));

        if (parts.ShouldRun(PDiscover))
          toDiscover.AddRange(await db.DiscoverChannelsViaRecs());
      }

      // perform full update on channels with a last full update older than 90 days (max X at a time because of quota limit).
      var fullUpdate = existingChannels
        .Where(c => c.Channel.Updated == default || c.Channel.Updated - c.Channel.LastFullUpdate > 90.Days())
        .Randomize().Take(200)
        .Select(c => c.Channel.ChannelId).ToHashSet();

      var channels = existingChannels
        .Select(plan => {
          var c = plan.Channel;
          var full = fullUpdate.Contains(c.ChannelId);
          var chanDetail = ExpiredOrInTodaysCycle(c.ChannelId, c.Updated, cycleDays: 7);
          return plan with {
            Update = full ? Full : chanDetail ? Standard : StandardNoChannel,
            VideosFrom = full ? null : plan.VideosFrom // don't limit from date when on a full update
          };
        })
        .Concat(explicitMissing)
        .Concat(toDiscover).ToArray();

      if (!parts.ShouldRun(PChannel)) return channels;

      var (updatedChannels, duration) = await channels
        .Where(c => c.Update != StandardNoChannel)
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
        channelLog.Debug("Collect - {Channel} - channel details ({Update})", c.ChannelTitle, plan.Update.EnumString());
      }
      catch (Exception ex) {
        channelLog.Error(ex, "Collect - {Channel} - Error when updating details for channel : {Error}", c.ChannelTitle, ex.Message);
      }
      return plan with {Channel = c};
    }

    public async Task<YtCollectDbCtx> Db(ILogger log) => new(RCfg, await Sf.Open(log), log);

    [Pipe]
    public async Task<ProcessChannelResults> ProcessChannels(IReadOnlyCollection<ChannelUpdatePlan> channels,
      ExtraPart[] parts, ILogger log = null, CancellationToken cancel = default, DateTime? videoFromExplicit = null) {
      log ??= Logger.None;
      var workSw = Stopwatch.StartNew();

      var results = await channels.Batch(RCfg.ChannelBatchSize).BlockMap(async (planBatch, batchNo) => {
        var channelBatch = planBatch.Select(p => p.Channel).ToArray();

        // to save on db round trips we batch our plan for updates
        var channelPlans = new Dictionary<string, VideoExtraPlans>();
        VideoExtraPlans VideoPlans(string channelId) => channelPlans.GetOrAdd(channelId, () => new());
        void SetPlanPart(string channelId, string videoId, ExtraPart part) => VideoPlans(channelId).SetPart(videoId, part);

        using (var db = await Db(log)) {
          // configure video plans
          if (parts.ShouldRun(EExtra))
            foreach (var u in await db.VideosForUpdate(channelBatch))
              VideoPlans(u.ChannelId).SetForUpdate(u);

          if (parts.ShouldRun(ECaption))
            foreach (var (c, v) in await db.MissingCaptions(channelBatch))
              SetPlanPart(c, v, ECaption);

          if (parts.ShouldRun(EComment))
            foreach (var (c, v) in await db.MissingComments(channelBatch))
              SetPlanPart(c, v, EComment);
        }

        var channelResults = await planBatch
          .Select(p => new {ChannelPlan = p, VideoPlans = channelPlans.TryGet(p.Channel.ChannelId) ?? new VideoExtraPlans()})
          .BlockMap(async (plan, i) => {
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

    async Task UpdateAllInChannel(ILogger log, ChannelUpdatePlan plan, ExtraPart[] parts, VideoExtraPlans plans,
      DateTime? videosFromExplicit = null, CollectPart[] collectParts = null) {
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
      if (collectParts.ShouldRun(PChannelVideos) || parts.ShouldRunAny(ERec, ECaption) || discover && collectParts.ShouldRun(PDiscover)) {
        log.Information("Collect - {Channel} - Starting channel update of videos/recs/captions", c.ChannelTitle);

        // get the oldest date for videos to store updated statistics for. This overlaps so that we have a history of video stats.
        var videosFrom = videosFromExplicit ?? plan.VideosFrom ?? DateTime.UtcNow - RCfg.RefreshVideosWithinNew;
        var videoItemsLimit = discover ? RCfg.DiscoverChannelVids : full ? RCfg.MaxChannelFullVideos : RCfg.MaxChannelDailyVideos;
        var vidsEnum = ChannelVidItems(c, videosFrom, plans, log);
        videoItems = await vidsEnum.Take(videoItemsLimit).ToListAsync();

        if (collectParts.ShouldRun(PChannelVideos))
          await SaveVids(c, videoItems, DbStore.Videos, log);
      }

      if (parts.ShouldRunAny(ERec, ECaption, EComment) || collectParts.ShouldRun(PChannelVideos)) {
        if (parts.ShouldRun(ERec) && !discover)
          plans.SetPart(VideoToUpdateRecs(plan, videoItems), ERec);

        if (parts.ShouldRun(EExtra)) {
          // add videos that seem missing given the current update
          var videoItemsKey = videoItems.KeyBy(v => v.Id);
          var oldestUpdate = videoItems.Min(v => v.UploadDate);

          var suspectMissingFresh = plans
            .Where(v => v.ForUpdate?.UploadDate > oldestUpdate // newer than our oldest update
              && !videoItemsKey.ContainsKey(v.VideoId) // missing from this run
              && v.ForUpdate?.ExtraUpdated.OlderThanOrNull(7.Days()) == true) // haven't tried updating extra for more than 7d
            .Select(v => v.VideoId).ToArray();
          log.Debug("Collect {Channel} - Video-extra for {Videos} video's because they look  missing", c.ChannelTitle, suspectMissingFresh.Length);
          plans.SetPart(suspectMissingFresh, EExtra);

          // videos that we just refreshed but don't have any extra yet
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
          plans.SetPart(videoItems.Where(v => plan.LastCaptionUpdate == null || v.UploadDate > plan.LastCaptionUpdate).Select(v => v.Id), ECaption);

          // take a random sample of comments for new videos since last comment record (NOTE: this is in addition to missing comments have separately been planned)
          if (parts.ShouldRun(EComment))
            plans.SetPart(videoItems
              .Where(v => plan.LastCommentUpdate == null || v.UploadDate > plan.LastCommentUpdate)
              .Randomize().Take(RCfg.MaxChannelComments).Select(v => v.Id), EComment);
        }
        await SaveExtraAndParts(Platform.YouTube, c, parts, log, plans);
      }
    }

    /// <summary>Returns a list of videos that should have recommendations updated.</summary>
    IEnumerable<string> VideoToUpdateRecs(ChannelUpdatePlan plan, List<YtVideoItem> vids) {
      var c = plan.Channel;
      var vidsDesc = vids.OrderByDescending(v => v.UploadDate).ToList();
      var inThisWeeksRecUpdate = ExpiredOrInTodaysCycle(plan.Channel.ChannelId, plan.LastRecUpdate, cycleDays: 7);
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

    /// <summary>true if: a) the lst update is older than max age b) the hash in on a cycle day. E.g. to update once per week
    ///   use cycleDays = 7, and it will hash channels to days</summary>
    static bool ExpiredOrInTodaysCycle(string hashOn, DateTime? lastUpdate, int cycleDays, TimeSpan? maxAge = null) =>
      lastUpdate.OlderThanOrNull(maxAge ?? cycleDays.Days() + 1.Days()) ||
      hashOn.GetHashCode().Abs() % cycleDays == (DateTime.Today - DateTime.UnixEpoch).TotalDays.RoundToInt() % cycleDays;

    static async Task SaveVids(Channel c, IReadOnlyCollection<YtVideoItem> vids, JsonlStore<Video> vidStore, ILogger log) {
      var vidsStored = ToVidsStored(c, vids);
      if (vidsStored.Any())
        await vidStore.Append(vidsStored, log);
      log.Information("Collect - {Channel} - Recorded {VideoCount} videos", c.ChannelTitle, vids.Count);
    }

    async IAsyncEnumerable<YtVideoItem> ChannelVidItems(Channel c, DateTime uploadFrom, VideoExtraPlans plans, ILogger log) {
      long vidCount = 0;
      var chan = await Scraper.Channel(log, c.ChannelId);
      await foreach (var vids in chan.Videos()) {
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

    public IAsyncEnumerable<ExtraAndParts> GetExtras(VideoExtraPlans extras, ILogger log, string channelId = null, string channelTitle = null) {
      var errors = 0;
      var extrasWithParts = extras.Where(e => e.Parts.Any()).ToArray();
      return extrasWithParts
        .BlockMap(async (v, i) => {
            if (i % 100 == 0) log.Debug("YtCollect - recorded {Extras}/{Total} extras", i, extrasWithParts.Length);
            return await Scraper.GetExtra(log, v.VideoId, v.Parts, channelId, channelTitle)
              .Swallow(ex => {
                log.Warning(ex, "error in GetExtra for video {VideoId}", v.VideoId);
                Interlocked.Increment(ref errors);
                if (errors > RCfg.MaxExtraErrorsInChannel)
                  throw ex;
              });
          },
          RCfg.WebParallel);
    }

    public async Task<(VideoExtra[] Extras, Rec[] Recs, VideoComment[] Comments, VideoCaption[] Captions)> SaveExtraAndParts(
      Platform platform, Channel c, ExtraPart[] parts,
      ILogger log, VideoExtraPlans planedExtras) {
      var extrasAndParts = await GetExtras(planedExtras, log, c?.ChannelId, c?.ChannelTitle).NotNull().ToListAsync();
      var s = extrasAndParts.Split();
      var updated = DateTime.UtcNow;
      if (parts.ShouldRun(EExtra)) {
        // provide channel if it is missing 
        var extras = s.Extras.Select(e => e with {ChannelId = e.ChannelId ?? c?.ChannelId, ChannelTitle = c?.ChannelTitle}).ToArray();
        await DbStore.VideoExtra.Append(extras, log);
      }
      if (parts.ShouldRun(ERec)) await DbStore.Recs.Append(ToRecStored(extrasAndParts, updated), log);
      if (parts.ShouldRun(EComment)) await DbStore.Comments.Append(s.Comments, log);
      if (parts.ShouldRun(ECaption)) await DbStore.Captions.Append(s.Captions, log);

      log.Information("Collect - {Channel} - Recorded {WebExtras} web-extras, {Recs} recs, {Comments} comments, {Captions} captions",
        c?.ChannelTitle ?? "(unknown)", s.Extras.Length, s.Recs.Length, s.Comments.Length, s.Captions.Length);

      return s;
    }
  }
}