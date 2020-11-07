using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
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
using Troschuetz.Random;
using YtReader.Db;
using YtReader.Store;
using YtReader.YtApi;
using YtReader.YtWebsite;
using static Mutuo.Etl.Pipe.PipeArg;
using static YtReader.UpdateChannelType;
using VideoItem = YtReader.YtWebsite.VideoItem;

namespace YtReader {
  public static class RefreshHelper {
    public static bool IsOlderThan(this DateTime updated, TimeSpan age, DateTime? now = null) => (now ?? DateTime.UtcNow) - updated > age;
    public static bool IsYoungerThan(this DateTime updated, TimeSpan age, DateTime? now = null) => !updated.IsOlderThan(age, now);
  }

  public static class YtCollectorRegion {
    static readonly Region[] Regions = {Region.USEast, Region.USWest, Region.USWest2, Region.USEast2, Region.USSouthCentral};
    static readonly TRandom  Rand    = new TRandom();

    public static Region RandomUsRegion() => Rand.Choice(Regions);
  }

  public enum UpdateChannelType {
    /// <summary>Don't update the channel details. Has no impact the collection of videos/recs/caption.</summary>
    None,
    /// <summary>A standard & cheap update to the channel details</summary>
    Standard,
    /// <summary>Update the subscribers and other more costly information about a channel</summary>
    Full,
    /// <summary>Update a un-cassified channels information useful for predicting political/non and tags</summary>
    Discover
  }

  public class ChannelUpdatePlan {
    public ChannelUpdatePlan(ChannelStored2 channel, UpdateChannelType update = Standard, DateTime? videosFrom = null) {
      Channel = channel;
      Update = update;
      VideosFrom = videosFrom;
    }

    public ChannelStored2    Channel    { get; set; }
    public UpdateChannelType Update     { get; set; }
    public DateTime?         VideosFrom { get; set; }
  }

  public class YtCollector {
    readonly YtClient                    Api;
    readonly AppCfg                      Cfg;
    readonly SnowflakeConnectionProvider Sf;
    readonly IPipeCtx                    PipeCtx;
    readonly WebScraper                  Scraper;
    readonly ChromeScraper               ChromeScraper;
    readonly YtStore                     Store;

    public YtCollector(YtStore store, AppCfg cfg, SnowflakeConnectionProvider sf, IPipeCtx pipeCtx, WebScraper webScraper, ChromeScraper chromeScraper,
      YtClient api) {
      Store = store;
      Cfg = cfg;
      Sf = sf;
      PipeCtx = pipeCtx;
      Scraper = webScraper;
      ChromeScraper = chromeScraper;
      Api = api;
    }

    YtCollectCfg RCfg => Cfg.Collect;

    [Pipe]
    public async Task Collect(ILogger log, bool forceUpdate = false, bool disableDiscover = false, string[] limitChannels = null,
      CancellationToken cancel = default) {
      var channels = await PlanAndUpdateChannelStats(disableDiscover, limitChannels, log, cancel);
      if (cancel.IsCancellationRequested)
        return;
      var (result, dur) = await channels
        .Randomize() // randomize to even the load
        .Process(PipeCtx,
          b => ProcessChannels(b, forceUpdate, Inject<ILogger>(), Inject<CancellationToken>()), log: log, cancel: cancel)
        .WithDuration();

      var allChannelResults = result.Where(r => r.OutState != null).SelectMany(r => r.OutState.Channels).ToArray();
      log.Information("Collect - {Pipe} Complete - {Success}/{Total} channels updated in {Duration}",
        nameof(Collect), allChannelResults.Count(c => c.Success), allChannelResults.Length, dur.HumanizeShort());
    }

    static string SqlList<T>(IEnumerable<T> items) => items.Join(",", i => i.ToString().SingleQuote());

    /// <summary>Update channel data from the YouTube API and determine what type of update should be performed on each channel</summary>
    /// <returns></returns>
    async Task<IReadOnlyCollection<ChannelUpdatePlan>> PlanAndUpdateChannelStats(bool disableDiscover, string[] limitChannels,
      ILogger log,
      CancellationToken cancel) {
      var store = Store.Channels;
      var explicitChannels = limitChannels.HasItems() ? limitChannels.ToHashSet() : Cfg.LimitedToSeedChannels?.ToHashSet() ?? new HashSet<string>();
      log.Information("Collect - Starting channels update. Limited to ({Included})",
        explicitChannels.Any() ? explicitChannels.Join("|") : "All");

      var toDiscover = new List<ChannelUpdatePlan>();
      var noExplicit = explicitChannels.None();

      IKeyedCollection<string, ChannelUpdatePlan> existingChannels;
      using (var db = await Sf.OpenConnection(log)) {
        // retrieve previous channel state to update with new classification (algos and human) and stats form the API
        existingChannels = (await db.Query<(string j, long? daysBack)>("channels - previous", $@"with
 latest as (
  select v
  from channel_stage -- query from stage because it can be deserialized without modification
   where exists(select * from channel_accepted c where c.channel_id=v:ChannelId)
   {(noExplicit ? "" : $"and v:ChannelId in ({SqlList(explicitChannels)})")} -- or it is explicit
   qualify row_number() over (partition by v:ChannelId::string order by v:Updated::timestamp_ntz desc)=1
)
select v, b.daily_update_days_back
from latest l
left join channel_collection_days_back b on b.channel_id = v:ChannelId"))
          .Select(r => new ChannelUpdatePlan(r.j.ToObject<ChannelStored2>(Store.Channels.JCfg),
            videosFrom: r.daysBack != null ? DateTime.UtcNow - r.daysBack.Value.Days() : (DateTime?) null))
          .ToKeyedCollection(r => r.Channel.ChannelId);

        if (limitChannels != null) // discover channels specified in limit if they aren't in our dataset
          toDiscover.AddRange(limitChannels.Where(c => !existingChannels.ContainsKey(c))
            .Select(c => new ChannelUpdatePlan(new ChannelStored2 {ChannelId = c}, Discover)));

        if (!disableDiscover) toDiscover.AddRange(await ChannelsToDiscover(db, log));
      }

      // perform full update on channels with a last full update older than 90 days (max X at a time because of quota limit).
      var fullUpdate = existingChannels
        .Where(c => c.Channel.Updated == default || c.Channel.Updated - c.Channel.LastFullUpdate > 90.Days())
        .Randomize().Take(200)
        .Select(c => c.Channel.ChannelId).ToHashSet();

      var channels = existingChannels
        .Select(c => new ChannelUpdatePlan(c.Channel, fullUpdate.Contains(c.Channel.ChannelId) ? Full : Standard, c.VideosFrom))
        .Concat(toDiscover).ToArray();

      var (updatedChannels, duration) = await channels.Where(c => c.Update != None)
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
        c.Updated = DateTime.Now;
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
      return new ChannelUpdatePlan(c, plan.Update, plan.VideosFrom);
    }

    async Task<ChannelUpdatePlan[]> ChannelsToDiscover(ILoggedConnection<IDbConnection> db, ILogger log) {
      var toAdd = await db.Query<(string channel_id, string channel_title, string source)>("channels to classify",
        @"with review_channels as (
  select channel_id
       , channel_title -- probably missing values. reviews without channels don't have titles
  from channel_review r
  where not exists(select * from channel_stage c where c.v:ChannelId::string=r.channel_id)
)
   , rec_channels as (
  select to_channel_id as channel_id, any_value(to_channel_title) as channel_title
  from rec r
  where to_channel_id is not null
    and not exists(select * from channel_stage c where c.v:ChannelId::string=r.to_channel_id)
  group by to_channel_id
)
   , s as (
  select channel_id, channel_title, 'review' as source
  from review_channels sample (:remaining rows)
  union all
  select channel_id, channel_title, 'rec' as source
  from rec_channels sample (:remaining rows)
)
select *
from s
limit :remaining", param: new {remaining = RCfg.DiscoverChannels});

      log.Debug("Collect - found {Channels} new channels for discovery", toAdd.Count);

      var toDiscover = toAdd
        .Select(c => new ChannelUpdatePlan(new ChannelStored2 {
          ChannelId = c.channel_id,
          ChannelTitle = c.channel_title
        }, c.source == "review" ? Standard : Discover)).ToArray();

      return toDiscover;
    }

    [Pipe]
    public async Task<ProcessChannelResults> ProcessChannels(IReadOnlyCollection<ChannelUpdatePlan> channels,
      bool forceUpdate, ILogger log = null, CancellationToken cancel = default) {
      log ??= Logger.None;
      var workSw = Stopwatch.StartNew();

      // to save on db costs, get anything we need in advance of collection
      MultiValueDictionary<string, string> channelChromeVideos;
      MultiValueDictionary<string, string> channelDeadVideos;
      using (var db = await Sf.OpenConnection(log)) {
        var forChromeUpdate = await channels.Select(c => c.Channel).Batch(1000)
          .BlockFunc(c => VideosForChromeUpdate(c, db, log));
        channelChromeVideos = forChromeUpdate.SelectMany()
          .Randomize().Take(RCfg.ChromeUpdateMax)
          .ToMultiValueDictionary(c => c.ChannelId, c => c.VideoId);
        
        channelDeadVideos = (await channels.Select(c => c.Channel).Batch(1000)
          .BlockFunc(c => DeadVideosForExtraUpdate(c, db, log)))
          .SelectMany().ToMultiValueDictionary(v => v.ChannelId, c => c.VideoId);
      }

      var channelResults = await channels
        .Select((c, i) => (c, i))
        .BlockFunc(async item => {
          var (plan, i) = item;
          var c = plan.Channel;
          var sw = Stopwatch.StartNew();
          var cLog = log
            .ForContext("ChannelId", c.ChannelId)
            .ForContext("Channel", c.ChannelTitle);
          try {
            await using var conn = new Defer<ILoggedConnection<IDbConnection>>(async () => await Sf.OpenConnection(cLog));
            await UpdateAllInChannel(plan, channelChromeVideos.TryGet(c.ChannelId), channelDeadVideos.TryGet(c.ChannelId), cLog);
            cLog.Information("Collect - {Channel} - Completed videos/recs/captions in {Duration}. Progress: channel {Count}/{BatchTotal}",
              c.ChannelTitle, sw.Elapsed.HumanizeShort(), i + 1, channels.Count);
            return (c, Success: true);
          }
          catch (Exception ex) {
            ex.ThrowIfUnrecoverable();
            cLog.Error(ex, "Collect - Error updating channel {Channel}: {Error}", c.ChannelTitle, ex.Message);
            return (c, Success: false);
          }
        }, RCfg.ParallelChannels, cancel: cancel);

      var res = new ProcessChannelResults {
        Channels = channelResults.Select(r => new ProcessChannelResult {ChannelId = r.c.ChannelId, Success = r.Success}).ToArray(),
        Duration = workSw.Elapsed
      };

      log.Information(
        "Collect - {Pipe} complete - {ChannelsComplete} channel videos/captions/recs, {ChannelsFailed} failed {Duration}",
        nameof(ProcessChannels), channelResults.Count(c => c.Success), channelResults.Count(c => !c.Success), res.Duration);

      return res;
    }

    async Task UpdateAllInChannel(ChannelUpdatePlan plan, IReadOnlyCollection<string> videosForChromeUpdate, IReadOnlyCollection<string> deadVideosForExtraUpdate,
      ILogger log) {
      var c = plan.Channel;

      void NotUpdatingLog(string reason) =>
        log.Information("Collect - {Channel} - Not updating videos/recs/captions because: {Reason} ",
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

      var md = await Store.Videos.LatestFile(c.ChannelId);
      var lastUpload = md?.Ts?.ParseFileSafeTimestamp();
      log.Information("Collect - {Channel} - Starting channel update of videos/recs/captions. Last video stage {LastUpload}",
        c.ChannelTitle, lastUpload);

      // get the oldest date for videos to store updated statistics for. This overlaps so that we have a history of video stats.
      var discover = plan.Update == Discover;
      var uploadedFrom = plan.VideosFrom ?? DateTime.UtcNow - (lastUpload == null ? RCfg.RefreshVideosWithinNew : RCfg.RefreshVideosWithinDaily);
      var vidsEnum = ChannelVidItems(c, uploadedFrom, log);
      var vids = discover ? await vidsEnum.Take(RCfg.DiscoverChannelVids).ToListAsync() : await vidsEnum.ToListAsync();
      await SaveVids(c, vids, Store.Videos, lastUpload, log);
      var discoverVids = discover ? vids.OrderBy(v => v.Statistics.ViewCount).Take(RCfg.DiscoverChannelVids).ToList() : null;
      var forChromeUpdate = (discover
          ? discoverVids.Select(v => v.Id) // when discovering channels update all using chrome
          : videosForChromeUpdate ?? new string[] { }
        ).ToHashSet();

      var forWebUpdate = new List<string>();
      if (!discover)
          forWebUpdate.AddRange((await VideoToUpdateRecs(c, vids)).Where(v => !forChromeUpdate.Contains(v)));
      if (deadVideosForExtraUpdate?.Any() == true) {
        log.Debug("Collect -  {Channel} - updating video extra for {Videos} suspected dead videos", c.ChannelTitle, deadVideosForExtraUpdate.Count);
        forWebUpdate.AddRange(deadVideosForExtraUpdate);
      }
      
      await SaveRecsAndExtra(c, forChromeUpdate, forWebUpdate.ToArray(), log);
      var forCaptionSave = discover ? discoverVids : vids;
      await SaveNewCaptions(c, forCaptionSave, log);
    }

    static bool ChannelInTodaysUpdate(ChannelStored2 c, int cycleDays) =>
      c.ChannelId.GetHashCode() % cycleDays == (DateTime.Today - DateTime.UnixEpoch).TotalDays.RoundToInt() % cycleDays;

    static async Task SaveVids(ChannelStored2 c, IReadOnlyCollection<VideoItem> vids, JsonlStore<VideoStored2> vidStore, DateTime? uploadedFrom,
      ILogger log) {
      var updated = DateTime.UtcNow;
      var vidsStored = vids.Select(v => new VideoStored2 {
        VideoId = v.Id,
        Title = v.Title,
        Description = v.Description,
        Duration = v.Duration,
        Keywords = v.Keywords.ToList(),
        Statistics = v.Statistics,
        ChannelId = c.ChannelId,
        ChannelTitle = c.ChannelTitle,
        UploadDate = v.UploadDate,
        AddedDate = v.AddedDate,
        Updated = updated
      }).ToList();

      if (vidsStored.Count > 0)
        await vidStore.Append(vidsStored, log);

      var newVideos = vidsStored.Count(v => uploadedFrom == null || v.UploadDate > uploadedFrom);

      log.Information("Collect - {Channel} - Recorded {VideoCount} videos. {NewCount} new, {UpdatedCount} updated",
        c.ChannelTitle, vids.Count, newVideos, vids.Count - newVideos);
    }

    async IAsyncEnumerable<VideoItem> ChannelVidItems(ChannelStored2 c, DateTime uploadFrom, ILogger log) {
      await foreach (var vids in Scraper.GetChannelUploadsAsync(c.ChannelId, log)) {
        foreach (var v in vids)
          yield return v;
        if (vids.Any(v => v.AddedDate < uploadFrom)) 
          yield break;// return all vids on a page because its free. But stop once we have a page with something older than uploadFrom
      }
    }

    static int MaxConsecutiveCaptionsMissing = 10;

    /// <summary>Saves captions for all new videos from the vids list</summary>
    async Task SaveNewCaptions(ChannelStored2 channel, IEnumerable<VideoItem> vids, ILogger log) {
      var lastUpload =
        (await Store.Captions.LatestFile(channel.ChannelId))?.Ts.ParseFileSafeTimestamp(); // last video upload in this channel partition we have captions for

      var consecutiveCaptionMissing = 0;
      
      async Task<VideoCaptionStored2> GetCaption(VideoItem v) {
        if (consecutiveCaptionMissing >= MaxConsecutiveCaptionsMissing) return null;
        var videoLog = log.ForContext("VideoId", v.Id);
        ClosedCaptionTrack track;
        try {
          var captions = await Scraper.GetCaptions(v.Id, log);
          var enInfo = captions.FirstOrDefault(t => t.Language.Code == "en");
          if (enInfo == null) {
            if (Interlocked.Increment(ref consecutiveCaptionMissing) == MaxConsecutiveCaptionsMissing)
              log.Debug("SaveCaptions - too many consecutive videos are missing captions. Assuming it won't have any.");
            return null;
          }
          track = await Scraper.GetClosedCaptionTrackAsync(enInfo, videoLog);
          consecutiveCaptionMissing = 0;
        }
        catch (Exception ex) {
          ex.ThrowIfUnrecoverable();
          log.Warning(ex, "Unable to get captions for {VideoID}: {Error}", v.Id, ex.Message);
          return null;
        }

        return new VideoCaptionStored2 {
          ChannelId = channel.ChannelId,
          VideoId = v.Id,
          UploadDate = v.UploadDate,
          Updated = DateTime.Now,
          Info = track.Info,
          Captions = track.Captions
        };
      }

      var captionsToStore =
        (await vids.Where(v => lastUpload == null || v.UploadDate > lastUpload)
          .BlockFunc(GetCaption, RCfg.CaptionParallel)).NotNull().ToList();

      if (captionsToStore.Any())
        await Store.Captions.Append(captionsToStore, log);

      log.Information("Collect - {Channel} - Saved {Captions} captions", channel.ChannelTitle, captionsToStore.Count);
    }

    /// <summary>Saves recs for all of the given vids</summary>
    async Task SaveRecsAndExtra(ChannelStored2 c, HashSet<string> forChromeUpdate, string[] forWebUpdate, ILogger log) {
      var chromeExtra = await ChromeScraper.GetRecsAndExtra(forChromeUpdate, log);
      var webExtra = await Scraper.GetRecsAndExtra(forWebUpdate, log, c.ChannelId, c.ChannelTitle);
      var allExtra = chromeExtra.Concat(webExtra).ToArray();
      var extra = allExtra.Select(v => v.Extra).NotNull().ToArray();

      foreach (var e in extra) {
        e.ChannelId ??= c.ChannelId; // if the video has an error, it may not have picked up the channel
        e.ChannelTitle ??= c.ChannelTitle;
      }

      var updated = DateTime.UtcNow;
      var recs = ToRecStored(allExtra, updated);

      if (recs.Any())
        await Store.Recs.Append(recs, log);

      if (extra.Any())
        await Store.VideoExtra.Append(extra, log);

      log.Information("Collect - {Channel} - Recorded {WebExtras} web-extras, {ChromeExtras} chrome-extras, {Recs} recs, {Comments} comments",
        c.ChannelTitle, webExtra.Count, chromeExtra.Count, recs.Length, extra.Sum(e => e.Comments?.Length ?? 0));
    }

    public static RecStored2[] ToRecStored(RecsAndExtra[] allExtra, DateTime updated) =>
      allExtra
        .SelectMany(v => v.Recs?.Select((r, i) => new RecStored2 {
          FromChannelId = v.Extra.ChannelId,
          FromVideoId = v.Extra.VideoId,
          FromVideoTitle = v.Extra.Title,
          ToChannelTitle = r.ToChannelTitle,
          ToChannelId = r.ToChannelId,
          ToVideoId = r.ToVideoId,
          ToVideoTitle = r.ToVideoTitle,
          Rank = r.Rank,
          Source = r.Source,
          ForYou = r.ForYou,
          ToViews = r.ToViews,
          ToUploadDate = r.ToUploadDate,
          Updated = updated
        }) ?? new RecStored2[] { }).ToArray();


    async Task<IReadOnlyCollection<(string ChannelId, string VideoId)>> DeadVideosForExtraUpdate(IReadOnlyCollection<ChannelStored2> channels,
      ILoggedConnection<IDbConnection> db,
      ILogger log) {
      var ids = await db.Query<(string ChannelId, string VideoId)>("missing videos", $@"
select v.channel_id, v.video_id from video_missing v
left join video_extra_latest e on e.video_id = v.video_id
where e.error is null and
v.channel_id in ({channels.Join(",", c => $"'{c.ChannelId}'")})
");
      return ids;
    }

    /// <summary>Find videos that we should update to collect comments (chrome update). We do this once x days after a video is
    ///   uploaded.</summary>
    async Task<IReadOnlyCollection<(string ChannelId, string VideoId)>> VideosForChromeUpdate(IReadOnlyCollection<ChannelStored2> channels,
      ILoggedConnection<IDbConnection> db,
      ILogger log) {
      var ids = await db.Query<(string ChannelId, string VideoId)>("videos sans-comments",
        $@"with chrome_extra_latest as (
  select video_id
       , updated
       , row_number() over (partition by video_id order by updated desc) as age_no
       , count(1) over (partition by video_id) as extras
  from video_extra v
  where source='Chrome'
    qualify age_no=1
)
   , videos_to_update as (
  select *
       , row_number() over (partition by channel_id order by views desc) as channel_rank
  from (
         select v.video_id
              , v.channel_id
              , v.views
              , datediff(d, e.updated, convert_timezone('UTC', current_timestamp())) as extra_ago
              , datediff(d, v.upload_date, convert_timezone('UTC', current_timestamp())) as upload_ago

         from video_latest v
                left join chrome_extra_latest e on e.video_id=v.video_id
         where v.channel_id in ({channels.Join(",", c => $"'{c.ChannelId}'")})
           and upload_ago>7
           and e.updated is null -- update only 7 days after being uploaded
       )
    qualify channel_rank<=:videos_per_channel
)
select channel_id, video_id
from videos_to_update",
        param: new {videos_per_channel = RCfg.PopulateMissingCommentsLimit});
      return ids;
    }

    async Task<IReadOnlyCollection<string>> VideoToUpdateRecs(ChannelStored2 c, IEnumerable<VideoItem> vids) {
      var prevUpdateMeta = await Store.Recs.LatestFile(c.ChannelId);
      var prevUpdate = prevUpdateMeta?.Ts.ParseFileSafeTimestamp();
      var vidsDesc = vids.OrderByDescending(v => v.UploadDate).ToList();
      var inThisWeeksRecUpdate = ChannelInTodaysUpdate(c, cycleDays: 7);

      var toUpdate = new List<VideoItem>();
      if (prevUpdate == null) {
        Log.Debug("Collect - {Channel} - first rec update, collecting max", c.ChannelTitle);
        toUpdate.AddRange(vidsDesc.Take(RCfg.RefreshRecsMax));
      }
      else if (inThisWeeksRecUpdate) {
        Log.Debug("Collect - {Channel} - performing weekly recs update", c.ChannelTitle);
        toUpdate.AddRange(vidsDesc.Where(v => v.UploadDate > prevUpdate || v.UploadDate.IsYoungerThan(RCfg.RefreshRecsWithin))
          .Take(RCfg.RefreshRecsMax));
        var deficit = RCfg.RefreshRecsMin - toUpdate.Count;
        if (deficit > 0)
          toUpdate.AddRange(vidsDesc.Where(v => toUpdate.All(u => u.Id != v.Id))
            .Take(deficit)); // if we don't have new videos, refresh the min amount by adding videos 
      }
      else {
        Log.Debug("Collect - {Channel} - skipping rec update because it's not this channels day", c.ChannelTitle);
      }

      return toUpdate.Select(v => v.Id).ToList();
    }

    public class ProcessChannelResult {
      public string ChannelId { get; set; }
      public bool   Success   { get; set; }
    }

    public class ProcessChannelResults {
      public ProcessChannelResult[] Channels { get; set; }
      public TimeSpan               Duration { get; set; }
    }
  }
}