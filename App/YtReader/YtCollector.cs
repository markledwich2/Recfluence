using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using CsvHelper;
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
using static YtReader.CollectPart;
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

  public enum CollectPart {
    Channel,
    VidStats,
    VidExtra,
    VidRecs,
    Caption,
    DiscoverPart
  }

  public record ChannelUpdatePlan {
    public ChannelStored2    Channel           { get; set; }
    public UpdateChannelType Update            { get; set; }
    public DateTime?         VideosFrom        { get; set; }
    public DateTime?         LastVideoUpdate   { get; set; }
    public DateTime?         LastCaptionUpdate { get; set; }
    public DateTime?         LastRecUpdate     { get; set; }
  }

  public class YtCollector {
    readonly YtClient                    Api;
    readonly AppCfg                      Cfg;
    readonly SnowflakeConnectionProvider Sf;
    readonly IPipeCtx                    PipeCtx;
    readonly WebScraper                  Scraper;
    readonly ChromeScraper               ChromeScraper;
    readonly YtStore                     DbStore;

    public YtCollector(YtStores stores, AppCfg cfg, SnowflakeConnectionProvider sf, IPipeCtx pipeCtx, WebScraper webScraper, ChromeScraper chromeScraper,
      YtClient api, ILogger log) {
      DbStore = new YtStore(stores.Store(DataStoreType.Db), log);
      Cfg = cfg;
      Sf = sf;
      PipeCtx = pipeCtx;
      Scraper = webScraper;
      ChromeScraper = chromeScraper;
      Api = api;
    }

    YtCollectCfg RCfg => Cfg.Collect;

    [Pipe]
    public async Task Collect(ILogger log, string[] limitChannels = null, CollectPart[] parts = null,
      StringPath collectVideoPath = null, CancellationToken cancel = default) {
      if (collectVideoPath != null) {
        log.Information("Collecting video list from {Path}", collectVideoPath);
        IReadOnlyCollection<string> videoIds;
        using (var db = await Sf.OpenConnection(log))
          videoIds = await db.Query<string>("missing video's", @$"
with raw_vids as (
  select $1::string video_id
  from @public.yt_data/{collectVideoPath} (file_format => tsv)
)
   , missing_or_old as (
  select v.video_id
  from raw_vids v
         left join video_extra e on v.video_id=e.video_id
  where not exists(select * from video_extra e where e.video_id = e.video_id and current_date() - e.updated::date < 3)
     or not exists(select * from rec r where r.from_video_id = v.video_id and current_date() - r.updated::date < 3)
)
select video_id from missing_or_old
");
        var (res, dur) = await videoIds
          .Process(PipeCtx, b => ProcessVideos(b, Inject<ILogger>(), Inject<CancellationToken>()), log: log, cancel: cancel)
          .WithDuration();
        log.Information("Collect - {Pipe} Complete - {Total} videos updated in {Duration}",
          nameof(ProcessVideos), res.Count, dur.HumanizeShort());
      }
      else {
        var channels = await PlanAndUpdateChannelStats(parts, limitChannels, log, cancel);
        if (cancel.IsCancellationRequested)
          return;
        var (result, dur) = await channels
          .Randomize() // randomize to even the load
          .Process(PipeCtx,
            b => ProcessChannels(b, parts, Inject<ILogger>(), Inject<CancellationToken>()), log: log, cancel: cancel)
          .WithDuration();

        var allChannelResults = result.Where(r => r.OutState != null).SelectMany(r => r.OutState.Channels).ToArray();
        log.Information("Collect - {Pipe} Complete - {Success}/{Total} channels updated in {Duration}",
          nameof(ProcessChannels), allChannelResults.Count(c => c.Success), allChannelResults.Length, dur.HumanizeShort());
      }
    }

    [Pipe]
    public async Task<bool> ProcessVideos(IReadOnlyCollection<string> videoIds, ILogger log, CancellationToken cancel) {
      log ??= Logger.None;
      const int batchSize = 1000;
      await videoIds.Batch(batchSize).Select((b, i) => (vids: b, i)).BlockAction(async b => {
        var (vids, i) = b;
        await SaveRecsAndExtra(c: null, new[] {VidExtra, VidRecs}, new HashSet<string>(), vids, log);
        log.Information("Collect - Saved extra and recs {Batch}/{TotalBatches} ", i + 1, videoIds.Count / batchSize);
      }, parallel: Cfg.Collect.ParallelChannels, cancel: cancel);
      return true;
    }

    static async IAsyncEnumerable<string> ReadVideoIds(Stream stream) {
      using var zr = new GZipStream(stream, CompressionMode.Decompress);
      using var tr = new StreamReader(zr);
      using var csv = new CsvReader(tr, CultureInfo.InvariantCulture);
      while (await csv.ReadAsync())
        yield return csv.GetField(0);
    }

    static string SqlList<T>(IEnumerable<T> items) => items.Join(",", i => i.ToString().SingleQuote());

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
      var noExplicit = explicitChannels.None();

      IKeyedCollection<string, ChannelUpdatePlan> existingChannels;
      using (var db = await Sf.OpenConnection(log)) {
        // retrieve previous channel state to update with new classification (algos and human) and stats form the API
        existingChannels = (await db.Query<(string j, long? daysBack, DateTime? lastVideoUpdate, DateTime? lastCaptionUpdate, DateTime? lastRecUpdate)>(
            "channels - previous", $@"
with review_filtered as (
  select channel_id, channel_title
  from channel_review
  where meets_review_criteria
  {(noExplicit ? "" : $"and channel_id in ({SqlList(explicitChannels)})")}--only explicit channel when provided
)
   , stage_latest as (
  select v
  from channel_stage -- query from stage because it can be deserialized without modification
  where exists(select * from review_filtered r where r.channel_id=v:ChannelId)
    qualify row_number() over (partition by v:ChannelId::string order by v:Updated::timestamp_ntz desc)=1
)
select coalesce(v, object_construct('ChannelId', r.channel_id)) channel_json
     , b.daily_update_days_back
     , (select max(vs.v:Updated::timestamp_ntz) from video_stage vs where vs.v:ChannelId=r.channel_id) last_video_update
     , (select max(cs.v:Updated::timestamp_ntz) from caption_stage cs where cs.v:ChannelId=r.channel_id) last_caption_update
     , (select max(rs.v:Updated::timestamp_ntz) from rec_stage rs where rs.v:FromChannelId=r.channel_id) last_rec_update
from review_filtered r
       left join stage_latest on v:ChannelId=r.channel_id
       left join channel_collection_days_back b on b.channel_id=v:ChannelId
       left join channel_latest l on v:ChannelId=l.channel_id
"))
          .Select(r => new ChannelUpdatePlan {
            Channel = r.j.ToObject<ChannelStored2>(DbStore.Channels.JCfg),
            VideosFrom = r.daysBack != null ? DateTime.UtcNow - r.daysBack.Value.Days() : (DateTime?) null,
            LastVideoUpdate = r.lastVideoUpdate,
            LastCaptionUpdate = r.lastCaptionUpdate,
            LastRecUpdate = r.lastRecUpdate
          })
          .ToKeyedCollection(r => r.Channel.ChannelId);

        if (parts.ShouldRun(DiscoverPart)) {
          if (limitChannels != null) // discover channels specified in limit if they aren't in our dataset
            toDiscover.AddRange(limitChannels.Where(c => !existingChannels.ContainsKey(c))
              .Select(c => new ChannelUpdatePlan {Channel = new ChannelStored2 {ChannelId = c}, Update = Discover}));
          toDiscover.AddRange(await ChannelsToDiscover(db, log));
        }
      }

      // perform full update on channels with a last full update older than 90 days (max X at a time because of quota limit).
      var fullUpdate = existingChannels
        .Where(c => c.Channel.Updated == default || c.Channel.Updated - c.Channel.LastFullUpdate > 90.Days())
        .Randomize().Take(200)
        .Select(c => c.Channel.ChannelId).ToHashSet();

      var channels = existingChannels
        .Select(c => c with { Update = fullUpdate.Contains(c.Channel.ChannelId) ? Full : Standard })
        .Concat(toDiscover).ToArray();

      if (!parts.ShouldRun(Channel)) return channels;

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
      return plan with { Channel = c };
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
        .Select(c => new ChannelUpdatePlan {
          Channel = new ChannelStored2 {
            ChannelId = c.channel_id,
            ChannelTitle = c.channel_title
          },
          Update = c.source == "review" ? Standard : Discover
        })
        .ToArray();

      return toDiscover;
    }

    [Pipe]
    public async Task<ProcessChannelResults> ProcessChannels(IReadOnlyCollection<ChannelUpdatePlan> channels,
      CollectPart[] parts, ILogger log = null, CancellationToken cancel = default) {
      IEnumerable<IReadOnlyCollection<ChannelStored2>> ChannelBatches() => channels.Select(c => c.Channel).Batch(1000);

      log ??= Logger.None;
      var workSw = Stopwatch.StartNew();

      // to save on db costs, get anything we need in advance of collection
      var channelChromeVideos = new MultiValueDictionary<string, string>();
      var channelDeadVideos = new MultiValueDictionary<string, string>();
      var missingCaptions = new MultiValueDictionary<string, string>();

      if (parts.ShouldRunAny(VidExtra, Caption)) {
        using var db = await Sf.OpenConnection(log);

        if (parts.ShouldRun(VidExtra)) {
          var forChromeUpdate = await ChannelBatches().BlockFunc(c => VideosForChromeUpdate(c, db, log));
          channelChromeVideos = forChromeUpdate.SelectMany()
            .Randomize().Take(RCfg.ChromeUpdateMax)
            .ToMultiValueDictionary(c => c.ChannelId, c => c.VideoId);

          channelDeadVideos = (await ChannelBatches().BlockFunc(c => MissingVideosForExtraUpdate(c, db, log)))
            .SelectMany().ToMultiValueDictionary(v => v.ChannelId, v => v.VideoId);
        }

        if (parts.ShouldRun(Caption))
          missingCaptions = (await ChannelBatches().BlockFunc(channelBatch =>
              db.Query<(string ChannelId, string VideoId)>("missing captions", $@"
select channel_id, video_id
from video_latest v
where not exists(select * from caption_stage c where c.v:VideoId=v.video_id)
  qualify row_number() over (partition by channel_id order by upload_date desc)<=400
    and channel_id in ({channelBatch.Join(",", c => $"'{c.ChannelId}'")})
")))
            .SelectMany().ToMultiValueDictionary(v => v.ChannelId, v => v.VideoId);
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
            await UpdateAllInChannel(plan, parts,
              channelChromeVideos.TryGet(c.ChannelId), channelDeadVideos.TryGet(c.ChannelId), missingCaptions.TryGet(c.ChannelId), cLog);
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

    async Task UpdateAllInChannel(ChannelUpdatePlan plan, CollectPart[] parts, IReadOnlyCollection<string> videosForChromeUpdate,
      IReadOnlyCollection<string> deadVideosForExtraUpdate,
      IReadOnlyCollection<string> missingCaptions,
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

      var discover = plan.Update == Discover; // no need to check this against parts, that is done when planning the update
      var forChromeUpdate = new HashSet<string>();
      var vids = new List<VideoItem>();
      var discoverVids = new List<VideoItem>();
      if (parts.ShouldRunAny(VidStats, VidRecs, Caption) || discover && parts.ShouldRun(DiscoverPart)) {
        log.Information("Collect - {Channel} - Starting channel update of videos/recs/captions", c.ChannelTitle);

        // get the oldest date for videos to store updated statistics for. This overlaps so that we have a history of video stats.
        var uploadedFrom = plan.VideosFrom ?? DateTime.UtcNow - RCfg.RefreshVideosWithinNew;
        var vidsEnum = ChannelVidItems(c, uploadedFrom, log);
        vids = discover ? await vidsEnum.Take(RCfg.DiscoverChannelVids).ToListAsync() : await vidsEnum.ToListAsync();
        if (parts.ShouldRun(VidStats))
          await SaveVids(c, vids, DbStore.Videos, log);
        discoverVids = discover ? vids.OrderBy(v => v.Statistics.ViewCount).Take(RCfg.DiscoverChannelVids).ToList() : null;
        forChromeUpdate = (discover
            ? discoverVids.Select(v => v.Id) // when discovering channels update all using chrome
            : videosForChromeUpdate ?? new string[] { }
          ).ToHashSet();
      }

      if (parts.ShouldRunAny(VidRecs, VidExtra)) {
        var forWebUpdate = new List<string>();
        if (parts.ShouldRun(VidRecs) && !discover)
          forWebUpdate.AddRange(VideoToUpdateRecs(plan, vids).Where(v => !forChromeUpdate.Contains(v)));

        if (parts.ShouldRun(VidExtra) && deadVideosForExtraUpdate?.Any() == true) {
          var ids = vids.Select(v => v.Id).ToHashSet();
          deadVideosForExtraUpdate = deadVideosForExtraUpdate.Where(d => !ids.Contains(d)).ToList();
          log.Debug("Collect -  {Channel} - updating video extra for {Videos} suspected dead videos", c.ChannelTitle, deadVideosForExtraUpdate.Count);
          forWebUpdate.AddRange(deadVideosForExtraUpdate);
        }
        await SaveRecsAndExtra(c, parts, forChromeUpdate, forWebUpdate.ToArray(), log);
      }

      if (parts.ShouldRun(Caption)) {
        var forCaptionSave = discover ? discoverVids : vids;
        await SaveCaptions(plan, forCaptionSave, missingCaptions, log);
      }
    }

    static async Task SaveVids(ChannelStored2 c, IReadOnlyCollection<VideoItem> vids, JsonlStore<VideoStored2> vidStore, ILogger log) {
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

      log.Information("Collect - {Channel} - Recorded {VideoCount} videos", c.ChannelTitle, vids.Count);
    }

    async IAsyncEnumerable<VideoItem> ChannelVidItems(ChannelStored2 c, DateTime uploadFrom, ILogger log) {
      await foreach (var vids in Scraper.GetChannelUploadsAsync(c.ChannelId, log)) {
        foreach (var v in vids)
          yield return v;
        if (vids.Any(v => v.AddedDate < uploadFrom))
          yield break; // return all vids on a page because its free. But stop once we have a page with something older than uploadFrom
      }
    }

    /// <summary>Saves captions for all new videos from the vids list</summary>
    async Task SaveCaptions(ChannelUpdatePlan plan, IEnumerable<VideoItem> vids, IReadOnlyCollection<string> missingCaptions, ILogger log) {
      var channel = plan.Channel;

      async Task<VideoCaptionStored2> GetCaption(string videoId) {
        var videoLog = log.ForContext("VideoId", videoId);
        ClosedCaptionTrack track;
        try {
          var captions = await Scraper.GetCaptionTracks(videoId, log);
          var enInfo = captions.FirstOrDefault(t => t.Language.Code == "en");
          if (enInfo == null)
            return new VideoCaptionStored2 {
              ChannelId = channel.ChannelId,
              VideoId = videoId,
              Updated = DateTime.Now
            };
          track = await Scraper.GetClosedCaptionTrackAsync(enInfo, videoLog);
        }
        catch (Exception ex) {
          ex.ThrowIfUnrecoverable();
          videoLog.Warning(ex, "Unable to get captions for {VideoID}: {Error}", videoId, ex.Message);
          return null;
        }
        return new VideoCaptionStored2 {
          ChannelId = channel.ChannelId,
          VideoId = videoId,
          Updated = DateTime.Now,
          Info = track.Info,
          Captions = track.Captions
        };
      }

      var newCaptions = vids.Where(v => plan.LastCaptionUpdate == null || v.UploadDate > plan.LastCaptionUpdate).Select(v => v.Id).ToArray();

      var toUpdate = newCaptions.Concat(missingCaptions).Distinct().ToArray();

      log.Debug("Collect - {Channel} - about to load {New} new and {Missing} missing captions",
        channel.ChannelTitle, newCaptions.Length, missingCaptions.Count);

      var toStore = (await toUpdate.BlockFunc(GetCaption, RCfg.CaptionParallel)).NotNull().ToArray();
      if (toStore.Any())
        await DbStore.Captions.Append(toStore, log);
      log.Information("Collect - {Channel} - Saved {Captions} captions: {CaptionList}", channel.ChannelTitle, toStore.Length,
        toStore.Select(c => c.VideoId).ToArray());
    }

    /// <summary>Saves recs for all of the given vids</summary>
    async Task SaveRecsAndExtra(ChannelStored2 c, CollectPart[] parts, HashSet<string> forChromeUpdate, IReadOnlyCollection<string> forWebUpdate, ILogger log) {
      var chromeExtra = await ChromeScraper.GetRecsAndExtra(forChromeUpdate, log);
      var webExtra = await Scraper.GetRecsAndExtra(forWebUpdate, log, c?.ChannelId, c?.ChannelTitle);
      var allExtra = chromeExtra.Concat(webExtra).ToArray();
      var extra = allExtra.Select(v => v.Extra).NotNull().ToArray();

      foreach (var e in extra) {
        e.ChannelId ??= c?.ChannelId; // if the video has an error, it may not have picked up the channel
        e.ChannelTitle ??= c?.ChannelTitle;
      }

      var updated = DateTime.UtcNow;
      var recs = new List<RecStored2>();
      if (parts.ShouldRun(VidRecs)) {
        recs.AddRange(ToRecStored(allExtra, updated));
        if (recs.Any())
          await DbStore.Recs.Append(recs, log);
      }

      if (extra.Any())
        await DbStore.VideoExtra.Append(extra, log);

      log.Information("Collect - {Channel} - Recorded {WebExtras} web-extras, {ChromeExtras} chrome-extras, {Recs} recs, {Comments} comments",
        c?.ChannelTitle ?? "(unknown channels)", webExtra.Count, chromeExtra.Count, recs.Count, extra.Sum(e => e.Comments?.Length ?? 0));
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

    async Task<IReadOnlyCollection<(string ChannelId, string VideoId)>> MissingVideosForExtraUpdate(IReadOnlyCollection<ChannelStored2> channels,
      ILoggedConnection<IDbConnection> db,
      ILogger log) {
      var ids = await db.Query<(string ChannelId, string VideoId)>("missing videos", $@"
with chans as (
  select channel_id
  from channel_accepted
  where status_msg<>'Dead'
   and channel_id in ({channels.Join(",", c => $"'{c.ChannelId}'")})
)
   , missing as (
  select v.channel_id
       , v.channel_title
       , v.video_id
       , e.video_id is not null as has_extra
       , v.video_title
       , e.error
       , e.sub_error
       , v.updated
       , v.latest_update
       , v.video_no
       , datediff(d, e.updated, v.latest_update) days_since_extra_update -- check missing only if the video extra is older than the latest update
  from video_missing v
         left join video_extra e on e.video_id=v.video_id

  where (
      e.video_id is null
      or days_since_extra_update>30)
    and exists(select * from chans c where c.channel_id=v.channel_id)
)
   , missing_and_fixes as (
  select channel_id, video_id, 'missing'
  from missing
  union all
  select channel_id, video_id, 'copyright'
  from video_extra e
  where (error like '%copyright%' or sub_error like '%copyright%' or sub_error = 'This video contains content from ')
    and copyright_holder is null
    and e.channel_id is not null
    and exists(select * from chans c where c.channel_id=e.channel_id)
)
select *
from missing_and_fixes
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

    static bool ChannelInTodaysRecsUpdate(ChannelUpdatePlan plan, int cycleDays) =>
      plan.LastRecUpdate == null ||
      DateTime.UtcNow - plan.LastRecUpdate > 8.Days() ||
      plan.Channel.ChannelId.GetHashCode().Abs() % cycleDays == (DateTime.Today - DateTime.UnixEpoch).TotalDays.RoundToInt() % cycleDays;

    /// <summary>Returns a list of videos that should have recommendations updated.</summary>
    IReadOnlyCollection<string> VideoToUpdateRecs(ChannelUpdatePlan plan, IEnumerable<VideoItem> vids) {
      var c = plan.Channel;
      var vidsDesc = vids.OrderByDescending(v => v.UploadDate).ToList();
      var inThisWeeksRecUpdate = ChannelInTodaysRecsUpdate(plan, cycleDays: 7);
      var toUpdate = new List<VideoItem>();
      if (plan.LastRecUpdate == null) {
        Log.Debug("Collect - {Channel} - first rec update, collecting max", c.ChannelTitle);
        toUpdate.AddRange(vidsDesc.Take(RCfg.RefreshRecsMax));
      }
      else if (inThisWeeksRecUpdate) {
        Log.Debug("Collect - {Channel} - performing weekly recs update", c.ChannelTitle);
        toUpdate.AddRange(vidsDesc.Where(v => v.UploadDate.IsYoungerThan(RCfg.RefreshRecsWithin))
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

  public static class YtCollectExtensions {
    public static bool ShouldRunAny(this CollectPart[] parts, params CollectPart[] toRun) => parts == null || toRun.Any(parts.Contains);
    public static bool ShouldRun(this CollectPart[] parts, CollectPart part) => parts == null || parts.Contains(part);
  }
}