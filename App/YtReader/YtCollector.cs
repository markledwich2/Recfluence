using System;
using System.Collections.Generic;
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
using YtReader.YtApi;
using YtReader.YtWebsite;
using static Mutuo.Etl.Pipe.PipeArg;
using static YtReader.Store.ChannelReviewStatus;
using VideoItem = YtReader.YtWebsite.VideoItem;

namespace YtReader {
  public static class RefreshHelper {
    public static bool IsOlderThan(this DateTime updated, TimeSpan age, DateTime? now = null) => (now ?? DateTime.UtcNow) - updated > age;
    public static bool IsYoungerThan(this DateTime updated, TimeSpan age, DateTime? now = null) => !updated.IsOlderThan(age, now);
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
      var channels = await UpdateAllChannels(disableDiscover, limitChannels, log, cancel);
      if (cancel.IsCancellationRequested)
        return;
      var (result, dur) = await channels
        .Randomize() // randomize to even the load. Without this the large channels at the beginning make the first batch go way slower
        .Process(PipeCtx,
          b => ProcessChannels(b, forceUpdate, Inject<ILogger>(), Inject<CancellationToken>()), log: log, cancel: cancel)
        .WithDuration();

      var allChannelResults = result.SelectMany(r => r.OutState.Channels).ToArray();
      log.Information("Collect - {Pipe} Complete - {Success}/{Total} channels updated in {Duration}",
        nameof(Collect), allChannelResults.Count(c => c.Success), allChannelResults.Length, dur.HumanizeShort());
    }

    async Task<IEnumerable<ChannelStored2>> UpdateAllChannels(bool disableDiscover, string[] limitChannels, ILogger log, CancellationToken cancel) {
      var store = Store.Channels;
      var limitChannelHash = limitChannels.HasItems() ? limitChannels.ToHashSet() : Cfg.LimitedToSeedChannels?.ToHashSet() ?? new HashSet<string>();
      log.Information("Collect - Starting channels update. Limited to ({Included})",
        limitChannelHash.Any() ? limitChannelHash.Join("|") : "All");

      IKeyedCollection<string, ChannelStored2> channelNotRejected;
      ChannelStored2[] toDiscover;
      using (var db = await Sf.OpenConnection(log)) {
        var channelLimitSql = limitChannelHash.Any() ? $" and v:ChannelId::string in ({limitChannelHash.Join(",", c => $"'{c}'")})" : "";
        // retrieve previous channel state to update with new classification (algos and human) and stats form the API
        channelNotRejected = (await db.Query<string>("channels - previous", $@"select v
from channel_stage
where v:Status::string is null or v:Status::string not in ('ManualRejected', 'AlgoRejected') {channelLimitSql}
qualify row_number() over (partition by v:ChannelId::string order by v:Updated::timestamp_ntz desc) = 1"))
          .Select(s => s.ToObject<ChannelStored2>(Store.Channels.JCfg)).ToKeyedCollection(c => c.ChannelId);
        toDiscover = disableDiscover ? new ChannelStored2[] { } : await ChannelsToDiscover(db, channelNotRejected, log);
      }

      var toUpdate = channelNotRejected.Where(c => c.ReviewStatus.Accepted())
        .Where(c => limitChannelHash.IsEmpty() || limitChannelHash.Contains(c.ChannelId))
        .ToArray();

      // perform full update on channels older than 30 days (max X at a time because of quota limit).
      var fullUpdate = toUpdate
        .Where(c => c.Updated == default || c.Updated - c.LastFullUpdate > 30.Days())
        .Take(600)
        .Select(c => c.ChannelId).ToHashSet();

      var (channels, dur) = await toUpdate.Concat(toDiscover)
        .BlockFunc(async c => await UpdateChannel(c, fullUpdate.Contains(c.ChannelId), log), Cfg.DefaultParallel, cancel: cancel,
          progressUpdate: p => log.Debug("Collect - Reading channels {ChannelCount}/{ChannelTotal}", p.CompletedTotal, toUpdate.Length))
        .WithDuration();

      if (cancel.IsCancellationRequested) return channels;

      if (channels.Any())
        await store.Append(channels, log);

      log.Information("Collect - Updated stats for {Channels} existing and {Discovered} discovered channels in {Duration}",
        toUpdate.Length, toDiscover.Length, dur);

      return channels;
    }

    async Task<ChannelStored2> UpdateChannel(ChannelStored2 channel, bool full, ILogger log) {
      var channelLog = log.ForContext("Channel", channel.ChannelId).ForContext("ChannelId", channel.ChannelId);
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
        channelLog.Information("Collect - {Channel} - read {Full} channel details ({ReviewStatus})",
          c.ChannelTitle, full ? "full" : "simple", c.ReviewStatus.EnumString());
      }
      catch (Exception ex) {
        channelLog.Error(ex, "Collect - {Channel} - Error when updating details for channel : {Error}", c.ChannelTitle, ex.Message);
      }
      return c;
    }

    ChannelStored2 ChannelStored(ChannelSheet sheet, IKeyedCollection<string, ChannelStored2> channelPev) {
      var stored = channelPev[sheet.Id] ?? new ChannelStored2 {
        ChannelId = sheet.Id,
        ChannelTitle = sheet.Title,
        HardTags = sheet.HardTags,
        SoftTags = sheet.SoftTags,
        Relevance = sheet.Relevance,
        LR = sheet.LR,
        MainChannelId = sheet.MainChannelId,
        UserChannels = sheet.UserChannels,
        ReviewStatus = ManualAccepted
      };
      return stored;
    }

    async Task<ChannelStored2[]> ChannelsToDiscover(LoggedConnection db, IKeyedCollection<string, ChannelStored2> channelPev, ILogger log) {
      var pendingSansVideo = await db.Query<(string channel_id, string channel_title)>("channels pending classification",
        @"select distinct channel_id, channel_title
from channel_latest c
where review_status in ('Pending')
  and not exists(select * from video_extra_stage e where e.v:ChannelId::string=c.channel_id)
limit :DiscoverChannels
        ", param: new {RCfg.DiscoverChannels});

      log.Debug("Collect - found {Channels} channels pending discovery", pendingSansVideo.Count);

      var remaining = RCfg.DiscoverChannels - pendingSansVideo.Count;
      var newDiscover = remaining > 0
        ? await db.Query<(string channel_id, string channel_title)>("channels to classify",
          @"with sam as (
  select $1::string as channel_id, $2::int as subs, $3::int as not_sure, $4::double as political, $5::string as channel_title
  from (@yt_data/import/samclark_is_political_all_preds_20200707.ts (file_format => tsv)) s
  where political>0.9
    and subs>10000
)
   , sam_filtered as (
  select channel_id, channel_title
  from sam
  --where not exists(select * from channel_stage c where c.v:ChannelId::string=s.$1)
)
   , rec_channels as (
  select to_channel_id as channel_id, any_value(to_channel_title) as channel_title
  from rec
  where to_channel_id is not null
  group by to_channel_id
)
, s as (
  select channel_id, channel_title, 'sam' as source
  from sam_filtered sample (:remaining rows)
  union all
  select channel_id, channel_title, 'rec' as source
  from rec_channels sample (:remaining rows)
)
select * from s limit :remaining", param: new {remaining})
        : new (string channel_id, string channel_title)[] { };

      log.Debug("Collect - found {Channels} new channels for discovery", newDiscover.Count);

      var toDiscover = pendingSansVideo.Concat(newDiscover)
        .Select(c => channelPev[c.channel_id] ?? new ChannelStored2 {
          ChannelId = c.channel_id,
          ChannelTitle = c.channel_title,
          ReviewStatus = Pending
        }).ToArray();

      return toDiscover;
    }

    [Pipe]
    public async Task<ProcessChannelResults> ProcessChannels(IReadOnlyCollection<ChannelStored2> channels,
      bool forceUpdate, ILogger log = null, CancellationToken cancel = default) {
      log ??= Logger.None;
      var workSw = Stopwatch.StartNew();


      var toUpdate = channels
        .Where(c => c.Status == ChannelStatus.Alive &&
                    c.ReviewStatus.In(Pending, AlgoAccepted, ManualAccepted)).ToArray();

      // to save on db costs, get anything we need in advance of collection
      IDictionary<string, string[]> channelChromeVideos;
      using (var db = await Sf.OpenConnection(log)) {
        var forChromeUpdate = await toUpdate.Where(u => u.ReviewStatus != Pending)
          .BlockFunc(async c => new {c.ChannelId, ForChromeUpdate = await VideosForChromeUpdate(c, db, log)});
        channelChromeVideos = forChromeUpdate.ToDictionary(c => c.ChannelId, c => c.ForChromeUpdate.ToArray());
      }

      var channelResults = await toUpdate
        .Select((c, i) => (c, i))
        .BlockFunc(async item => {
          var (c, i) = item;
          var sw = Stopwatch.StartNew();
          log = log
            .ForContext("ChannelId", c.ChannelId)
            .ForContext("Channel", c.ChannelTitle);
          try {
            await using var conn = new Defer<LoggedConnection>(() => Sf.OpenConnection(log));
            await UpdateAllInChannel(c, forceUpdate, channelChromeVideos.TryGet(c.ChannelId), log);
            log.Information("Collect - {Channel} - Completed videos/recs/captions in {Duration}. Progress: channel {Count}/{BatchTotal}",
              c.ChannelTitle, sw.Elapsed.HumanizeShort(), i + 1, channels.Count);
            return (c, Success: true);
          }
          catch (Exception ex) {
            log.Error(ex, "Collect - Error updating channel {Channel}: {Error}", c.ChannelTitle, ex.Message);
            return (c, Success: false);
          }
        }, RCfg.ParallelChannels, cancel: cancel);

      var res = new ProcessChannelResults {
        Channels = channelResults.Select(r => new ProcessChannelResult {ChannelId = r.c.ChannelId, Success = r.Success}).ToArray(),
        Duration = workSw.Elapsed
      };

      log.Information(
        "Collect - {Pipe} complete - {ChannelsComplete} channel videos/captions/recs, {ChannelsFailed} failed in {Duration}",
        nameof(ProcessChannels), channelResults.Count(c => c.Success), channelResults.Count(c => !c.Success), res.Duration);

      return res;
    }

    async Task UpdateAllInChannel(ChannelStored2 c, bool forceUpdate, string[] videosForChromeUpdate, ILogger log) {
      if (c.StatusMessage.HasValue()) {
        log.Information("Collect - {Channel} - Not updating videos/recs/captions because it has a status msg: {StatusMessage} ",
          c.ChannelTitle, c.StatusMessage);
        return;
      }

      var md = await Store.Videos.LatestFile(c.ChannelId);
      var lastUpload = md?.Ts?.ParseFileSafeTimestamp();

      log.Information("Collect - {Channel} - Starting channel update of videos/recs/captions. Last video stage {LastUpload}",
        c.ChannelTitle, lastUpload);

      var lastModified = md?.Modified;
      var recentlyUpdated = !forceUpdate && lastModified != null && lastModified.Value.IsYoungerThan(RCfg.RefreshAllAfter);
      if (recentlyUpdated) {
        log.Information("Collect - {Channel} - skipping update, video stats have been updated recently {LastModified}", c.ChannelTitle, lastModified);
        return;
      }
      var discover = c.ReviewStatus == Pending;
      if (discover && lastModified != null) {
        log.Information("Collect - {Channel} - skipping update of pending channel, already populated videos", c.ChannelTitle, lastModified);
        return;
      }

      // get the oldest date for videos to store updated statistics for. This overlaps so that we have a history of video stats.
      var uploadedFrom = md == null ? RCfg.From : DateTime.UtcNow - RCfg.RefreshVideosWithin;
      var vids = await ChannelVidItems(c, uploadedFrom, discover ? RCfg.DiscoverChannelVids : (int?) null, log).ToListAsync();
      await SaveVids(c, vids, Store.Videos, lastUpload, log);

      var discoverVids = discover ? vids.OrderBy(v => v.Statistics.ViewCount).Take(RCfg.DiscoverChannelVids).ToList() : null;
      var forChromeUpdate = (discover
          ? discoverVids.Select(v => v.Id) // when discovering channels update all using chrome
          : videosForChromeUpdate ?? new string[] { }
        ).ToHashSet();
      var forWebUpdate = discover ? new string[] { } : (await VideoToUpdateRecs(c, vids)).Where(v => !forChromeUpdate.Contains(v)).ToArray();

      await SaveRecsAndExtra(c, forChromeUpdate, forWebUpdate, log);
      var forCaptionSave = discover ? discoverVids : vids;
      await SaveNewCaptions(c, forCaptionSave, log);
    }

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
        Thumbnail = VideoThumbnail.FromVideoId(v.Id),
        ChannelId = c.ChannelId,
        ChannelTitle = c.ChannelTitle,
        UploadDate = v.UploadDate.UtcDateTime,
        Updated = updated
      }).ToList();

      if (vidsStored.Count > 0)
        await vidStore.Append(vidsStored, log);

      var newVideos = vidsStored.Count(v => uploadedFrom == null || v.UploadDate > uploadedFrom);

      log.Information("Collect - {Channel} - Recorded {VideoCount} videos. {NewCount} new, {UpdatedCount} updated",
        c.ChannelTitle, vids.Count, newVideos, vids.Count - newVideos);
    }

    async IAsyncEnumerable<VideoItem> ChannelVidItems(ChannelStored2 c, DateTime uploadFrom, int? max, ILogger log) {
      var count = 0;
      await foreach (var vids in Scraper.GetChannelUploadsAsync(c.ChannelId, log))
      foreach (var v in vids) {
        count++;
        if (v.UploadDate.UtcDateTime > uploadFrom && (max == null || count <= max)) yield return v;
        else yield break;
      }
    }

    /// <summary>Saves captions for all new videos from the vids list</summary>
    async Task SaveNewCaptions(ChannelStored2 channel, IEnumerable<VideoItem> vids, ILogger log) {
      var lastUpload =
        (await Store.Captions.LatestFile(channel.ChannelId))?.Ts.ParseFileSafeTimestamp(); // last video upload in this channel partition we have captions for

      async Task<VideoCaptionStored2> GetCaption(VideoItem v) {
        var videoLog = log.ForContext("VideoId", v.Id);

        ClosedCaptionTrack track;
        try {
          var captions = await Scraper.GetCaptions(v.Id, log);
          var enInfo = captions.FirstOrDefault(t => t.Language.Code == "en");
          if (enInfo == null) return null;
          track = await Scraper.GetClosedCaptionTrackAsync(enInfo, videoLog);
        }
        catch (Exception ex) {
          log.Warning(ex, "Unable to get captions for {VideoID}: {Error}", v.Id, ex.Message);
          return null;
        }

        return new VideoCaptionStored2 {
          ChannelId = channel.ChannelId,
          VideoId = v.Id,
          UploadDate = v.UploadDate.UtcDateTime,
          Updated = DateTime.Now,
          Info = track.Info,
          Captions = track.Captions
        };
      }

      var captionsToStore =
        (await vids.Where(v => lastUpload == null || v.UploadDate.UtcDateTime > lastUpload)
          .BlockFunc(GetCaption, Cfg.DefaultParallel)).NotNull().ToList();

      if (captionsToStore.Any())
        await Store.Captions.Append(captionsToStore, log);

      log.Information("Collect - {Channel} - Saved {Captions} captions", channel.ChannelTitle, captionsToStore.Count);
    }

    /// <summary>Saves recs for all of the given vids</summary>
    async Task SaveRecsAndExtra(ChannelStored2 c, HashSet<string> forChromeUpdate, string[] forWebUpdate, ILogger log) {
      var chromeExtra = await ChromeScraper.GetRecsAndExtra(forChromeUpdate, log);
      var webExtra = await Scraper.GetRecsAndExtra(forWebUpdate, log);
      var allExtra = chromeExtra.Concat(webExtra).ToArray();
      var extra = allExtra.Select(v => v.Extra).NotNull().ToArray();
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

    async Task<IReadOnlyCollection<string>> VideosForChromeUpdate(ChannelStored2 c, LoggedConnection db, ILogger log) {
      // use chrome when its +1 week old and +1 month old. We should get most of the comments that way
      var ids = await db.Query<string>("videos sans-comments",
        @"with chrome_extra_latest as (
  select video_id
       , updated
       , channel_id
       , error
       , source
       , views
       , row_number() over (partition by video_id order by updated desc) as age_no
       , count(1) over (partition by video_id) as extras
  from video_extra v
  where channel_id = :channel_id
        and source = 'Chrome'
    qualify age_no = 1
)
   , videos_to_update as (
  select v.video_id
       , v.video_title
       , v.upload_date
       , e.extras
       , e.source
       , e.views
       , datediff(d, e.updated, convert_timezone('UTC', current_timestamp())) as extra_ago
  from video_latest v
         left join chrome_extra_latest e on e.video_id = v.video_id
  where v.channel_id = :channel_id
    and (e.updated is null
    or extra_ago > 7 and extras <= 1
    or extra_ago > 30 and extras <= 2)
)
select video_id
from videos_to_update
order by views desc
limit :limit",
        param: new {channel_id = c.ChannelId, limit = RCfg.PopulateMissingCommentsLimit});
      log.Information("Collect - {Channel} - found {Videos} in need of chrome update", c.ChannelTitle, ids.Count);
      return ids;
    }

    async Task<IReadOnlyCollection<string>> VideoToUpdateRecs(ChannelStored2 c, IEnumerable<VideoItem> vids) {
      var prevUpdateMeta = await Store.Recs.LatestFile(c.ChannelId);
      var prevUpdate = prevUpdateMeta?.Ts.ParseFileSafeTimestamp();
      var vidsDesc = vids.OrderByDescending(v => v.UploadDate).ToList();

      var toUpdate = prevUpdate == null
        ? vidsDesc //  all videos if this is the first time for this channel
        :
        // new vids since the last rec update, or the vid was created in the last RefreshRecsWithin (e.g. 30d)
        vidsDesc.Where(v => v.UploadDate > prevUpdate || v.UploadDate.UtcDateTime.IsYoungerThan(RCfg.RefreshRecsWithin))
          .Take(RCfg.RefreshRecsMax)
          .ToList();
      var deficit = RCfg.RefreshRecsMin - toUpdate.Count;
      if (deficit > 0)
        toUpdate.AddRange(vidsDesc.Where(v => toUpdate.All(u => u.Id != v.Id))
          .Take(deficit)); // if we don't have new videos, refresh the min amount by adding videos 
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