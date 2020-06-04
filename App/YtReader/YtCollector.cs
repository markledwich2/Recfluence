﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using AngleSharp.Common;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Mutuo.Etl.Pipe;
using Serilog;
using Serilog.Core;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;
using YtReader.Yt;
using YtReader.YtWebsite;
using static Mutuo.Etl.Pipe.PipeArg;
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
    readonly WebScraper                   Scraper;
    readonly ChromeScraper               ChromeScraper;
    readonly YtStore                     Store;

    public YtCollector(YtStore store, AppCfg cfg, SnowflakeConnectionProvider sf, IPipeCtx pipeCtx, ILogger log) {
      Store = store;
      Cfg = cfg;
      Sf = sf;
      PipeCtx = pipeCtx;
      Scraper = new WebScraper(cfg.Scraper);
      ChromeScraper = new ChromeScraper(cfg.Scraper);
      Api = new YtClient(cfg.YTApiKeys, log);
    }

    YtCollectCfg RCfg => Cfg.YtCollect;
    
    [Pipe]
    public async Task Collect(ILogger log, bool forceUpdate = false) {
      var channels = await UpdateAllChannels(log);
      var (result, dur) = await channels
        .Randomize() // randomize to even the load. Without this the large channels at the beginning make the first batch go way slower
        .Process(PipeCtx,
          b => ProcessChannels(b, forceUpdate, Inject<ILogger>()))
        .WithDuration();

      var allChannelResults = result.SelectMany(r => r.OutState.Channels).ToArray();
      log.Information("{Pipe} Complete - {Success}/{Total} channels updated in {Duration}",
        nameof(Collect), allChannelResults.Count(c => c.Success), allChannelResults.Length, dur.HumanizeShort());
    }

    async Task<IEnumerable<ChannelStored2>> UpdateAllChannels(ILogger log) {
      var store = Store.Channels;
      log.Information("Starting channels update. Limited to ({Included})",
        Cfg.LimitedToSeedChannels?.HasItems() == true ? Cfg.LimitedToSeedChannels.Join("|") : "All");

      var seeds = await ChannelSheets.Channels(Cfg.Sheets, log);

      var (channels, dur) = await seeds.Where(c => Cfg.LimitedToSeedChannels.IsEmpty() || Cfg.LimitedToSeedChannels.Contains(c.Id))
        .BlockFunc(UpdateChannel, Cfg.DefaultParallel,
          progressUpdate: p => log.Debug("Reading channels {ChannelCount}/{ChannelTotal}", p.CompletedTotal, seeds.Count)).WithDuration();

      if (channels.Any())
        await store.Append(channels, log);

      log.Information("Updated stats for {Channels} channels in {Duration}", channels.Count, dur);

      return channels;

      async Task<ChannelStored2> UpdateChannel(ChannelSheet channel) {
        var channelLog = log.ForContext("Channel", channel.Title).ForContext("ChannelId", channel.Id);

        var channelData = new ChannelData {Id = channel.Id, Title = channel.Title};
        try {
          channelData = await Api.ChannelData(channel.Id) ?? // Use API to get channel instead of scraper. We get better info faster
                        new ChannelData {Id = channel.Id, Title = channel.Title, Status = ChannelStatus.Dead};
          channelLog.Information("{Channel} - read channel details", channelData.Title);
        }
        catch (Exception ex) {
          channelData.Status = ChannelStatus.Dead;
          channelLog.Error(ex, "{Channel} - Error when updating details for channel : {Error}", channel.Title, ex.Message);
        }

        var channelStored = new ChannelStored2 {
          ChannelId = channel.Id,
          ChannelTitle = channelData.Title ?? channel.Title,
          Status = channelData.Status,
          MainChannelId = channel.MainChannelId,
          Description = channelData.Description,
          LogoUrl = channelData.Thumbnails?.Default__?.Url,
          Subs = channelData.Stats?.SubCount,
          ChannelViews = channelData.Stats?.ViewCount,
          Country = channelData.Country,
          Updated = DateTime.UtcNow,
          Relevance = channel.Relevance,
          LR = channel.LR,
          HardTags = channel.HardTags,
          SoftTags = channel.SoftTags,
          UserChannels = channel.UserChannels,
          ReviewStatus = ChannelReviewStatus.ManualAccepted
        };
        return channelStored;
      }
    }

    [Pipe]
    public async Task<ProcessChannelResults> ProcessChannels(IReadOnlyCollection<ChannelStored2> channels,
      bool forceUpdate, ILogger log = null) {
      log ??= Logger.None;
      var workSw = Stopwatch.StartNew();
      
      var channelResults = await channels
        .Where(c => c.Status == ChannelStatus.Alive &&
                    c.ReviewStatus.In(ChannelReviewStatus.Pending, ChannelReviewStatus.AlgoAccepted, ChannelReviewStatus.ManualAccepted))
        .Select((c, i) => (c, i))
        .BlockFunc(async item => {
          var (c, i) = item;
          var sw = Stopwatch.StartNew();
          log = log
            .ForContext("ChannelId", c.ChannelId)
            .ForContext("Channel", c.ChannelTitle);
          try {
            var conn = new AsyncLazy<LoggedConnection>(() => Sf.OpenConnection(log));
            await UpdateAllInChannel(c, conn, forceUpdate, log);
            log.Information("{Channel} - Completed videos/recs/captions in {Duration}. Progress: channel {Count}/{BatchTotal}",
              c.ChannelTitle, sw.Elapsed.HumanizeShort(), i + 1, channels.Count);
            return (c, Success: true);
          }
          catch (Exception ex) {
            log.Error(ex, "Error updating channel {Channel}: {Error}", c.ChannelTitle, ex.Message);
            return (c, Success: false);
          }
        }, RCfg.ParallelChannels);

      var res = new ProcessChannelResults {
        Channels = channelResults.Select(r => new ProcessChannelResult {ChannelId = r.c.ChannelId, Success = r.Success}).ToArray(),
        Duration = workSw.Elapsed,
        RequestStats = Scraper.RequestStats
      };

      log.Information(
        "{Pipe} complete - {ChannelsComplete} channel videos/captions/recs, {ChannelsFailed} failed in {Duration}, {DirectRequests} direct requests, {ProxyRequests} proxy requests",
        nameof(ProcessChannels), channelResults.Count(c => c.Success), channelResults.Count(c => !c.Success), res.Duration, res.RequestStats.direct,
        res.RequestStats.proxy);

      return res;
    }

    async Task UpdateAllInChannel(ChannelStored2 c, AsyncLazy<LoggedConnection> conn, bool forceUpdate, ILogger log) {
      if (c.StatusMessage.HasValue()) {
        log.Information("{Channel} - Not updating videos/recs/captions because it has a status msg: {StatusMessage} ",
          c.ChannelTitle, c.StatusMessage);
        return;
      }

      log.Information("{Channel} - Starting channel update of videos/recs/captions", c.ChannelTitle);

      var md = await Store.Videos.LatestFile(c.ChannelId);
      var lastUpload = md?.Ts?.ParseFileSafeTimestamp();
      var lastModified = md?.Modified;
      var recentlyUpdated = !forceUpdate && lastModified != null && lastModified.Value.IsYoungerThan(RCfg.RefreshAllAfter);

      // get the oldest date for videos to store updated statistics for. This overlaps so that we have a history of video stats.
      var uploadedFrom = md == null ? RCfg.From : DateTime.UtcNow - RCfg.RefreshVideosWithin;

      if (recentlyUpdated)
        log.Information("{Channel} - skipping update, video stats have been updated recently {LastModified}", c.ChannelTitle, lastModified);

      var vids = recentlyUpdated ? null : await ChannelVidItems(c, uploadedFrom, log).ToListAsync();

      if (vids != null)
        await SaveVids(c, vids, Store.Videos, lastUpload, log);
      if (vids != null)
        await SaveRecsAndExtra(c, vids, conn, log);
      if (vids != null)
        await SaveNewCaptions(c, vids, log);
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

      log.Information("{Channel} - Recorded {VideoCount} videos. {NewCount} new, {UpdatedCount} updated",
        c.ChannelTitle, vids.Count, newVideos, vids.Count - newVideos);
    }

    async IAsyncEnumerable<VideoItem> ChannelVidItems(ChannelStored2 c, DateTime uploadFrom, ILogger log) {
      await foreach (var vids in Scraper.GetChannelUploadsAsync(c.ChannelId, log))
      foreach (var v in vids)
        if (v.UploadDate > uploadFrom) yield return v;
        else yield break; // break on the first video older than updateFrom.
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

      log.Information("{Channel} - Saved {Captions} captions", channel.ChannelTitle, captionsToStore.Count);
    }

    /// <summary>Saves recs for all of the given vids</summary>
    async Task SaveRecsAndExtra(ChannelStored2 c, IReadOnlyCollection<VideoItem> vids, AsyncLazy<LoggedConnection> getConn,
      ILogger log) {
      var conn = await getConn.GetOrCreate();
      // which method do we use for each video??
      // use chrome when its +1 week old and +1 month old. We should get most of the comments that way (max 50 vids)
      var forChromeUpdate = (await VideosForChromeUpdate(c, conn, log)).ToHashSet();
      var forUpdate = (await VideoToUpdateRecs(c, vids)).Where(v => !forChromeUpdate.Contains(v)).ToArray();
      
      var chromeExtra = await ChromeScraper.GetRecsAndExtra(forChromeUpdate, log);
      var webExtra = await Scraper.GetRecsAndExtra(forUpdate, log);

      var allExtra = (chromeExtra.Concat(webExtra)).ToArray();
      
      var extra = allExtra.Select(v => v.Extra).NotNull().ToArray();
      
      var updated = DateTime.UtcNow;
      var recs = allExtra
        .SelectMany(v => v.Recs.Select((r, i) => new RecStored2 {
          FromChannelId = c.ChannelId,
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
        })).ToArray();
      
      if (recs.Any())
        await Store.Recs.Append(recs, log);
      
      if (extra.Any()) 
        await Store.VideoExtra.Append(extra, log);
      
      log.Information("{Channel} - Recorded {WebExtras} web-extras, {ChromeExtras} chrome-extras, {Recs} recs, {Comments} comments",
        c.ChannelTitle, webExtra.Count, chromeExtra.Count, recs.Length, extra.Sum(e => e.Comments.Length));
    }

    async Task<IReadOnlyCollection<string>> VideosForChromeUpdate(ChannelStored2 c, LoggedConnection connection, ILogger log) {
      var ids = await connection.Query<string>("videos sans-comments",
        @"with chrome_extra_latest as (
  // to use table once we have these new fields loaded
  select v:Id as video_id
       , v:Updated::timestamp_ntz updated
       , v:ChannelId::string channel_id
       , v:Error::string as error
       , v:Source::string as source
       , row_number() over (partition by video_id order by updated desc) as age_no
       , count(1) over (partition by video_id) as extras
  from video_extra_stage v
  where channel_id = :channel_id
        and v:Source::string = 'Chrome'
    qualify age_no = 1
)
   , videos_to_update as (
  select v.video_id
       , v.video_title
       , v.upload_date
       , e.extras
       , e.source
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
order by upload_date desc
limit :limit",
        param: new {channel_id = c.ChannelId, limit = RCfg.PopulateMissingCommentsLimit});
      log.Information("{Channel} - found {Videos} in need of chrome update", c.ChannelTitle, ids.Count);
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
      public ProcessChannelResult[]    Channels     { get; set; }
      public TimeSpan                  Duration     { get; set; }
      public (long direct, long proxy) RequestStats { get; set; }
    }
  }
}