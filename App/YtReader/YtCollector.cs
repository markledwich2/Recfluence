using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Humanizer.Localisation;
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
  public enum CollectorMode {
    All,
    Channels,
    // go back and populate recommendations for videos that are missing any recorded recs
    AllWithMissingRecs
  }

  public static class RefreshHelper {
    public static bool IsOlderThan(this DateTime updated, TimeSpan age, DateTime? now = null) => (now ?? DateTime.UtcNow) - updated > age;
    public static bool IsYoungerThan(this DateTime updated, TimeSpan age, DateTime? now = null) => !updated.IsOlderThan(age, now);
  }

  public class YtCollector {
    readonly YtClient                    Api;
    readonly AppCfg                      Cfg;
    readonly SnowflakeConnectionProvider Sf;
    readonly IPipeCtx                    PipeCtx;
    readonly YtScraper                   Scraper;
    readonly YtStore                     Store;

    public YtCollector(YtStore store, AppCfg cfg, SnowflakeConnectionProvider sf, IPipeCtx pipeCtx, ILogger log) {
      Store = store;
      Cfg = cfg;
      Sf = sf;
      PipeCtx = pipeCtx;
      Scraper = new YtScraper(cfg.Scraper);
      Api = new YtClient(cfg.YTApiKeys, log);
    }

    YtReaderCfg RCfg => Cfg.YtReader;

    public async Task Collect(ILogger log, CollectorMode mode = CollectorMode.All, bool forceUpdate = false) {
      var channels = await UpdateAllChannels(log);
      var (result, dur) = await channels
        .Randomize() // randomize to even the load. Without this the large channels at the beginning make the first batch go way slower
        .Process(PipeCtx,
          b => ProcessChannels(b, mode, forceUpdate, Inject<ILogger>()))
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

      var channels = await seeds.Where(c => Cfg.LimitedToSeedChannels.IsEmpty() || Cfg.LimitedToSeedChannels.Contains(c.Id))
        .BlockFunc(UpdateChannel, Cfg.DefaultParallel,
          progressUpdate: p => log.Debug("Reading channels {ChannelCount}/{ChannelTotal}", p.CompletedTotal, seeds.Count)).WithDuration();

      if (channels.Result.Any())
        await store.Append(channels.Result, log);

      log.Information("Updated stats for {Channels} channels in {Duration}", channels.Result.Count, channels.Duration);

      return channels.Result;

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
          UserChannels = channel.UserChannels
        };
        return channelStored;
      }
    }

    [Pipe]
    public async Task<ProcessChannelResults> ProcessChannels(IReadOnlyCollection<ChannelStored2> channels,
      CollectorMode mode, bool forceUpdate, ILogger log = null) {
      log ??= Logger.None;
      var workSw = Stopwatch.StartNew();
      var conn = new AsyncLazy<LoggedConnection>(() => Sf.OpenConnection(log));
      var channelResults = await channels
        .Where(c => c.Status == ChannelStatus.Alive)
        .Select((c, i) => (c, i)).BlockFunc(async item => {
          var (c, i) = item;
          var sw = Stopwatch.StartNew();
          log = log
            .ForContext("ChannelId", c.ChannelId)
            .ForContext("Channel", c.ChannelTitle);
          try {
            await UpdateAllInChannel(c, conn, mode, forceUpdate, log);
            log.Information("{Channel} - Completed videos/recs/captions in {Duration}. Progress: channel {Count}/{BatchTotal}",
              c.ChannelTitle, sw.Elapsed.HumanizeShort(2, TimeUnit.Millisecond), i + 1, channels.Count);
            return (c, Success: true);
          }
          catch (Exception ex) {
            log.Error(ex, "Error updating channel {Channel}: {Error}", c.ChannelTitle, ex.Message);
            return (c, Success: false);
          }
        }, Cfg.ParallelChannels);

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

    async Task UpdateAllInChannel(ChannelStored2 c, AsyncLazy<LoggedConnection> conn, CollectorMode mode, bool forceUpdate, ILogger log) {
      if (c.StatusMessage.HasValue()) {
        log.Information("{Channel} - Not updating videos/recs/captions because it has a status msg: {StatusMessage} ",
          c.ChannelTitle, c.StatusMessage);
        return;
      }
      log.Information("{Channel} - Starting channel update of videos/recs/captions", c.ChannelTitle);

      // fix updated if missing. Remove once all records have been updated

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
      if (vids != null || mode == CollectorMode.AllWithMissingRecs)
        await SaveRecsAndExtra(c, vids, conn, mode, log);
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
        Thumbnails = v.Thumbnails,
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
    async Task SaveRecsAndExtra(ChannelStored2 c, IReadOnlyCollection<VideoItem> vids, AsyncLazy<LoggedConnection> conn, CollectorMode updateType,
      ILogger log) {
      var toUpdate = updateType switch {
        CollectorMode.AllWithMissingRecs => await VideosWithNoRecs(c, await conn.GetOrCreate(), log),
        _ => await VideoToUpdateRecs(c, vids)
      };

      var recs = await toUpdate.BlockFunc(
        async v => (fromId: v.Id, fromTitle: v.Title, recs: await Scraper.GetRecsAndExtra(v.Id, log)),
        Cfg.DefaultParallel);

      // read failed recs from the API (either because of an error, or because the video is 18+ restricted)
      var restricted = recs.Where(v => v.recs.extra.Error == YtScraper.RestrictedVideoError).ToList();

      if (restricted.Any()) {
        var apiRecs = await restricted.BlockFunc(async f => {
          ICollection<RecommendedVideoListItem> related = new List<RecommendedVideoListItem>();
          try {
            related = await Api.GetRelatedVideos(f.fromId);
          }
          catch (Exception ex) {
            log.Warning(ex, "Unable to get related videos for {VideoId}: {Error}", f.fromId, ex.Message);
          }
          return (f.fromId, f.fromTitle, recs: (related.NotNull().Select(r => new Rec {
            Source = RecSource.Api,
            ToChannelTitle = r.ChannelTitle,
            ToChannelId = r.ChannelId,
            ToVideoId = r.VideoId,
            ToVideoTitle = r.VideoTitle,
            Rank = r.Rank
          }).ToReadOnly(), (VideoExtraStored2) null));
        });

        recs = recs.Concat(apiRecs).ToList();

        log.Information("{Channel} - {Videos} videos recommendations fell back to using the API: {VideoList}",
          c.ChannelTitle, restricted.Count, apiRecs.Select(r => r.fromId));
      }

      var updated = DateTime.UtcNow;
      var recsStored = recs
        .SelectMany(v => v.recs.recs.Select((r, i) => new RecStored2 {
          FromChannelId = c.ChannelId,
          FromVideoId = v.fromId,
          FromVideoTitle = v.fromTitle,
          ToChannelTitle = r.ToChannelTitle,
          ToChannelId = r.ToChannelId,
          ToVideoId = r.ToVideoId,
          ToVideoTitle = r.ToVideoTitle,
          Rank = i + 1,
          Source = r.Source,
          Updated = updated
        })).ToList();

      if (recsStored.Any())
        await Store.Recs.Append(recsStored, log);

      var extraStored = recs.Select(r => r.recs.extra).NotNull().ToArray();
      if (extraStored.Any())
        await Store.VideoExtra.Append(extraStored, log);

      log.Information("{Channel} - Recorded {RecCount} recs: {Recs}",
        c.ChannelTitle, recsStored.Count, recs.Select(v => new {Id = v.fromId, v.recs.recs.Count}).ToList());

      log.Information("{Channel} - Recorded {VideoExtra} extra info on video's",
        c.ChannelTitle, extraStored.Length);
    }

    async Task<IReadOnlyCollection<(string Id, string Title)>> VideosWithNoRecs(ChannelStored2 c, LoggedConnection connection, ILogger log) {
      var ids = await connection.Query<(string Id, string Title)>("videos sans-recs", $@"select v.video_id, v.video_title
        from video_latest v where v.channel_id = '{c.ChannelId}'
        and not exists(select * from rec r where r.from_video_id = v.video_id)
                       group by v.video_id, v.video_title");
      log.Information("{Channel} - found {Recommendations} video's missing recommendations", c.ChannelTitle, ids.Count);
      return ids;
    }

    async Task<IReadOnlyCollection<(string Id, string Title)>> VideoToUpdateRecs(ChannelStored2 c, IEnumerable<VideoItem> vids) {
      var prevUpdateMeta = await Store.Recs.LatestFile(c.ChannelId);
      var prevUpdate = prevUpdateMeta?.Ts.ParseFileSafeTimestamp();
      var vidsDesc = vids.OrderByDescending(v => v.UploadDate).ToList();

      var toUpdate = prevUpdate == null
        ? vidsDesc
        : //  all videos if this is the first time for this channel
        // new vids since the last rec update, or the vid was created in the last RefreshRecsWithin (e.g. 30d)
        vidsDesc.Where(v => v.UploadDate > prevUpdate || v.UploadDate.UtcDateTime.IsYoungerThan(RCfg.RefreshRecsWithin)).ToList();
      var deficit = RCfg.RefreshRecsMin - toUpdate.Count;
      if (deficit > 0)
        toUpdate.AddRange(vidsDesc.Where(v => toUpdate.All(u => u.Id != v.Id))
          .Take(deficit)); // if we don't have new videos, refresh the min amount by adding videos 
      return toUpdate.Select(v => (v.Id, v.Title)).ToList();
    }

    /// <summary>A once off command to populate existing uploaded videos evenly with checks for ads.</summary>
    [Pipe]
    public async Task BackfillVideoExtra(ILogger log, string videoIds = null, int? limit = null) {
      if (videoIds == null) {
        using var conn = await Sf.OpenConnection(log);

        var limitString = limit == null ? "" : $"limit {limit}";
        var toUpdate = (await conn.Query<ChannelVideoItem>("videos sans-extra", @$"
select video_id as VideoId, channel_id as ChannelId, channel_title as ChannelTitle
from (select video_id, channel_id, channel_title
           , upload_date
           , row_number() over (partition by channel_id order by upload_date desc) as num
           , ad_checks
      from video_latest l
    --where ad_checks > 0 and no_ads = ad_checks -- TODO: temporarily look at no_ads in-case they were actually errors
     )
where num <= 50 -- most recent for each channel 
{limitString}")).ToArray();

        var res = await toUpdate
          .Process(PipeCtx, b => ProcessVideoExtra(b, Inject<ILogger>()), new PipeRunCfg {MinWorkItems = 1000, MaxParallel = 8}, log)
          .WithDuration();

        var videos = res.Result.Sum(o => o.OutState.Sum(v => v.Updated));
        log.Information("Finished {Pipe} of {Channels} channels, {Videos} videos in {Duration}",
          nameof(BackfillVideoExtra), res.Result.Count, videos, res.Duration);
      }
      else {
        var chId = "123TestChannel";
        var toUpdate = videoIds.Split("|")
          .Select(v => new ChannelVideoItem {ChannelId = chId, VideoId = v});


        var res = await toUpdate.Process(PipeCtx, b => ProcessVideoExtra(b, Inject<ILogger>()), new PipeRunCfg {MinWorkItems = 200, MaxParallel = 2}, log);

        /*var recsAndExtra = await videoIds.Split("|")
          .BlockTransform(async v => await Scraper.GetRecsAndExtra(v, log), Cfg.DefaultParallel);
        await recsAndExtra.GroupBy(v => v.extra.ChannelId).BlockAction(async g => {
          var store = Store.VideoExtraStore(g.Key);
          await store.Append(g.Select(c => c.extra).ToArray());
      });*/
      }
    }

    [Pipe]
    public async Task<IReadOnlyCollection<ProcessVideoExtraBatch>> ProcessVideoExtra(IEnumerable<ChannelVideoItem> videos, ILogger log) {
      var batch = await videos.BatchGreedy(2000).BlockFunc(async b => {
        var recsAndExtra = await b.NotNull().BlockFunc(async v => await Scraper.GetRecsAndExtra(v.VideoId, log), Cfg.DefaultParallel);
        var extra = recsAndExtra.Select(r => r.extra).ToArray();
        await Store.VideoExtra.Append(extra, log);
        log.Information("Recorded {VideoExtra} video_extra records", extra.Length);
        return new ProcessVideoExtraBatch {
          Updated = extra.Length
        };
      });
      return batch;
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
    //bool Expired(DateTime updated, TimeSpan refreshAge) => (RCfg.To ?? DateTime.UtcNow) - updated > refreshAge;

    public class UpdateChannelWork {
      public ChannelStored2 Channel { get; set; }
    }

    public class ChannelVideoItem {
      public string ChannelId    { get; set; }
      public string ChannelTitle { get; set; }
      public string VideoId      { get; set; }
    }

    public class ProcessVideoExtraBatch {
      public int Updated { get; set; }
    }
  }
}