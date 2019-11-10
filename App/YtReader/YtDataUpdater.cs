using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Mutuo.Etl;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Yt;
using YtReader.YtWebsite;
using VideoItem = YtReader.YtWebsite.VideoItem;

namespace YtReader {
  public class YtDataUpdater {
    YtStore            Store { get; }
    AppCfg             Cfg   { get; }
    ILogger            Log   { get; }
    readonly YtScraper Scraper;
    readonly YtClient  Api;

    public YtDataUpdater(YtStore store, AppCfg cfg, ILogger log) {
      Store = store;
      Cfg = cfg;
      Log = log;
      Scraper = new YtScraper(cfg.Scraper);
      Api = new YtClient(cfg, log);
    }

    YtReaderCfg RCfg => Cfg.YtReader;
    bool Expired(DateTime updated, TimeSpan refreshAge) => (RCfg.To ?? DateTime.UtcNow) - updated > refreshAge;

    public async Task UpdateData() {
      Log.Information("Starting data update");
      var sw = Stopwatch.StartNew();

      var channels = await UpdateChannels().WithDuration();
      Log.Information("Updated stats for {Channels} channels in {Duration}", channels.Result.Count, channels.Duration);
      
      var channelResults = await channels.Result
        .BlockTransform(async c => {
          try {
            await UpdateAllInChannel(c);
            return (c, Success: true);
          }
          catch (Exception ex) {
            Log.Error(ex, "Error updating channel {Channel}: {Error}", c.ChannelTitle, ex.Message);
            return (c, Success: false);
          }
        }, Cfg.ParallelChannels);

      Log.Information("Updated {Completed} channel videos/captions/recs, {Errors} failed in {Duration}",
        channelResults.Count(c => c.Success), channelResults.Count(c => !c.Success), sw.Elapsed);
    }

    async Task<IReadOnlyCollection<ChannelStored2>> UpdateChannels() {
      var store = Store.ChannelStore;

      Log.Information("Starting channels update. Limited to ({Included})",
        Cfg.LimitedToSeedChannels?.HasItems() == true ? Cfg.LimitedToSeedChannels.Join("|") : "All");

      async Task<ChannelStored2> UpdateChannel(ChannelWithUserData channel) {
        var log = Log.ForContext("Channel", channel.Title).ForContext("ChannelId", channel.Id);

        ChannelData channelData = null;
        string statusMessage = null;
        try {
          channelData = await Api.ChannelData(channel.Id); // Use API to get channel instead of scraper. We get better info faster
          log.Information("{Channel} - read channel details", channelData.Title);
        }
        catch (Exception ex) {
          statusMessage = ex.Message;
          log.Error(ex, "{Channel} - Error when updating details for channel : {Error}", channel.Title, ex.Message);
        }
        var channelStored = new ChannelStored2 {
          ChannelId = channel.Id,
          ChannelTitle = channelData?.Title ?? channel.Title,
          MainChannelId = channel.MainChannelId,
          Description = channelData?.Description,
          LogoUrl = channelData?.Thumbnails?.Standard?.Url,
          Subs = channelData?.Stats?.SubCount,
          ChannelViews = channelData?.Stats?.ViewCount,
          Country = channelData?.Country,
          Updated = DateTime.UtcNow,
          Relevance = channel.Relevance,
          LR = channel.LR,
          HardTags = channel.HardTags,
          SoftTags = channel.SoftTags,
          SheetIds = channel.SheetIds,
          StatusMessage = statusMessage
        };
        return channelStored;
      }

      var seeds = await ChannelSheets.Channels(Cfg.Sheets, Log);

      var channels = await seeds.Where(c => Cfg.LimitedToSeedChannels.IsEmpty() || Cfg.LimitedToSeedChannels.Contains(c.Id))
        .BlockTransform(UpdateChannel, Cfg.ParallelGets,
          progressUpdate: p => Log.Debug("Reading channels {ChannelCount}/{ChannelTotal}", p.CompletedTotal, seeds.Count));

      if (channels.Any())
        await store.Append(channels);

      return channels;
    }

    async Task UpdateAllInChannel(ChannelStored2 c) {
      var log = Log
        .ForContext("ChannelId", c.ChannelId)
        .ForContext("Channel", c.ChannelTitle);

      if (c.StatusMessage.HasValue()) {
        log.Information("{Channel} - Not updating videos/recs/captions because it has a status msg: {StatusMessage} ",
          c.ChannelTitle, c.StatusMessage);
        return;
      }
      log.Information("{Channel} - Starting channel update of videos/recs/captions", c.ChannelTitle);
      var sw = Stopwatch.StartNew();

      // fix updated if missing. Remove once all records have been updated
      var vidStore = Store.VideoStore(c.ChannelId);

      var md = await vidStore.LatestFileMetadata();
      var lastUpload = md?.Ts?.ParseFileSafeTimestamp();
      var lastModified = md?.Modified;
      if (lastModified != null && !Expired(lastModified.Value, RCfg.RefreshAllAfter)) {
        log.Information("{Channel} - skipping update, video stats have been updated recently {LastModified}", c.ChannelTitle, lastModified);
        return;
      }

      // get the oldest date for videos to store updated statistics for. This overlaps so that we have a history of video stats.
      var uploadedFrom = md == null ? RCfg.From : DateTime.UtcNow - RCfg.RefreshVideosWithin;

      var vids = await ChannelVidItems(c, uploadedFrom, log).ToListAsync();
      await SaveVids(c, vids, vidStore, lastUpload, log);
      await SaveRecs(c, vids, log);
      await SaveNewCaptions(c, vids, log);

      log.Information("{Channel} - Completed  update of videos/recs/captions in {Duration}", c.ChannelTitle, sw.Elapsed);
    }

    static async Task SaveVids(ChannelStored2 c, IReadOnlyCollection<VideoItem> vids, AppendCollectionStore<VideoStored2> vidStore, DateTime? uploadedFrom,
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
        await vidStore.Append(vidsStored);

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

    /// <summary>
    ///   Saves captions for all new videos from the vids list
    /// </summary>
    async Task SaveNewCaptions(ChannelStored2 channel, IEnumerable<VideoItem> vids, ILogger log) {
      var store = Store.CaptionStore(channel.ChannelId);
      var lastUpload = (await store.LatestFileMetadata())?.Ts.ParseFileSafeTimestamp(); // last video upload we have captions for

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
          VideoId = v.Id,
          UploadDate = v.UploadDate.UtcDateTime,
          Updated = DateTime.Now,
          Info = track.Info,
          Captions = track.Captions
        };
      }

      var captionsToStore =
        (await vids.Where(v => lastUpload == null || v.UploadDate.UtcDateTime > lastUpload)
          .BlockTransform(GetCaption, Cfg.ParallelGets)).NotNull().ToList();

      if (captionsToStore.Any())
        await store.Append(captionsToStore);

      log.Information("{Channel} - Saved {Captions} captions", channel.ChannelTitle, captionsToStore.Count);
    }

    /// <summary>
    ///   Saves recs for all of the given vids
    /// </summary>
    async Task SaveRecs(ChannelStored2 c, IReadOnlyCollection<VideoItem> vids, ILogger log) {
      var recStore = Store.RecStore(c.ChannelId);
      var prevUpdate = await recStore.LatestFileMetadata();

      var vidsDesc = vids.OrderByDescending(v => v.UploadDate).ToList();
      // refresh all videos if this is the first time for this channel, otherwise take up to max
      var toUpdate = prevUpdate == null ? vidsDesc : vidsDesc.Take(RCfg.RefreshRecsMax).ToList();
      var deficit = RCfg.RefreshRecsMin - toUpdate.Count;
      if (deficit > 0)
        toUpdate.AddRange(vidsDesc.Take(deficit));

      var recs = await toUpdate.BlockTransform(
        async v => (v, recs: await Scraper.GetRecs(v.Id, log)),
        Cfg.ParallelGets);

      var updated = DateTime.UtcNow;
      var recsStored = recs
        .SelectMany(v => v.recs.Select((r, i) => new RecStored2 {
          FromChannelId = c.ChannelId,
          FromVideoId = v.v.Id,
          FromVideoTitle = v.v.Title,
          ToChannelTitle = r.ToChannelTitle,
          ToVideoId = r.ToVideoId,
          ToVideoTitle = r.ToVideoTitle,
          Rank = i + 1,
          Updated = updated
        })).ToList();

      if (recsStored.Any())
        await recStore.Append(recsStored);

      Log.Information("{Channel} - Recorded {RecCount} recs for {VideoCount} videos", c.ChannelTitle, recsStored.Count, toUpdate);
    }
  }
}