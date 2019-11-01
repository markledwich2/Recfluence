using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Mutuo.Etl;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.YtWebsite;

namespace YtReader {
  public class YtDataUpdater {
    YtStore Store { get; }
    AppCfg Cfg { get; }
    ILogger Log { get; }
    readonly YtScraper Scraper;

    public YtDataUpdater(YtStore store, AppCfg cfg, ILogger log) {
      Store = store;
      Cfg = cfg;
      Log = log;
      Scraper = new YtScraper(cfg.Proxy);
    }

    YtReaderCfg RCfg => Cfg.YtReader;
    bool Expired(DateTime updated, TimeSpan refreshAge) => (RCfg.To ?? DateTime.UtcNow) - updated > refreshAge;

    public async Task UpdateData() {
      Log.Information("Starting data update");
      var sw = Stopwatch.StartNew();

      var channels = await UpdateChannels().WithDuration();
      Log.Information("Completed channel info list. {RefreshedChannels}/{Channels} channels in {Duration}",
        channels.Result.Count(c => c.Refresh), channels.Result.Count, channels.Duration);

      var includedChannels = channels.Result.Where(c => c.Include).ToList();
      await includedChannels
        .BlockAction(c => {
          try {
            return UpdateAllInChannel(c.Channel);
          }
          catch (Exception ex) {
            Log.Error(ex, "Error updating channel {Channel}: {Error}", c.Channel.ChannelTitle, ex.Message);
            return null;
          }
        }, Cfg.ParallelChannels);
      Log.Information("Updated {Channels} channel's videos/captions/recs in {Duration}",
        includedChannels.Count, sw.Elapsed);
    }

    async Task<IReadOnlyCollection<(ChannelStored2 Channel, bool Refresh, bool IsNew, bool Include)>> UpdateChannels() {
      var seeds = await ChannelSheets.Channels(Cfg.Sheets, Log);
      var store = Store.ChannelStore;
      var latestItems = await store.LatestItems();
      var latestStored = latestItems.Items
        .Where(c => c.ChannelId.HasValue())
        .ToKeyedCollection(c => c.ChannelId, StringComparer.Ordinal);

      async Task<(ChannelStored2 Channel, bool Refresh, bool IsNew, bool Include)> UpdateChannel(ChannelWithUserData channel) {
        var log = Log.ForContext("Channel", channel.Title).ForContext("ChannelId", channel.Id);
        var channelStored = latestStored[channel.Id];
        var isNew = channelStored == null;
        var includeChannel = Cfg.LimitedToSeedChannels.IsEmpty() || Cfg.LimitedToSeedChannels.Contains(channel.Id);
        var refreshChannel = includeChannel && (channelStored == null || Expired(channelStored.Updated, RCfg.RefreshChannel));
        if (isNew || includeChannel && refreshChannel)
          try {
            var channelData = await Scraper.GetChannelAsync(channel.Id, log);

            channelStored = new ChannelStored2 {
              ChannelId = channelData.Id,
              ChannelTitle = channelData.Title ?? channel.Title,
              LogoUrl = channelData.LogoUrl,
              Subs = channelData.Subs,
              Updated = DateTime.UtcNow,
              Relevance = channel.Relevance,
              LR = channel.LR,
              HardTags = channel.HardTags,
              SoftTags = channel.SoftTags,
              SheetIds = channel.SheetIds,
              StatusMessage = channelData.StatusMessage
            };
            Log.Information("{Channel} - read channel details", channelStored.ChannelTitle);
          }
          catch (Exception ex) {
            Log.Error(ex, "{Channel} - Error when updating details for channel : {Error}", channel.Title, ex.Message);
          }
        return (Channel: channelStored, Refresh: refreshChannel, IsNew: isNew, Include: includeChannel);
      }

      var channels = await seeds.BlockTransform(UpdateChannel, Cfg.ParallelGets,
        progressUpdate: p => Log.Debug("Reading channels {ChannelCount}/{ChannelTotal}", p.CompletedTotal, seeds.Count));

      if (channels.Any(c => c.IsNew || c.Refresh))
        await store.Append(channels.Select(c => c.Channel).NotNull().ToList());

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
      if (lastModified != null && Expired(lastModified.Value, RCfg.RefreshChannel)) {
        log.Information("{Channel} - skipping update, video stats have been updated recently {LastModified}", c.ChannelTitle, lastModified);
        return;
      }

      // get the oldest date for videos to store updated statistics for. This overlaps so that we have a history of video stats.
      var updateFrom = md == null ? RCfg.From : DateTime.UtcNow - RCfg.VideoDead;

      var vids = await ChannelVidItems(c, updateFrom, log).ToListAsync();
      await UpdateVids(c, vids, vidStore, lastUpload, log);
      await UpdateRecs(c, vids, log);
      await UpdateCaptions(c, vids, log);

      log.Information("{Channel} - Completed  update of videos/recs/captions in {Duration}", c.ChannelTitle, sw.Elapsed);
    }

    async Task UpdateVids(ChannelStored2 c, IReadOnlyCollection<VideoItem> vids, AppendCollectionStore<VideoStored2> vidStore, DateTime? lastUpload,
      ILogger log) {
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
        UploadDate = v.UploadDate.UtcDateTime
      }).ToList();

      if (vidsStored.Count > 0)
        await vidStore.Append(vidsStored);

      var newVideos = vids.Count(v => v.UploadDate > lastUpload);

      log.Information("{Channel} - Recorded {VideoCount} videos. {NewCount} new, {UpdatedCount} updated",
        c.ChannelTitle, vids.Count, newVideos, vids.Count - newVideos);
    }

    async IAsyncEnumerable<VideoItem> ChannelVidItems(ChannelStored2 c, DateTime updateFrom, ILogger log) {
      await foreach (var vids in Scraper.GetChannelUploadsAsync(c.ChannelId, log)) {
        if (!vids.Any(v => v.UploadDate > updateFrom))
          yield break;
        foreach (var v in vids)
          if (v.UploadDate > updateFrom)
            yield return v;
      }
    }

    /// <summary>
    ///   Saves captions for all new videos from the vids list
    /// </summary>
    async Task UpdateCaptions(ChannelStored2 channel, IEnumerable<VideoItem> vids, ILogger log) {
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
        return new VideoCaptionStored2 {VideoId = v.Id, UploadDate = v.UploadDate.UtcDateTime, Info = track.Info, Captions = track.Captions};
      }

      var captionsToStore =
        (await vids.Where(v => lastUpload == null || v.UploadDate > lastUpload)
          .BlockTransform(GetCaption, Cfg.ParallelGets)).NotNull().ToList();

      if (captionsToStore.Any())
        await store.Append(captionsToStore);

      log.Information("{Channel} - Saved {Captions} captions", channel.ChannelTitle, captionsToStore.Count);
    }

    /// <summary>
    ///   Saves recs for all of the given vids
    /// </summary>
    async Task UpdateRecs(ChannelStored2 c, IReadOnlyCollection<VideoItem> vids, ILogger log) {
      RecStored2 Rec(VideoItem v, Rec r) =>
        new RecStored2 {
          FromChannelId = c.ChannelId,
          FromVideoId = v.Id,
          FromVideoTitle = v.Title,
          ToChannelTitle = r.ToChannelTitle,
          ToVideoId = r.ToVideoId,
          ToVideoTitle = r.ToVideoTitle
        };

      var recsStored = (await vids.BlockTransform(
          async v => (v, recs: await Scraper.GetRecs(v.Id, log)),
          Cfg.ParallelGets)).ToList()
        .SelectMany(v => v.recs.Select(r => Rec(v.v, r))).ToList();

      if (recsStored.Any())
        await Store.RecStore(c.ChannelId).Append(recsStored);

      Log.Information("{Channel} - Recorded {RecCount} recs for {VideoCount} videos", c.ChannelTitle, recsStored.Count, vids.Count);
    }
  }
}