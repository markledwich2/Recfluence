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
        channels.Result.Count(c => c.Refresh), channels.Result.Count, channels.Duration.Humanize(2));

      var includedChannels = channels.Result.Where(c => c.Include).ToList();
      await includedChannels
        .BlockAction(c => UpdateAllInChannel(c.Channel), Cfg.ParallelChannels);
      Log.Information("Completed updated of {Channels} channel videos/captions/recs in {Duration}",
        includedChannels.Count, sw.Elapsed.Humanize(2));
    }

    async Task<IReadOnlyCollection<(ChannelStored2 Channel, bool Refresh, bool IsNew, bool Include)>> UpdateChannels() {
      var seeds = await ChannelSheets.Channels(Cfg.Sheets, Log);
      var store = Store.ChannelStore;
      var latestStored = (await store.LatestItems())
        .Where(c => c.ChannelId.HasValue())
        .ToKeyedCollection(c => c.ChannelId, StringComparer.Ordinal);

      var stored = await seeds.BlockTransform(async channel => {
          var log = Log.ForContext("Channel", channel.Title).ForContext("ChannelId", channel.Id);
          var channelStored = latestStored[channel.Id];
          var isNew = channelStored == null;
          var includeChannel = Cfg.LimitedToSeedChannels.IsEmpty() || Cfg.LimitedToSeedChannels.Contains(channel.Id);
          var refreshChannel = includeChannel && (isNew || Expired(channelStored.Updated, RCfg.RefreshChannel));
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
              Log.Error(ex, "{Channel} - Error when updating details for channel : {Error}",
                channel.Title, ex.Message);
            }
          return (Channel: channelStored, Refresh: refreshChannel, IsNew: isNew, Include: includeChannel);
        },
        Cfg.ParallelGets,
        progressUpdate: p => Log.Debug("Reading channels {ChannelCount}/{ChannelTotal}", p.CompletedTotal, seeds.Count));

      if (stored.Any(c => c.IsNew || c.Refresh))
        await store.Append(stored.Select(c => c.Channel).NotNull().ToList());

      return stored;
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

      // fix updated if missing. Remove once all records have been updated
      var vidStore = Store.VideoStore(c.ChannelId);
      var ts = (await vidStore.LatestTimestamp())?.ParseFileSafeTimestamp();
      // get the oldest date for videos to store updated statistics for. This overlaps so that we have a history of video stats.
      var updateFrom = ts == null ? RCfg.From : DateTime.UtcNow - RCfg.VideoDead;

      var vids = await ChannelVidsToRefresh(c, updateFrom, log);

      await UpdateVids(c, vids, vidStore, ts, log);
      await UpdateRecs(c, vids, log);
      await UpdateCaptions(c, vids, log);
    }

    async Task UpdateVids(ChannelStored2 c, IReadOnlyCollection<Video> vids, AppendCollectionStore<VideoStored2> vidStore, DateTime? ts, ILogger log) {
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

      var newVideos = vids.Count(v => v.UploadDate > ts);

      log.Information("{channel} - Recorded {VideoCount} videos. {NewCount} new, {UpdatedCount} updated",
        c.ChannelTitle, vids.Count, newVideos, vids.Count - newVideos);
    }

    async Task<IReadOnlyCollection<Video>> ChannelVidsToRefresh(ChannelStored2 c, DateTime from, ILogger log) {
      var vidItems = await ChannelVidItems(c, log, from).ToListAsync();
      var vids = await vidItems.BlockTransform(async v => await Scraper.GetVideoAsync(v.Id, log),
        Cfg.ParallelGets,
        progressUpdate: p => log.Debug("Reading {Channel} videos: {Video}{VideoTotal}", c.ChannelTitle, p.CompletedTotal, vidItems.Count));
      return vids;
    }

    async IAsyncEnumerable<VideoItem> ChannelVidItems(ChannelStored2 c, ILogger log, DateTime updateFrom) {
      await foreach (var vids in Scraper.GetChannelUploadsAsync(c.ChannelId, log)) {
        if (!vids.Any(v => v.UploadDate > updateFrom))
          yield break;
        foreach (var v in vids)
          if (v.UploadDate > updateFrom)
            yield return v;
      }
    }

    async Task UpdateCaptions(ChannelStored2 channel, IEnumerable<Video> vids, ILogger log) {
      var store = Store.CaptionStore(channel.ChannelId);
      var ts = (await store.LatestTimestamp())?.ParseFileSafeTimestamp();
      var captions = (await vids.Where(v => ts == null || v.UploadDate > ts)
        .BlockTransform(async v => {
          var videoLog = log.ForContext("VideoId", v.Id);

          var enInfo = v.Captions.FirstOrDefault(t => t.Language.Code == "en");
          if (enInfo == null) return null;

          ClosedCaptionTrack track;
          try {
            track = await Scraper.GetClosedCaptionTrackAsync(enInfo, videoLog);
          }
          catch (Exception ex) {
            log.Warning(ex, "Unable to get captions for {VideoID}: {Error}", v.Id, ex.Message);
            return null;
          }

          return new VideoCaptionStored2 {
            VideoId = v.Id,
            UploadDate = v.UploadDate.UtcDateTime,
            Info = track.Info,
            Captions = track.Captions
          };
        }, Cfg.ParallelGets)).NotNull().ToList();

      if (captions.Any())
        await store.Append(captions);

      log.Information("{Channel} - Saved {Captions} captions", channel.ChannelTitle, captions.Count);
    }

    async Task UpdateRecs(ChannelStored2 c, IReadOnlyCollection<Video> vids, ILogger log) {
      RecStored2 Rec(Video v, Rec r) =>
        new RecStored2 {
          FromChannelId = c.ChannelId,
          FromVideoId = v.Id,
          FromVideoTitle = v.Title,
          ToChannelTitle = r.ToChannelTitle,
          ToVideoId = r.ToVideoId,
          ToVideoTitle = r.ToVideoTitle
        };

      var recsStored = vids.SelectMany(v => v.Recs.Select(r => Rec(v, r))).ToList();
      if (recsStored.Any())
        await Store.RecStore(c.ChannelId).Append(recsStored);

      Log.Information("{Channel} - Recorded {RecCount} recs for {VideoCount} videos", c.ChannelTitle, recsStored.Count, vids.Count);
    }
  }
}