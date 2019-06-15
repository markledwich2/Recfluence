using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Polly;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Net;
using SysExtensions.Text;
using YoutubeExplode;
using YoutubeExplode.Exceptions;
using YoutubeExplode.Models.ClosedCaptions;

namespace YtReader {
  public class YtStore {
    readonly YoutubeClient ytScaper = new YoutubeClient();

    public YtStore(YtClient reader, ISimpleFileStore store) {
      Yt = reader;
      Store = store;

      Channels = new FileCollection<ChannelStored>(Store, v => v.ChannelId, "Channels", CollectionCacheType.Memory,
        CacheDataDir);
      ChannelVideosCollection =
        new FileCollection<ChannelVideosStored>(Store, c => c.ChannelId, "ChannelVideos", CollectionCacheType.Memory,
          CacheDataDir);
      Videos = new FileCollection<VideoStored>(Store, v => v.VideoId, "Videos", Yt.Cfg.CacheType, CacheDataDir);
      RecommendedVideosCollection =
        new FileCollection<RecommendedVideoStored>(Store, v => v.VideoId, "RecommendedVideos", Yt.Cfg.CacheType,
          CacheDataDir);
    }

    FPath CacheDataDir => "Data".AsPath().InAppData(Setup.AppName);

    public ISimpleFileStore Store { get; }
    YtClient Yt { get; }
    AppCfg Cfg => Yt.Cfg;
    YtReaderCfg RCfg => Cfg.YtReader;

    public FileCollection<VideoStored> Videos { get; }
    public FileCollection<ChannelStored> Channels { get; }
    public FileCollection<ChannelVideosStored> ChannelVideosCollection { get; }

    public FileCollection<RecommendedVideoStored> RecommendedVideosCollection { get; }

    /// <summary>
    ///   Gets the video with that ID. Caches in S3 (including historical information) with this
    /// </summary>
    public async Task<(VideoStored Video, UpdateStatus Status)> GetAndUpdateVideo(string id) {
      var v = await Videos.Get(id);
      if (v != null && v.Latest.Updated == default(DateTime))
        v.Latest.Updated = v.Latest.Stats.Updated;

      var status = v == null ? UpdateStatus.Created : UpdateStatus.Updated;
      var needsNewStats = v == null || Expired(v.Latest.Updated, VideoRefreshAge(v.Latest));
      if (!needsNewStats) return (v, status);

      var videoData = await Yt.VideoData(id);
      if (videoData != null) {
        if (v == null)
          v = new VideoStored {Latest = videoData};
        else
          v.SetLatest(videoData);
        v.Latest.Updated = DateTime.UtcNow;
      }

      if (v != null)
        await Videos.Set(v);

      return (v, status);
    }

    public bool VideoDead(ChannelVideoListItem v) => Expired(v.PublishedAt, RCfg.VideoDead);

    TimeSpan VideoRefreshAge(ChannelVideoListItem v) =>
      Expired(v.PublishedAt, RCfg.VideoDead) ? TimeSpan.MaxValue :
      Expired(v.PublishedAt, RCfg.VideoOld) ? RCfg.RefreshOldVideos : RCfg.RefreshYoungVideos;

    public async Task<ChannelStored> GetAndUpdateChannel(string id) {
      var c = await Channels.Get(id);
      var lastUpdated = c?.Latest?.Stats?.Updated;
      var needsNewStats = lastUpdated == null || Expired(lastUpdated.Value, RCfg.RefreshChannel);
      if (!needsNewStats) return c;

      var channelData = await Yt.ChannelData(id);
      if (c == null)
        c = new ChannelStored {Latest = channelData};
      else
        c.SetLatest(channelData);

      await Channels.Set(c);

      return c;
    }

    bool Expired(DateTime updated, TimeSpan refreshAge) => (RCfg.To ?? DateTime.UtcNow) - updated > refreshAge;

    public async Task<ChannelVideosStored> GetAndUpdateChannelVideos(ChannelData c) {
      var cv = await ChannelVideosCollection.Get(c.Id);

      // fix updated if missing. Remove once all records have been updated
      var mostRecent = cv?.Vids.OrderByDescending(v => v.Updated).FirstOrDefault();
      if (cv != null && mostRecent != null && cv.Updated == default(DateTime))
        cv.Updated = mostRecent.Updated;

      var needsUpdate = cv == null || Expired(cv.Updated, RCfg.RefreshChannelVideos)
                                   || cv.From != RCfg.From; // when from is chaged, update all videos
      if (!needsUpdate) return cv;

      if (cv == null)
        cv = new ChannelVideosStored {ChannelId = c.Id, ChannelTitle = c.Title, Updated = DateTime.UtcNow};
      else
        cv.Updated = DateTime.UtcNow;

      var queryForm = cv.From != RCfg.From ? RCfg.From : mostRecent?.PublishedAt ?? RCfg.From;
      var created = await Yt.VideosInChannel(c, queryForm, RCfg.To);

      cv.Vids.AddRange(created);
      cv.From = RCfg.From;
      await ChannelVideosCollection.Set(cv);

      return cv;
    }

    public async Task<ChannelVideosStored> ChannelVideosStored(ChannelData c) => await ChannelVideosCollection.Get(c.Id);

    TimeSpan RecommendedRefreshAge(ChannelVideoListItem v) =>
      Expired(v.PublishedAt, RCfg.VideoDead)
        ? TimeSpan.MaxValue
        : Expired(v.PublishedAt, RCfg.VideoOld)
          ? RCfg.RefreshOldRecommendedVideos
          : RCfg.RefreshYoungRecommendedVideos;

    public async Task<RecommendedVideoStored> GetAndUpdateRecommendedVideos(ChannelVideoListItem v) {
      var rv = await RecommendedVideosCollection.Get(v.VideoId);

      var age = RecommendedRefreshAge(v);
      var needsUpdate = rv == null || Expired(rv.Updated, age);
      if (!needsUpdate)
        return rv;

      if (rv == null)
        rv = new RecommendedVideoStored {VideoId = v.VideoId, VideoTitle = v.VideoTitle, Updated = DateTime.UtcNow};

      var created = await Yt.GetRelatedVideos(v.VideoId);
      rv.Recommended.Add(new RecommendedVideos {Updated = DateTime.UtcNow, Top = RCfg.Related, Recommended = created});
      rv.Updated = DateTime.UtcNow;
      await RecommendedVideosCollection.Set(rv);
      return rv;
    }

    public async Task<string> GetAndUpdateVideoCaptions(string channelId, string videoId, ILogger log) {
      IReadOnlyList<ClosedCaptionTrackInfo> tracks;
      try {
        tracks = await ytScaper.GetVideoClosedCaptionTrackInfosAsync(videoId);
      }
      catch (Exception ex) {
        log.Warning(ex, "Unable to get captions for {VideoID}: {Error}", videoId, ex.Message);
        return null;
      }
      var en = tracks.FirstOrDefault(t => t.Language.Code == "en");
      if (en == null) return null;

      ClosedCaptionTrack track;
      try {
        track = await Policy.Handle<HttpRequestException>()
          .RetryWithBackoff()
          .ExecuteAsync(() => ytScaper.GetClosedCaptionTrackAsync(en));
      }
      catch (Exception ex) {
        log.Warning(ex, "Unable to get captions for {VideoID}: {Error}", videoId, ex.Message);
        return null;
      }
      var text = track.Captions.Select(c => c.Text).Join("\n");

      if (text != null) {
        var path = StringPath.Relative("VideoCaptions", channelId, $"{videoId}.txt");
        try {
          await Store.Save(path, text.AsStream());
        }
        catch (Exception ex) {
          log.Warning(ex, "Error when saving captions {Path}", path);
        }
      }
      return text;
    }
  }

  public class RecommendedVideoStored {
    public string VideoId { get; set; }
    public string VideoTitle { get; set; }
    public ICollection<RecommendedVideos> Recommended { get; set; } = new List<RecommendedVideos>();
    public DateTime Updated { get; set; }
  }

  public class RecommendedVideos {
    public DateTime Updated { get; set; }
    public int Top { get; set; }
    public ICollection<RecommendedVideoListItem> Recommended { get; set; } = new List<RecommendedVideoListItem>();
  }

  public class ChannelVideosStored {
    public string ChannelId { get; set; }
    public string ChannelTitle { get; set; }
    public DateTime Updated { get; set; }
    public DateTime From { get; set; }

    [JsonIgnore]
    public IKeyedCollection<string, ChannelVideoListItem> Vids { get; set; } =
      new KeyedCollection<string, ChannelVideoListItem>(v => v.VideoId);

    [JsonProperty("videos")]
    public ChannelVideoListItem[] SerializedVideos {
      get => Vids.OrderBy(v => v.PublishedAt).ToArray();
      set => Vids.Init(value);
    }
  }

  public enum UpdateStatus {
    Updated,
    Created
  }
  
  public class VideoStored {
    public string VideoId => Latest?.VideoId;
    public string VideoTitle => Latest?.VideoTitle;
    public VideoData Latest { get; set; }
    public ICollection<VideoStats> History { get; set; } = new List<VideoStats>();

    public void SetLatest(VideoData v) {
      History.Add(Latest.Stats);
      Latest = v;
    }
  }

  public class ChannelStored {
    public string ChannelId => Latest?.Id;
    public string ChannelTitle => Latest?.Title;

    public ChannelData Latest { get; set; }
    public ICollection<ChannelStats> History { get; set; } = new List<ChannelStats>();

    public void SetLatest(ChannelData c) {
      History.Add(Latest.Stats);
      Latest = c;
    }

    public override string ToString() => $"{ChannelTitle}";
  }

  public class ChannelRecommendations {
    string ChannelId { get; set; }
    string ChannelTitle { get; set; }
    public ICollection<Recommendation> Recomendations { get; set; } = new List<Recommendation>();
  }

  public class Recommendation {
    public Recommendation() { }

    public Recommendation(VideoItem from, RecommendedVideoListItem to) {
      From = from;
      To = to;
    }

    public VideoItem From { get; set; }
    public RecommendedVideoListItem To { get; set; }
    public DateTime Updated { get; set; }
  }
}