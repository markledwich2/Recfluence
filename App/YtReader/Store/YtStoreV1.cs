using System;
using System.Collections.Generic;
using System.Linq;
using Newtonsoft.Json;
using SysExtensions.Collections;
using YtReader.Yt;

namespace YtReader.Store {
  public class RecommendedVideoStored {
    public string                         VideoId     { get; set; }
    public string                         VideoTitle  { get; set; }
    public ICollection<RecommendedVideos> Recommended { get; set; } = new List<RecommendedVideos>();
    public DateTime                       Updated     { get; set; }
  }

  public class RecommendedVideos {
    public DateTime                              Updated     { get; set; }
    public int                                   Top         { get; set; }
    public ICollection<RecommendedVideoListItem> Recommended { get; set; } = new List<RecommendedVideoListItem>();
  }

  public class ChannelVideosStored {
    public string   ChannelId    { get; set; }
    public string   ChannelTitle { get; set; }
    public DateTime Updated      { get; set; }
    public DateTime From         { get; set; }

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
    public string                  VideoId    => Latest?.VideoId;
    public string                  VideoTitle => Latest?.VideoTitle;
    public VideoData               Latest     { get; set; }
    public ICollection<VideoStats> History    { get; set; } = new List<VideoStats>();

    public void SetLatest(VideoData v) {
      History.Add(Latest.Stats);
      Latest = v;
    }
  }

  public class ChannelStored {
    public string ChannelId    => Latest?.Id;
    public string ChannelTitle => Latest?.Title;

    public ChannelData               Latest  { get; set; }
    public ICollection<ChannelStats> History { get; set; } = new List<ChannelStats>();

    public void SetLatest(ChannelData c) {
      History.Add(Latest.Stats);
      Latest = c;
    }

    public override string ToString() => $"{ChannelTitle}";
  }

  public class ChannelRecommendations {
    string                             ChannelId      { get; set; }
    string                             ChannelTitle   { get; set; }
    public ICollection<Recommendation> Recomendations { get; set; } = new List<Recommendation>();
  }

  public class Recommendation {
    public Recommendation() { }

    public Recommendation(VideoItem from, RecommendedVideoListItem to) {
      From = from;
      To = to;
    }

    public VideoItem                From    { get; set; }
    public RecommendedVideoListItem To      { get; set; }
    public DateTime                 Updated { get; set; }
  }
}