using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Mutuo.Etl.Db;
using SysExtensions.Collections;
using SysExtensions.Text;
using Troschuetz.Random;
using YtReader.Store;

namespace YtReader.Yt {
  
  public static class YtCollectEx {
    public static bool OlderThanOrNull(this DateTime? updated, TimeSpan age, DateTime? now = null) =>
      updated == null || updated.Value.OlderThan(age, now);

    public static bool OlderThan(this DateTime updated, TimeSpan age, DateTime? now = null) => (now ?? DateTime.UtcNow) - updated > age;
    public static bool YoungerThan(this DateTime updated, TimeSpan age, DateTime? now = null) => !updated.OlderThan(age, now);

    public static RecStored[] ToRecStored(IReadOnlyCollection<ExtraAndParts> allExtra, DateTime updated) =>
      allExtra
        .SelectMany(v => v.Recs?.Select((r, i) => new RecStored {
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
        }) ?? new RecStored[] { }).ToArray();
    
    
    public static Video[] ToVidsStored(Channel c, IReadOnlyCollection<YtVideoItem> vids) =>
      vids.Select(v => new Video {
        VideoId = v.Id,
        Title = v.Title,
        Duration = v.Duration,
        Statistics = v.Statistics,
        ChannelId = c.ChannelId,
        ChannelTitle = c.ChannelTitle,
        UploadDate = v.UploadDate,
        Updated = DateTime.UtcNow, 
        Platform = Platform.YouTube
      }).ToArray();
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
    Comments,
    [CollectPart(Explicit = true)] DiscoverPart
  }

  public record CollectOptions {
    public string[]                             LimitChannels { get; init; }
    public CollectPart[]                        Parts         { get; init; }
    public (CollectFromType Type, string Value) CollectFrom   { get; init; }
  }

  public enum CollectFromType {
    None,
    VideosPath,
    VideosView,
    ChannelsPath
  }

  public record ExtraAndParts(VideoExtra Extra) {
    public Rec[]          Recs     { get; init; } = Array.Empty<Rec>();
    public VideoComment[] Comments { get; init; } = Array.Empty<VideoComment>();
    public VideoCaption   Caption  { get; init; } = null;
  }

  public class ProcessChannelResult {
    public string ChannelId { get; set; }
    public bool   Success   { get; set; }
  }

  public class ProcessChannelResults {
    public ProcessChannelResult[] Channels { get; set; }
    public TimeSpan               Duration { get; set; }
  }

  public static class YtCollectorRegion {
    static readonly Region[] Regions = {Region.USEast, Region.USWest, Region.USWest2, Region.USEast2, Region.USSouthCentral};
    static readonly TRandom  Rand    = new();

    public static Region RandomUsRegion() => Rand.Choice(Regions);
  }

  /// <summary>Helps build up a plan for updating videos/summary>
  public record VideoExtraPlans : IEnumerable<VideoPlan> {
    readonly IKeyedCollection<string, VideoPlan> _c = new KeyedCollection<string, VideoPlan>(v => v.VideoId);

    public VideoExtraPlans() { }

    public VideoExtraPlans(IEnumerable<string> videosForExtra) {
      foreach (var v in videosForExtra)
        SetPart(v, ExtraPart.Extra);
    }

    public VideoExtraPlans(IEnumerable<VideoPlan> plans) => _c.AddRange(plans);

    VideoPlan GetOrAdd(string videoId) => _c.GetOrAdd(videoId, () => new VideoPlan(videoId));

    /// <summary>Adds a video to update with the given parts unioned with what is existing</summary>
    public void SetPart(string videoId, params ExtraPart[] parts) {
      var r = GetOrAdd(videoId);
      r.Parts = r.Parts.Union(parts).ToArray();
    }

    public void SetPart(IEnumerable<string> videoIds, params ExtraPart[] parts) {
      foreach (var v in videoIds)
        SetPart(v, parts);
    }

    public void SetForUpdate(VideoForUpdate forUpdate) {
      var plan = GetOrAdd(forUpdate.VideoId);
      plan.ForUpdate = forUpdate;
    }

    public IEnumerator<VideoPlan> GetEnumerator() => _c.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable) _c).GetEnumerator();
    public VideoPlan this[string videoId] => _c[videoId];

    public IEnumerable<VideoPlan> WithPart(ExtraPart part) => this.Where(p => p.Parts.Contains(part));

    public VideoPlan[] SerializableItems {
      get => _c.ToArray();
      set => _c.AddRange(value);
    }
  }

  public record VideoForUpdate(string ChannelId, string VideoId, DateTime Updated, DateTime? UploadDate, DateTime? ExtraUpdated);

  public record VideoPlan {
    public VideoPlan(string videoId) => VideoId = videoId;

    public string         VideoId   { get; set; }
    public ExtraPart[]    Parts     { get; set; } = Array.Empty<ExtraPart>();
    public VideoForUpdate ForUpdate { get; set; }
  }
}