using System.Collections;
using System.Runtime.Serialization;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Troschuetz.Random;
using YtReader.SimpleCollect;
using YtReader.Store;
using static YtReader.Yt.ExtraPart;

namespace YtReader.Yt; 

public static class YtCollectEx {
  public static bool OlderThanOrNull(this DateTime? updated, TimeSpan age, DateTime? now = null) =>
    updated == null || updated.Value.OlderThan(age, now);

  public static bool OlderThan(this DateTime updated, TimeSpan age, DateTime? now = null) => (now ?? DateTime.UtcNow) - updated > age;
  public static bool YoungerThan(this DateTime updated, TimeSpan age, DateTime? now = null) => !updated.OlderThan(age, now);

  public static RecStored[] ToRecStored(IReadOnlyCollection<ExtraAndParts> allExtra, DateTime updated) =>
    allExtra
      .SelectMany(v => v.Recs?.Select(r => new RecStored {
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
      }) ?? Array.Empty<RecStored>()).ToArray();

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
  /// <summary>A standard & cheap update to the channel details</summary>
  Standard,
  /// <summary>Don't update the channel details. Has no impact the collection of videos/recs/caption.</summary>
  StandardNoChannel,
  /// <summary>Update the subscribers and other more costly information about a channel</summary>
  Full,
  /// <summary>Update a un-classified channels information useful for predicting political/non and tags</summary>
  Discover,
  /// <summary>Just update the channel details for a user (no video extra parts). For quota reasons we use the website for
  ///   this.</summary>
  UserChannel
}

public enum CollectPart {
  [EnumMember(Value = "channel")] PChannel,
  [EnumMember(Value = "extra")]   PChannelVideos,
  [EnumMember(Value = "user")]    PUser,
  [EnumMember(Value = "discover")] [CollectPart(Explicit = true)]
  PDiscover
}

public record CollectOptions {
  public string[]          LimitChannels { get; init; }
  public CollectPart[]     Parts         { get; init; }
  public ExtraPart[]       ExtraParts    { get; init; }
  public SimpleCollectMode CollectMode   { get; set; }
}

public record ExtraAndParts(VideoExtra Extra) {
  public Rec[]          Recs     { get; init; } = Array.Empty<Rec>();
  public VideoComment[] Comments { get; init; } = Array.Empty<VideoComment>();
  public VideoCaption   Caption  { get; init; }
}

public record ProcessChannelResult {
  public string ChannelId { get; init; }
  public bool   Success   { get; init; }
}

public record ProcessChannelResults {
  public ProcessChannelResult[] Channels { get; init; }
  public TimeSpan               Duration { get; init; }
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
      SetPart(v, EExtra);
  }

  public VideoExtraPlans(IEnumerable<VideoPlan> plans) => _c.AddRange(plans);

  public int Count => _c.Count;
  public VideoPlan this[string videoId] => _c[videoId];

  public VideoPlan[] SerializableItems {
    get => _c.ToArray();
    set => _c.AddRange(value);
  }

  public IEnumerator<VideoPlan> GetEnumerator() => _c.GetEnumerator();
  IEnumerator IEnumerable.GetEnumerator() => ((IEnumerable) _c).GetEnumerator();

  public VideoPlan GetOrAdd(string videoId) => _c.GetOrAdd(videoId, () => new(videoId));

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

  public bool ContainsVideo(string videoId) => this[videoId] != null;

  public IEnumerable<VideoPlan> WithPart(ExtraPart part) => this.Where(p => p.Parts.Contains(part));
}

public record VideoForUpdate {
  public string    ChannelId    { get; init; }
  public string    VideoId      { get; init; }
  public DateTime  Updated      { get; init; }
  public DateTime? UploadDate   { get; init; }
  public DateTime? ExtraUpdated { get; init; }
  public bool      HasComment   { get; set; }
  public string    SourceId     { get; init; }
  public Platform  Platform     { get; init; }
}

public record VideoPlan {
  public VideoPlan(string videoId) => VideoId = videoId;

  public string         VideoId   { get; set; }
  public ExtraPart[]    Parts     { get; set; } = Array.Empty<ExtraPart>();
  public VideoForUpdate ForUpdate { get; set; }

  public void SetPart(params ExtraPart[] parts) => Parts = Parts.Union(parts).ToArray();

  public override string ToString() => $"{VideoId} ({Parts.Join("|")}): {ForUpdate}";
}