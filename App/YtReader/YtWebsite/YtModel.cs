using System;
using System.Collections.Generic;

// all of this only minor midifications to https://github.com/Tyrrrz/YoutubeExplode

namespace YtReader.YtWebsite {
  public class VideoItem {
    /// <summary>ID of this video.</summary>
    public string Id { get; }

    /// <summary>Author of this video.</summary>
    public string Author { get; }

    /// <summary>Upload date of this video.</summary>
    public DateTime UploadDate { get; }
    
    
    /// <summary>
    /// This is the same as upload date. But sometimes there are discrepancies and added is more reliable
    /// </summary>
    public DateTime? AddedDate { get; }

    /// <summary>Title of this video.</summary>
    public string Title { get; }

    /// <summary>Description of this video.</summary>
    public string Description { get; }

    /// <summary>Thumbnails of this video.</summary>
    public ThumbnailSet Thumbnails { get; }

    /// <summary>Duration of this video.</summary>
    public TimeSpan Duration { get; }

    /// <summary>Search keywords of this video.</summary>
    public IReadOnlyList<string> Keywords { get; }

    /// <summary>Statistics of this video.</summary>
    public Statistics Statistics { get; }

    public string ChannelId    { get; }
    public string ChannelTitle { get; }

    /// <summary>Initializes an instance of <see cref="Video" />.</summary>
    public VideoItem(string id, string author, DateTime uploadDate, DateTime? addedDate, string title, string description, TimeSpan duration, IReadOnlyList<string> keywords, Statistics statistics, string channelId, string channelTitle) {
      Id = id;
      Author = author;
      UploadDate = uploadDate;
      AddedDate = addedDate;
      Title = title;
      Description = description;
      Duration = duration;
      Keywords = keywords;
      Statistics = statistics;
      ChannelId = channelId;
      ChannelTitle = channelTitle;
    }

    /// <inheritdoc />
    public override string ToString() => Title;
  }

  /// <summary>User activity statistics.</summary>
  public class Statistics {
    /// <summary>View count.</summary>
    public ulong? ViewCount { get; set; }

    /// <summary>Like count.</summary>
    public ulong? LikeCount { get; set; }

    /// <summary>Dislike count.</summary>
    public ulong? DislikeCount { get; set; }
    
    /// <summary>Initializes an instance of <see cref="Statistics" />.</summary>
    public Statistics(ulong? viewCount, ulong? likeCount = null, ulong? dislikeCount = null) {
      ViewCount = viewCount;
      LikeCount = likeCount;
      DislikeCount = dislikeCount;
    }
  }

  /// <summary>Set of thumbnails for a video.</summary>
  public class ThumbnailSet {
    readonly string _videoId;

    /// <summary>Low resolution thumbnail URL.</summary>
    public string LowResUrl => $"https://img.youtube.com/vi/{_videoId}/default.jpg";

    /// <summary>Medium resolution thumbnail URL.</summary>
    public string MediumResUrl => $"https://img.youtube.com/vi/{_videoId}/mqdefault.jpg";

    /// <summary>High resolution thumbnail URL.</summary>
    public string HighResUrl => $"https://img.youtube.com/vi/{_videoId}/hqdefault.jpg";

    /// <summary>Standard resolution thumbnail URL. Not always available.</summary>
    public string StandardResUrl => $"https://img.youtube.com/vi/{_videoId}/sddefault.jpg";

    /// <summary>Max resolution thumbnail URL. Not always available.</summary>
    public string MaxResUrl => $"https://img.youtube.com/vi/{_videoId}/maxresdefault.jpg";

    /// <summary>Initializes an instance of <see cref="ThumbnailSet" />.</summary>
    public ThumbnailSet(string videoId) => _videoId = videoId;
  }

  public partial class Playlist {
    /// <summary>ID of this playlist.</summary>
    public string Id { get; }

    /// <summary>Type of this playlist.</summary>
    public PlaylistType Type { get; }

    /// <summary>Author of this playlist.</summary>
    public string Author { get; }

    /// <summary>Title of this playlist.</summary>
    public string Title { get; }

    /// <summary>Description of this playlist.</summary>
    public string Description { get; }

    /// <summary>Statistics of this playlist.</summary>
    public Statistics Statistics { get; }

    /// <summary>Collection of videos contained in this playlist.</summary>
    public IAsyncEnumerable<IReadOnlyCollection<VideoItem>> Videos { get; }

    /// <summary>Initializes an instance of <see cref="Playlist" />.</summary>
    public Playlist(string id, string author, string title, string description, Statistics statistics,
      IAsyncEnumerable<IReadOnlyCollection<VideoItem>> videos) {
      Id = id;
      Type = GetPlaylistType(id);
      Author = author;
      Title = title;
      Description = description;
      Statistics = statistics;
      Videos = videos;
    }

    /// <inheritdoc />
    public override string ToString() => Title;
  }

  public partial class Playlist {
    /// <summary>Get playlist type by ID.</summary>
    protected static PlaylistType GetPlaylistType(string id) {
      if (id.StartsWith("PL", StringComparison.Ordinal))
        return PlaylistType.Normal;

      if (id.StartsWith("RD", StringComparison.Ordinal))
        return PlaylistType.VideoMix;

      if (id.StartsWith("UL", StringComparison.Ordinal))
        return PlaylistType.ChannelVideoMix;

      if (id.StartsWith("UU", StringComparison.Ordinal))
        return PlaylistType.ChannelVideos;

      if (id.StartsWith("PU", StringComparison.Ordinal))
        return PlaylistType.PopularChannelVideos;

      if (id.StartsWith("OL", StringComparison.Ordinal))
        return PlaylistType.MusicAlbum;

      if (id.StartsWith("LL", StringComparison.Ordinal))
        return PlaylistType.LikedVideos;

      if (id.StartsWith("FL", StringComparison.Ordinal))
        return PlaylistType.Favorites;

      if (id.StartsWith("WL", StringComparison.Ordinal))
        return PlaylistType.WatchLater;

      throw new ArgumentOutOfRangeException(nameof(id), $"Unexpected playlist ID [{id}].");
    }
  }

  /// <summary>Playlist type.</summary>
  public enum PlaylistType {
    /// <summary>Regular playlist created by a user.</summary>
    Normal,

    /// <summary>Mix playlist generated to group similar videos.</summary>
    VideoMix,

    /// <summary>Mix playlist generated to group similar videos uploaded by the same channel.</summary>
    ChannelVideoMix,

    /// <summary>Playlist generated from channel uploads.</summary>
    ChannelVideos,

    /// <summary>Playlist generated from popular channel uploads.</summary>
    PopularChannelVideos,

    /// <summary>Playlist generated from automated music videos.</summary>
    MusicAlbum,

    /// <summary>System playlist for videos liked by a user.</summary>
    LikedVideos,

    /// <summary>System playlist for videos favorited by a user.</summary>
    Favorites,

    /// <summary>System playlist for videos user added to watch later.</summary>
    WatchLater
  }

  /// <summary>Text that gets displayed at specific time during video playback, as part of a
  ///   <see cref="ClosedCaptionTrack" />.</summary>
  public class ClosedCaption {
    /// <summary>Text displayed by this caption.</summary>
    public string Text { get; }

    /// <summary>Time at which this caption starts being displayed.</summary>
    public TimeSpan? Offset { get; }

    /// <summary>Duration this caption is displayed.</summary>
    public TimeSpan? Duration { get; }

    /// <summary>Initializes an instance of <see cref="ClosedCaption" />.</summary>
    public ClosedCaption(string text, TimeSpan? offset, TimeSpan? duration) {
      Text = text;
      Offset = offset;
      Duration = duration;
    }

    public override string ToString() => Text;
  }

  /// <summary>Set of captions that get displayed during video playback.</summary>
  public class ClosedCaptionTrack {
    /// <summary>Metadata associated with this track.</summary>
    public ClosedCaptionTrackInfo Info { get; }

    /// <summary>Collection of closed captions that belong to this track.</summary>
    public IReadOnlyList<ClosedCaption> Captions { get; }

    /// <summary>Initializes an instance of <see cref="ClosedCaptionTrack" />.</summary>
    public ClosedCaptionTrack(ClosedCaptionTrackInfo info, IReadOnlyList<ClosedCaption> captions) {
      Info = info;
      Captions = captions;
    }
  }

  /// <summary>Metadata associated with a certain <see cref="ClosedCaptionTrack" />.</summary>
  public class ClosedCaptionTrackInfo {
    /// <summary>Manifest URL of the associated track.</summary>
    public string Url { get; }

    /// <summary>Language of the associated track.</summary>
    public Language Language { get; }

    /// <summary>Whether the associated track was automatically generated.</summary>
    public bool IsAutoGenerated { get; }

    /// <summary>Initializes an instance of <see cref="ClosedCaptionTrackInfo" />.</summary>
    public ClosedCaptionTrackInfo(string url, Language language, bool isAutoGenerated) {
      Url = url;
      Language = language;
      IsAutoGenerated = isAutoGenerated;
    }

    /// <inheritdoc />
    public override string ToString() => $"{Language}";
  }

  /// <summary>Language information.</summary>
  public class Language {
    /// <summary>ISO 639-1 code of this language.</summary>
    public string Code { get; }

    /// <summary>Full English name of this language.</summary>
    public string Name { get; }

    /// <summary>Initializes an instance of <see cref="Language" />.</summary>
    public Language(string code, string name) {
      Code = code;
      Name = name;
    }

    /// <inheritdoc />
    public override string ToString() => $"{Code} ({Name})";
  }
}