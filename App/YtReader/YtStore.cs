using System;
using System.Collections.Generic;
using Mutuo.Etl;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using YtReader.YtWebsite;

namespace YtReader {
  public class YtStore {
    readonly ISimpleFileStore Store;
    readonly ILogger Log;

    public YtStore(ISimpleFileStore store, ILogger log) {
      Store = store;
      Log = log;
      ChannelStore = new AppendCollectionStore<ChannelStored2>(Store, "channels", c => c.Updated.FileSafeTimestamp(), Log);
    }

    public AppendCollectionStore<ChannelStored2> ChannelStore { get; }
    
    public AppendCollectionStore<VideoStored2> VideoStore(string channelId) => 
      new AppendCollectionStore<VideoStored2>(Store, StringPath.Relative("videos", channelId), v => v.UploadDate.FileSafeTimestamp(), Log);

    public AppendCollectionStore<RecStored2> RecStore(string channelId) => 
      new AppendCollectionStore<RecStored2>(Store, StringPath.Relative("recs", channelId), r => r.Updated.FileSafeTimestamp(), Log);
    
    public AppendCollectionStore<VideoCaptionStored2> CaptionStore(string channelId) => 
      new AppendCollectionStore<VideoCaptionStored2>(Store, StringPath.Relative("captions", channelId), c => c.UploadDate.FileSafeTimestamp(), Log);
  }

  public class ChannelStored2 {
    public string ChannelId { get; set; }
    public string ChannelTitle { get; set; }
    public string LogoUrl { get; set; }
    public double Relevance { get; set; }
    public string LR { get; set; }

    public long? Subs { get; set; }

    public IReadOnlyCollection<string> HardTags { get; set; }
    public IReadOnlyCollection<string> SoftTags { get; set; }
    public IReadOnlyCollection<string> SheetIds { get; set; }

    public DateTime Updated { get; set; }
    public string StatusMessage { get; set; }
    public override string ToString() => $"{ChannelTitle}";
  }

  public class VideoStored2 {
    public string VideoId { get; set; }
    public string Title { get; set; }
    public string ChannelId { get; set; }
    public string ChannelTitle { get; set; }
    public DateTime UploadDate { get; set; }
    public string Description { get; set; }
    public ThumbnailSet Thumbnails { get; set; }
    public TimeSpan Duration { get; set; }
    public IReadOnlyList<string> Keywords { get; set; } = new List<string>();
    public Statistics Statistics { get; set; }
    //public IReadOnlyCollection<ClosedCaptionTrackInfo> Captions { get; set; }

    public override string ToString() => $"{Title}";
  }

  public class RecStored2 : Rec {
    public string FromVideoId { get; set; }
    public string FromVideoTitle { get; set; }
    public string FromChannelId { get; set; }
    public DateTime Updated { get; set; }

    public override string ToString() => $"{FromVideoTitle} -> {ToVideoTitle}";
  }

  public class VideoCaptionStored2 {
    public string VideoId { get; set; }
    public DateTime UploadDate { get; set; }
    public ClosedCaptionTrackInfo Info { get; set; }
    public IReadOnlyCollection<ClosedCaption> Captions { get; set; } = new List<ClosedCaption>();
  }
}