using System;
using System.Collections.Generic;
using Mutuo.Etl;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using YtReader.Yt;
using YtReader.YtWebsite;

namespace YtReader {
  public class YtStore {
    public static readonly int StoreVersion = 1;

    readonly ISimpleFileStore Store;
    readonly ILogger          Log;

    public YtStore(ISimpleFileStore store, ILogger log) {
      Store = store;
      Log = log;
      ChannelStore = new AppendCollectionStore<ChannelStored2>(Store, "channels", c => c.Updated.FileSafeTimestamp(), Log, StoreVersion.ToString());
    }

    public AppendCollectionStore<ChannelStored2> ChannelStore { get; }

    public AppendCollectionStore<VideoStored2> VideoStore(string channelId) =>
      new AppendCollectionStore<VideoStored2>(Store, StringPath.Relative("videos", channelId), v => v.UploadDate.FileSafeTimestamp(),
        Log, StoreVersion.ToString());

    public AppendCollectionStore<VideoExtraStored2> VideoExtraStore(string channelId) =>
      new AppendCollectionStore<VideoExtraStored2>(Store, StringPath.Relative("video_extra", channelId), v => v.Updated.FileSafeTimestamp(),
        Log, StoreVersion.ToString());

    public AppendCollectionStore<RecStored2> RecStore(string channelId) =>
      new AppendCollectionStore<RecStored2>(Store, StringPath.Relative("recs", channelId), r => r.Updated.FileSafeTimestamp(), Log, StoreVersion.ToString());

    public AppendCollectionStore<VideoCaptionStored2> CaptionStore(string channelId) =>
      new AppendCollectionStore<VideoCaptionStored2>(Store, StringPath.Relative("captions", channelId), c => c.UploadDate.FileSafeTimestamp(),
        Log, StoreVersion.ToString());
  }

  public class ChannelStored2 {
    public string        ChannelId     { get; set; }
    public string        ChannelTitle  { get; set; }
    public string        MainChannelId { get; set; }
    public string        Description   { get; set; }
    public string        LogoUrl       { get; set; }
    public double        Relevance     { get; set; }
    public string        LR            { get; set; }
    public ulong?        Subs          { get; set; }
    public ulong?        ChannelViews  { get; set; }
    public string        Country       { get; set; }
    public ChannelStatus Status        { get; set; }

    public IReadOnlyCollection<string>            HardTags     { get; set; }
    public IReadOnlyCollection<string>            SoftTags     { get; set; }
    public IReadOnlyCollection<UserChannelStore2> UserChannels { get; set; }

    public DateTime Updated       { get; set; }
    public string   StatusMessage { get; set; }
    public override string ToString() => $"{ChannelTitle}";
  }

  public class UserChannelStore2 {
    public string                      SheetId   { get; set; }
    public string                      LR        { get; set; }
    public int                         Relevance { get; set; }
    public IReadOnlyCollection<string> SoftTags  { get; set; } = new List<string>();
    public string                      Notes     { get; set; }
    public double                      Weight    { get; set; }
  }

  public class VideoStored2 {
    public string                VideoId      { get; set; }
    public string                Title        { get; set; }
    public string                ChannelId    { get; set; }
    public string                ChannelTitle { get; set; }
    public DateTime              UploadDate   { get; set; }
    public string                Description  { get; set; }
    public ThumbnailSet          Thumbnails   { get; set; }
    public TimeSpan              Duration     { get; set; }
    public IReadOnlyList<string> Keywords     { get; set; } = new List<string>();
    public Statistics            Statistics   { get; set; }
    public DateTime              Updated      { get; set; }
    //public IReadOnlyCollection<ClosedCaptionTrackInfo> Captions { get; set; }

    public override string ToString() => $"{Title}";
  }

  public class RecStored2 : Rec {
    public string   FromVideoId    { get; set; }
    public string   FromVideoTitle { get; set; }
    public string   FromChannelId  { get; set; }
    public DateTime Updated        { get; set; }

    public override string ToString() => $"{FromVideoTitle} -> {ToVideoTitle}";
  }

  public class VideoCaptionStored2 : StoredItem {
    public string                             VideoId    { get; set; }
    public DateTime                           UploadDate { get; set; }
    public ClosedCaptionTrackInfo             Info       { get; set; }
    public IReadOnlyCollection<ClosedCaption> Captions   { get; set; } = new List<ClosedCaption>();
  }

  public class VideoExtraStored2 {
    public string   Id           { get; set; }
    public string   ChannelId    { get; set; }
    public string   ChannelTitle { get; set; }
    public bool?    HasAd        { get; set; }
    public string   Error        { get; set; }
    public string   SubError     { get; set; }
    public DateTime Updated      { get; set; }
  }

  public abstract class StoredItem {
    public DateTime Updated { get; set; }
  }
}