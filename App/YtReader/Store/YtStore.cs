﻿using System;
using System.Collections.Generic;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using YtReader.Yt;
using YtReader.YtWebsite;

namespace YtReader.Store {
  public enum DataStoreType {
    Pipe,
    Db,
    Results,
    Private,
    Backup
  }

  /// <summary>Access to any of the stores</summary>
  public class YtStores {
    readonly StorageCfg Cfg;
    readonly ILogger    Log;

    public YtStores(StorageCfg cfg, ILogger log) {
      Cfg = cfg;
      Log = log;
    }

    public AzureBlobFileStore Store(DataStoreType type) => type switch {
      DataStoreType.Backup => new AzureBlobFileStore(Cfg.BackupCs, Cfg.BackupRootPath, Log),
      _ => new AzureBlobFileStore(Cfg.DataStorageCs, StoragePath(type), Log)
    };

    StringPath StoragePath(DataStoreType type) =>
      Cfg.RootPath + "/" + type switch {
        DataStoreType.Pipe => Cfg.PipePath,
        DataStoreType.Db => Cfg.DbPath,
        DataStoreType.Private => Cfg.PrivatePath,
        DataStoreType.Results => Cfg.ResultsPath,
        _ => throw new NotImplementedException($"StoryType {type} not supported")
      };
  }

  /// <summary>Typed access to jsonl blob collections</summary>
  public class YtStore {
    public static readonly int     StoreVersion = 1;
    readonly               ILogger Log;

    public YtStore(ISimpleFileStore store, ILogger log) {
      Store = store;
      Log = log;
      Channels = CreateStore<ChannelStored2>("channels");
      Searches = CreateStore<UserSearchWithUpdated>("searches");
      Videos = CreateStore<VideoStored2>("videos", v => v.ChannelId);
      VideoExtra = CreateStore<VideoExtraStored2>("video_extra");
      Recs = CreateStore<RecStored2>("recs", r => r.FromChannelId);
      Captions = CreateStore<VideoCaptionStored2>("captions", c => c.ChannelId);
    }

    public ISimpleFileStore Store { get; }

    public JsonlStore<ChannelStored2>        Channels   { get; }
    public JsonlStore<UserSearchWithUpdated> Searches   { get; }
    public JsonlStore<VideoStored2>          Videos     { get; }
    public JsonlStore<VideoExtraStored2>     VideoExtra { get; }
    public JsonlStore<RecStored2>            Recs       { get; }
    public JsonlStore<VideoCaptionStored2>   Captions   { get; }

    public IJsonlStore[] AllStores => new IJsonlStore[] {Channels, Searches, Videos, VideoExtra, Recs, Captions};

    JsonlStore<T> CreateStore<T>(string name, Func<T, string> getPartition = null) where T : IHasUpdated =>
      new JsonlStore<T>(Store, name, c => c.Updated.FileSafeTimestamp(), Log, StoreVersion.ToString(), getPartition);
  }

  public class ChannelStored2 : WithUpdatedItem {
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

    public string StatusMessage { get; set; }
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

  public class VideoStored2 : WithUpdatedItem {
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

    public override string ToString() => $"{Title}";
  }

  public class RecStored2 : Rec, IHasUpdated {
    public string   FromVideoId    { get; set; }
    public string   FromVideoTitle { get; set; }
    public string   FromChannelId  { get; set; }
    public DateTime Updated        { get; set; }

    public override string ToString() => $"{FromVideoTitle} -> {ToVideoTitle}";
  }

  public class VideoCaptionStored2 : WithUpdatedItem {
    public string                             ChannelId  { get; set; }
    public string                             VideoId    { get; set; }
    public DateTime                           UploadDate { get; set; }
    public ClosedCaptionTrackInfo             Info       { get; set; }
    public IReadOnlyCollection<ClosedCaption> Captions   { get; set; } = new List<ClosedCaption>();
  }

  public class VideoExtraStored2 : WithUpdatedItem {
    public string Id           { get; set; }
    public string ChannelId    { get; set; }
    public string ChannelTitle { get; set; }
    public bool?  HasAd        { get; set; }
    public string Error        { get; set; }
    public string SubError     { get; set; }
  }

  public interface IHasUpdated {
    DateTime Updated { get; }
  }

  public abstract class WithUpdatedItem : IHasUpdated {
    public DateTime Updated { get; set; }
  }

  public class UserSearchWithUpdated : WithUpdatedItem {
    public string Origin { get; set; }
    /// <summary>Email of the user performing the search</summary>
    public string Email { get;        set; }
    public string   Query      { get; set; }
    public string[] Ideologies { get; set; }
    public string[] Channels   { get; set; }
  }
}