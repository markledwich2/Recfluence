using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;
using Humanizer;
using Parquet;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YtReader {
  public class YtCollect {
    public YtCollect(YtStore store, ISimpleFileStore simpleFileStore, AppCfg cfg, ILogger log) {
      Yt = store;
      Store = simpleFileStore;
      Cfg = cfg;
      Log = log;
    }

    AppCfg Cfg { get; }
    ILogger Log { get; }
    YtStore Yt { get; }
    public ISimpleFileStore Store { get; }

    FPath LocalDataDir => "Data".AsPath().InAppData(Setup.AppName);
    FPath LocalResultsDir => "Results".AsPath().InAppData(Setup.AppName);

    /// <summary>
    ///   For the configured time period creates the following
    ///   Channels.parquet - Basic channel info and statistics about recommendations at the granularity of Channel,Date
    ///   Recommends.parquet - Details about video recommendations at the granularity of From,To,Date ??
    /// </summary>
    /// <returns></returns>
    public async Task SaveChannelRelationData() {
      var sw = Stopwatch.StartNew();
      var analysisDir = DateTime.UtcNow.ToString("yyyy-MM-dd");
      await SaveCfg(analysisDir);
      
      var seeds = await ChannelSheets.Channels(Cfg.Sheets, Log);
      await Task.WhenAll(SaveChannels(), SaveVideosAndRecommends());

      async Task SaveVideosAndRecommends() {
        var par = (int) Math.Sqrt(Cfg.ParallelCollect);
        var vrTransform =
          new TransformBlock<ChannelWithUserData, (IReadOnlyCollection<VideoRow> vids, IReadOnlyCollection<RecommendRow> recs)>(
            async c => {
              var vids = (await (await ChannelVideos(c)).NotNull().BlockTransform(Video, par)).NotNull().ToReadOnly();
              var recs = (await vids.BlockTransform(Recommends, par)).NotNull().SelectMany(r => r).NotNull().ToReadOnly();
              return (vids, recs);
            },
            new ExecutionDataflowBlockOptions {MaxDegreeOfParallelism = par, EnsureOrdered = false}
          );
        
        var produceTask = seeds.Produce(vrTransform);
        var vidSink = new RowSink<VideoRow>((c, name) => SaveParquet(c, name, analysisDir), "Videos", 500_000);
        var recSink = new RowSink<RecommendRow>((c, name) => SaveParquet(c, name, analysisDir), "Recommends", 500_000);

        while (await vrTransform.OutputAvailableAsync()) {
          var (vids, recs) = await vrTransform.ReceiveAsync();
          await Task.WhenAll(vidSink.Add(vids), recSink.Add(recs));
        }

        await Task.WhenAll(vidSink.End(), recSink.End());
        
        Log.Information("Completed collect of {Channels} in {Duration}", seeds.Count, sw.Elapsed.Humanize(2));
        await produceTask;
      }

      async Task SaveChannels() {
          var channels = await seeds.BlockTransform(Channel, Cfg.Parallel,
            progressUpdate: p => Log.Information("Collecting channels {Channels}/{Total}. {Speed}", p.Results.Count,
              seeds.Count, p.Speed("channels")));
          
          await SaveParquet(channels.NotNull(), "Channels", analysisDir);
      }
    }

    async Task<ICollection<ChannelVideoRow>> ChannelVideos(IChannelId c) {
      var channelVids = await Yt.ChannelVideosCollection.Get(c.Id);
      return channelVids?.Vids.Select(v => new ChannelVideoRow {
        VideoId = v.VideoId,
        PublishedAt = v.PublishedAt.ToString("O"),
        ChannelId = c.Id
      }).ToList();
    }

    async Task<VideoRow> Video(ChannelVideoRow cv) {
      var v = await Yt.Videos.Get(cv.VideoId);
      if (v == null) {
        Log.Warning("Unable to find video {Video}", cv.VideoId);
        return null;
      }

      return new VideoRow {
        VideoId = v.VideoId,
        Title = v.VideoTitle,
        ChannelId = cv.ChannelId,
        PublishedAt = v.Latest.PublishedAt.ToString("O"),
        Views = (long) (v.Latest.Stats.Views ?? 0),
        /*
        Stats = v.History.Select(h => new VideoRowStats {
          Views = (long) (h.Views ?? 0),
          Likes = (long) (h.Likes ?? 0),
          Diskiles = (long) (h.Dislikes ?? 0),
          UpdatedAt = h.Updated.ToString("O")
        }).ToList()*/
      };
    }

    async Task<ChannelRow> Channel(ChannelWithUserData c) {
      var channel = await Yt.Channels.Get(c.Id);
      if(channel == null) {
        Log.Error("Unable to find seed channel {Channel}", c.Title);
        return null;
      }
      
      return new ChannelRow {
        ChannelId = channel.ChannelId,
        Title = channel.ChannelTitle,
        SubCount = (long) (channel.Latest?.Stats?.SubCount ?? 0),
        ViewCount = (long) (channel.Latest?.Stats?.ViewCount ?? 0),
        Thumbnail = channel.Latest?.Thumbnails?.Medium?.Url,
        UpdatedAt = channel.Latest?.Stats?.Updated.ToString("O"),
        Country = channel.Latest?.Country,
        Relevance = c.Relevance,
        Tags = c.HardTags.Concat(c.SoftTags).ToArray(),
        LR = c.LR,
        SheetIds = c.SheetIds.ToArray()
      };
    }

    async Task<IReadOnlyCollection<VideoStored>> ChannelVideoStats(ChannelStored c) {
      var channelVideos = await Yt.ChannelVideosCollection.Get(c.ChannelId);
      var channelVideoStats = await channelVideos.Vids.BlockTransform(v => Yt.Videos.Get(v.VideoId));
      return channelVideoStats;
    }

    async Task<IReadOnlyCollection<RecommendRow>> Recommends(VideoRow v) {
      var recommends = await Yt.RecommendedVideosCollection.Get(v.VideoId);
      if (recommends == null) {
        Log.Warning("Unable to find video recommends {VideoId}", v.VideoId);
        return null;
      }

      var flattened = recommends.Recommended
        .SelectMany(r => r.Recommended, (update, to) => new RecommendRow {
          ChannelId = to.ChannelId,
          VideoId = to.VideoId,
          FromChannelId = v.ChannelId,
          FromVideoId = v.VideoId,
          Rank = to.Rank,
          UpdatedAt = update.Updated.DateString()
        })
        .Where(r => r.FromChannelId != r.ChannelId)
        .ToList();

      return flattened;
    }

    async Task SaveJsonl<T>(IEnumerable<T> rows, string name, string dir) {
      var localFile = LocalResultsDir.Combine(dir).Combine($"{name}.jsonl");
      rows.ToJsonl(localFile.FullPath);
      await Upload(dir, localFile);
    }

    async Task SaveParquet<T>(IEnumerable<T> rows, string name, string dir) where T : new() {
      var localFile = LocalResultsDir.Combine(dir).Combine($"{name}.parquet");
      ParquetConvert.Serialize(rows, localFile.FullPath);
      await Upload(dir, localFile);
    }

    async Task Upload(string dir, FPath localFile) {
      var storeDir = StringPath.Relative(dir);
      var storePath = storeDir.Add(localFile.FileName);
      await Store.Save(storePath, localFile);
      Log.Information("Saved {Path}", storePath);
    }

    async Task SaveCfg(string dir) {
      var localDir = LocalResultsDir.Combine(dir);
      localDir.EnsureDirectoryExists();
      var storeDir = StringPath.Relative(dir);
      var localCfgFile = localDir.Combine("cfg.json");
      Cfg.ToJsonFile(localCfgFile);
      await Store.Save(storeDir.Add("cfg.json"), localCfgFile);
    }

    class RowSink<T> {
      readonly List<T> _buffer = new List<T>();
      readonly int _maxItems;
      readonly string _name;
      readonly Func<IReadOnlyCollection<T>, string, Task> _save;
      int _fileNum;

      public RowSink(Func<IReadOnlyCollection<T>, string, Task> save, string name, int maxItems = 100000) {
        _save = save;
        _maxItems = maxItems;
        _name = name;
      }

      public async Task Add(IEnumerable<T> items) {
        _buffer.AddRange(items);
        if (_buffer.Count >= _maxItems) await Save();
      }

      async Task Save() {
        await _save(_buffer, $"{_name}.{_fileNum}");
        _buffer.Clear();
        _fileNum++;
      }

      public async Task End() {
        if (_buffer.Count > 0) await Save();
      }
    }
  }

  public class ChannelVideoRow {
    public string VideoId { get; set; }
    public string ChannelId { get; set; }
    public string PublishedAt { get; set; }
  }

  public class RecommendRow {
    public string VideoId { get; set; }
    public string ChannelId { get; set; }
    public string FromVideoId { get; set; }
    public string FromChannelId { get; set; }

    public int Rank { get; set; }
    public string UpdatedAt { get; set; }
  }

  public class ChannelRow {
    public string ChannelId { get; set; }
    public string Title { get; set; }
    public long ViewCount { get; set; }
    public long SubCount { get; set; }
    public string Thumbnail { get; set; }
    public string UpdatedAt { get; set; }
    public string Country { get; set; }
    
    public double Relevance { get; set; }
    public string LR { get; set; }
    public string[] Tags { get; set; }
    public string[] SheetIds { get; set; }
  }

  public class VideoRow {
    public string VideoId { get; set; }
    public string Title { get; set; }
    public string ChannelId { get; set; }
    public string PublishedAt { get; set; }
    public long Views { get; set; }

    //public List<VideoRowStats> Stats { get; set; }
    //public string[] Tags { get; set; }
  }

  public struct VideoRowStats {
    public string UpdatedAt { get; set; }
    public long Views { get; set; }
    public long Likes { get; set; }
    public long Diskiles { get; set; }
  }
}