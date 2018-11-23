using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Parquet;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YouTubeReader {
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
        ///     For the configured time period creates the following
        ///     Channels.parquet - Basic channel info and statistics about recommendations at the granularity of Channel,Date
        ///     Recommends.parquet - Details about video recommendations at the granularity of From,To,Date ??
        /// </summary>
        /// <returns></returns>
        public async Task SaveChannelRelationData() {
            var analysisDir = DateTime.UtcNow.ToString("yyyy-MM-dd");
            await SaveCfg(analysisDir);
            
            var channelCfg = await Cfg.LoadChannelConfig();
            var seeds = channelCfg.Seeds;
            IReadOnlyCollection<ChannelVideoRow> channelVideos = null;

            async Task LoadChannels() {
                var channels = await seeds.BlockTransform(Channel, Cfg.Parallel,
                    progressUpdate: p => Log.Information("Collecting channels {Channels}/{Total}. {Speed}", p.Results.Count, seeds.Count, p.Speed("channels")));
                await SaveParquet(channels, "Channels", analysisDir);
            }

            async Task LoadChannelVideos() {
                var cvs  = await seeds.BlockTransform(ChannelVideos, Cfg.Parallel,
                    progressUpdate: p => Log.Information("Collecting channel videos {Channels}/{Total}. {Speed}", p.Results.Count, seeds.Count, p.Speed("channels")));
                channelVideos = cvs.SelectMany(cv => cv).ToList();
            }

            await Task.WhenAll(LoadChannels(), LoadChannelVideos());
            
            var videos = (await channelVideos.BlockTransform(Video, Cfg.Parallel,
                progressUpdate: p => Log.Information("Collecting videos {Videos}/{Total}. {Speed}", p.Results.Count, channelVideos.Count, p.Speed("videos")))).NotNull().ToList();
            await SaveParquet(videos, "Videos", analysisDir);
        
            var recommendsResult = await videos.BlockTransform(Recommends, Cfg.Parallel,
                progressUpdate: p => Log.Information("Collecting channel video recommendations {Videos}/{Total}. {Speed}", p.Results.Count, channelVideos.Count, p.Speed("videos")));
            await SaveParquet(recommendsResult.SelectMany(r => r), "Recommends", analysisDir);
        }

        async Task<ICollection<ChannelVideoRow>> ChannelVideos(SeedChannel c) {
            var channelVids = await Yt.ChannelVideosCollection.Get(c.Id);
            return channelVids.Vids.Select(v => new ChannelVideoRow {
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
                Views = (long) (v.Latest.Stats.Views ?? 0),
                PublishedAt = v.Latest.PublishedAt.ToString("O"),
                Tags = v.Latest.Tags.NotNull().ToArray()
            };
        }

        async Task<ChannelRow> Channel(SeedChannel c) {
            var channel = await Yt.Channels.Get(c.Id);
            return new ChannelRow {
                ChannelId = channel.ChannelId,
                Title = channel.ChannelTitle,
                SubCount = (long) (channel.Latest.Stats.SubCount ?? 0),
                ViewCount = (long) (channel.Latest.Stats.ViewCount ?? 0),
                LR = c.LR,
                Type = c.Type,
                Thumbnail = channel.Latest.Thumbnails.Medium.Url,
                UpdatedAt = channel.Latest.Stats.Updated.ToString("O")
            };
        }

        async Task<IReadOnlyCollection<VideoStored>> ChannelVideoStats(ChannelStored c) {
            var channelVideos = await Yt.ChannelVideosCollection.Get(c.ChannelId);
            var channelVideoStats = await channelVideos.Vids.BlockTransform(v => Yt.Videos.Get(v.VideoId));
            return channelVideoStats;
        }

        async Task<IReadOnlyCollection<RecommendRow>> Recommends(VideoRow v) {
            var recommends = await Yt.RecommendedVideosCollection.Get(v.VideoId);

            var flattened = recommends.Recommended
                .SelectMany(r => r.Recommended, (update, to) => new RecommendRow {
                    ChannelId = to.ChannelId,
                    VideoId = to.VideoId,
                    FromChannelId = v.ChannelId,
                    FromVideoId = v.VideoId,
                    Rank = to.Rank,
                    UpdatedAt = recommends.Updated.DateString()
                })
                .Where(r => r.FromChannelId != r.ChannelId)
                .ToList();
            
            return flattened;
        }

        async Task SaveParquet<T>(IEnumerable<T> rows, string name, string dir) where T : new() {
            var storeDir = StringPath.Relative(dir);
            var localFile = LocalResultsDir.Combine(dir).Combine($"{name}.parquet");
            ParquetConvert.Serialize(rows, localFile.FullPath);
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
        public string Type { get; set; }
        public string LR { get; set; }
        public long ViewCount { get; set; }
        public long SubCount { get; set; }
        //public long ChannelVideoViews { get; set; }
        //public string Month { get; set; }
        public string Thumbnail { get; set; }
        public string UpdatedAt { get; set; }
    }

    public class VideoRow {
        public string VideoId { get; set; }
        public string Title { get; set; }
        public string ChannelId { get; set; }
        public long Views { get; set; }
        public string PublishedAt { get; set; }
        public string[] Tags { get; set; }
    }
}