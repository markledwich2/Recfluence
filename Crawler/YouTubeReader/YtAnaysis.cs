using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Parquet;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YouTubeReader {
    public class YtAnaysis {
        public YtAnaysis(YtStore store, Cfg cfg, ILogger log) {
            Yt = store;
            Cfg = cfg;
            Log = log;
        }

        Cfg Cfg { get; }
        ILogger Log { get; }
        YtStore Yt { get; }

        FPath LocalDataDir => "Data".AsPath().InAppData(Setup.AppName);
        FPath LocalResultsDir => "Results".AsPath().InAppData(Setup.AppName);

        /// <summary>
        ///     For the configured time period creates the following
        ///     Channels.parquet - Basic channel info and statistics about recommendations at the granularity of Channel,Date
        ///     Recommends.parquet - Details about video recomendations at the granularity of From,To,Date ??
        /// </summary>
        /// <returns></returns>
        public async Task SaveChannelRelationData() {
            var analysisDir = DateTime.UtcNow.ToString("yyyy-MM-dd");
            await SaveCfg(analysisDir);
            
            var channelCfg = Cfg.LoadConfig();
            var seeds = channelCfg.Seeds;

            var channels = await seeds.BlockTransform(Channel, Cfg.Parallel,
                progressUpdate: p => Log.Information("Getting channel stats {Channels}/{Total}. {Speed}", p.Results.Count, seeds.Count, p.Speed("channels") ));
            await SaveParquet(channels, "Channels", analysisDir);

            var videos = await seeds.BlockTransform(Videos, Cfg.Parallel,
                progressUpdate: p => Log.Information("Getting channel videos {Channels}/{Total}. {Speed}", p.Results.Count, seeds.Count, p.Speed("channels")));
            await SaveParquet(videos.SelectMany(r => r), "Videos", analysisDir);

            var recommendsResult = await seeds.BlockTransform(Recommends, Cfg.Parallel,
                progressUpdate: p => Log.Information("Getting channel video recommendations {Channels}/{Total}. {Speed}", p.Results.Count, seeds.Count, p.Speed("channels")));
            await SaveParquet(recommendsResult.SelectMany(r => r), "Recommends", analysisDir);
        }

        async Task<ICollection<VideoRow>> Videos(SeedChannel c) {
            var channelVids = await Yt.ChannelVideosCollection.Get(c.Id);
            var vids = await channelVids.Videos.BlockTransform(v => Yt.Videos.Get(v.VideoId));
            return vids.Select(v => new VideoRow {
                VideoId = v.VideoId,
                Title = v.VideoTitle,
                ChannelId = c.Id,
                ChannelTitle = c.Title,
                Views = (long)(v.Latest.Stats.Views ?? 0),
            }).ToList();
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
                Thumbnail = channel.Latest.Thumbnails.Medium.Url
            };
        }

        async Task<IReadOnlyCollection<VideoStored>> ChannelVideoStats(ChannelStored c) {
            var channelVideos = await Yt.ChannelVideosCollection.Get(c.ChannelId);
            var channelVideoStats = await channelVideos.Videos.BlockTransform(v => Yt.Videos.Get(v.VideoId));
            return channelVideoStats;
        }

        async Task<IReadOnlyCollection<RecommendRow>> Recommends(SeedChannel c) {
            var channelVids = await Yt.ChannelVideosCollection.Get(c.Id);
            var recommends = await channelVids.Videos.BlockTransform(v => Yt.RecommendedVideosCollection.Get(v.VideoId));

            var flattened = recommends
                .SelectMany(r => r.Recommended, (from, rec) => new {from, rec})
                .SelectMany(r => r.rec.Recommended, (update, to) => new RecommendRow {
                    ChannelId = to.ChannelId,
                    ChannelTitle = to.ChannelTitle,
                    VideoId = to.VideoId,
                    Title = to.VideoTitle,
                    FromChannelId = c.Id,
                    FromChannelTitle = c.Title,
                    FromTitle = update.from.VideoTitle,
                    FromVideoId = update.from.VideoId,
                    Rank = to.Rank,
                    UpdatedAt = update.rec.Updated.ToString("yyyy-MM-dd HH:mm:ss")
                })
                .Where(r => r.FromChannelId != r.ChannelId)
                .ToList();
            
            return flattened;
        }

        async Task SaveParquet<T>(IEnumerable<T> rows, string name, string dir) where T : new() {
            var s3Dir = StringPath.Relative("Results", dir);
            var localFile = LocalResultsDir.Combine(dir).Combine($"{name}.parquet");
            ParquetConvert.Serialize(rows, localFile.FullPath);
            var s3Path = s3Dir.Add(localFile.FileName);
            await Yt.S3.Save(s3Path, localFile);
            Log.Information("Saved to s3 {Path}", s3Path);
        }

        async Task SaveCfg(string dir) {
            var localDir = LocalResultsDir.Combine(dir);
            localDir.EnsureDirectoryExists();
            var s3Dir = StringPath.Relative("Results", dir);
            var localCfgFile = localDir.Combine("Cfg.json");
            Cfg.ToJsonFile(localCfgFile);
            await Yt.S3.Save(s3Dir.Add("Cfg.json"), localCfgFile);
        }
    }

    public class RecommendRow {
        public string VideoId { get; set; }
        public string Title { get; set; }
        public string ChannelId { get; set; }
        public string ChannelTitle { get; set; }
        public string FromVideoId { get; set; }
        public string FromTitle { get; set; }
        public string FromChannelId { get; set; }
        public string FromChannelTitle { get; set; }

        //public long FromVideoViews { get; set; } // needs to be a delta from previous snapshot
        //public long ToVideoViews { get; set; } // delta from previous snap
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
    }

    public class VideoRow {
        public string VideoId { get; set; }
        public string Title { get; set; }
        public string ChannelId { get; set; }
        public string ChannelTitle { get; set; }
        public long Views { get; set; }
    }
}