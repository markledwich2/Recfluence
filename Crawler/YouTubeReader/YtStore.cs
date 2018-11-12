using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace YouTubeReader {
    public class YtStore {
        public YtStore(YtReader reader) {
            Yt = reader;
            S3 = new S3Store(reader.Cfg.S3, "YouTube");

            Videos = new S3Collection<VideoData>(
                S3, v => v.Id, id => Yt.GetVideoData(id), "Videos");
            Channels = new S3Collection<ChannelData>(
                S3, v => v.Id, id => Yt.ChannelData(id), "Channels");

            ChannelVideosCollection = new S3Collection<S3ChannelVideos>(
                S3, c => c.ChannelId, CreateVideosInChannel, $"ChannelVideos/{S3ChannelVideos.GetFromToKey(Yt.Cfg.From, Yt.Cfg.To)}");
            RecommendedVideosCollection = new S3Collection<S3RecommendedVideos>(
                S3, v => v.VideoId, CreateRecommendedVideos,
                $"RecommendedVideos/{Yt.Cfg.RecommendationCacheDayOverride ?? S3RecommendedVideos.GeUpdatedKey(Yt.Start)}");


            ChannelCrawls = new S3Collection<YtCrawler.ChannelCrawlResult>(S3, r => r.ChannelId, id => null,
                $"CrawlResults/{S3ChannelVideos.GetFromToKey(Yt.Cfg.From, Yt.Cfg.To)}", false);
        }

        public S3Store S3 { get; }
        YtReader Yt { get; }

        public S3Collection<VideoData> Videos { get; }
        public S3Collection<ChannelData> Channels { get; }
        public S3Collection<S3ChannelVideos> ChannelVideosCollection { get; }
        public S3Collection<S3RecommendedVideos> RecommendedVideosCollection { get; }
        public S3Collection<YtCrawler.ChannelCrawlResult> ChannelCrawls { get; }

        public Task<VideoData> Video(string id) =>
            Videos.GetOrCreate(id);

        public Task<ChannelData> Channel(string id) =>
            Channels.GetOrCreate(id);

        public async Task<S3ChannelVideos> ChannelVideos(ChannelData c) =>
            await ChannelVideosCollection.GetOrCreate(c.Id);

        public async Task<S3ChannelVideos> ChannelVideosStored(ChannelData c) => await ChannelVideosCollection.Get(c.Id);

        async Task<S3ChannelVideos> CreateVideosInChannel(string id) {
            var c = await Channel(id);
            var list = await Yt.VideosInChannel(c, Yt.Cfg.From, Yt.Cfg.To);
            return new S3ChannelVideos {
                ChannelId = c.Id,
                ChannelTitle = c.Title,
                From = Yt.Cfg.From,
                To = Yt.Cfg.To,
                Videos = list,
                Updated = DateTime.UtcNow
            };
        }

        async Task<S3RecommendedVideos> CreateRecommendedVideos(string id) {
            var v = await Yt.GetRelatedVideos(id);
            if (v == null) return null;
            return new S3RecommendedVideos {
                VideoId = id,
                Updated = DateTime.UtcNow,
                Recommended = v,
                Top = Yt.Cfg.Related
            };
        }

        public Task<S3RecommendedVideos> RecommendedVideos(string id) => RecommendedVideosCollection.GetOrCreate(id);
    }

    public class S3RecommendedVideos {
        public static string GeUpdatedKey(DateTime date) => S3ChannelVideos.Day(date);

        public string VideoId { get; set; }
        public DateTime Updated { get; set; }
        public int Top { get; set; }
        public ICollection<RecommendedVideoListItem> Recommended { get; set; } = new List<RecommendedVideoListItem>();
    }

    public class S3ChannelVideos {
        public static string Day(DateTime date) => date.ToString("yyyy-MM-dd");
        public static string GetFromToKey(DateTime from, DateTime to) => $"{Day(from)}_{Day(to)}";
        internal static object GetFromToKey() => throw new NotImplementedException();

        public string ChannelId { get; set; }
        public string ChannelTitle { get; set; }
        public DateTime From { get; set; }
        public DateTime To { get; set; }
        public DateTime Updated { get; set; }
        public ICollection<ChannelVideoListItem> Videos { get; set; } = new List<ChannelVideoListItem>();
    }
}