using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;

namespace YouTubeReader {
    public class YtStore {
        public YtStore(YtReader reader) {
            Yt = reader;
            S3 = new S3Store(reader.Cfg.S3, "YouTube2");

            Videos = new S3Collection<VideoStored>(S3, v => v.VideoId, "Videos", Yt.Cfg.CacheType, CacheDataDir);

            Channels = new S3Collection<ChannelStored>(S3, v => v.ChannelId, "Channels", Yt.Cfg.CacheType, CacheDataDir);

            RecommendedVideosCollection = new S3Collection<RecommendedVideoStored>(S3, v => v.VideoId, "RecommendedVideos", Yt.Cfg.CacheType, CacheDataDir);

            ChannelVideosCollection = new S3Collection<ChannelVideosStored>(S3, c => c.ChannelId, "ChannelVideos", Yt.Cfg.CacheType, CacheDataDir);

            // this is part of an analysis step
            //ChannelCrawls = new S3Collection<ChannelCrawlResult>(S3, r => r.ChannelId, $"CrawlResults/{Yt.Cfg.Month.StringValue}", false);
        }

        FPath CacheDataDir =>  "Data".AsPath().InAppData(Setup.AppName);

        public S3Store S3 { get; }
        YtReader Yt { get; }
        Cfg Cfg => Yt.Cfg;

        public S3Collection<VideoStored> Videos { get; }
        public S3Collection<ChannelStored> Channels { get; }
        public S3Collection<ChannelVideosStored> ChannelVideosCollection { get; }

        public S3Collection<RecommendedVideoStored> RecommendedVideosCollection { get; }
        //public S3Collection<ChannelCrawlResult> ChannelCrawls { get; }

        /// <summary>
        ///     Gets the video with that ID. Caches in d3 (including historical information) with this
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<VideoStored> GetAndUpdateVideo(string id) {
            var v = await Videos.Get(id);
            var needsNewStats = v == null || NeedsUpdate(v.Latest.Stats.Updated);
            if (!needsNewStats) return v;

            var videoData = await Yt.VideoData(id);
            if (v == null)
                v = new VideoStored {Latest = videoData};
            else
                v.SetLatest(videoData);

            await Videos.Set(v);

            return v;
        }

        public async Task<ChannelStored> GetAndUpdateChannel(string id) {
            var c = await Channels.Get(id);
            var needsNewStats = c == null || NeedsUpdate(c.Latest.Stats.Updated);
            if (!needsNewStats) return c;

            var channelData = await Yt.ChannelData(id);
            if (c == null)
                c = new ChannelStored {Latest = channelData};
            else
                c.SetLatest(channelData);

            await Channels.Set(c);

            return c;
        }

        bool NeedsUpdate(DateTime updated) => (Cfg.To ?? DateTime.UtcNow) - updated > 24.Hours();

        public async Task<ChannelVideosStored> GetAndUpdateChannelVideos(ChannelData c) {
            var cv = await ChannelVideosCollection.Get(c.Id);
            var mostRecent = cv?.Videos.OrderByDescending(v => v.PublishedAt).FirstOrDefault();
            var needsUpdate = mostRecent == null || NeedsUpdate(mostRecent.PublishedAt);

            if (!needsUpdate) return cv;

            if (cv == null)
                cv = new ChannelVideosStored {ChannelId = c.Id, ChannelTitle = c.Title};

            var created = await Yt.VideosInChannel(c, mostRecent?.PublishedAt ?? Cfg.From, Cfg.To);
            cv.Videos.AddRange(created);
            await ChannelVideosCollection.Set(cv);

            return cv;
        }

        public async Task<ChannelVideosStored> ChannelVideosStored(ChannelData c) => await ChannelVideosCollection.Get(c.Id);

        public async Task<RecommendedVideoStored> GetAndUpdateRecommendedVideos(VideoItem v) {
            var rv = await RecommendedVideosCollection.Get(v.VideoId);
            var mostRecent = rv?.Recommended.OrderByDescending(r => r.Updated).FirstOrDefault();
            var needsUpdate = mostRecent == null || NeedsUpdate(mostRecent.Updated);

            if (!needsUpdate) return rv;

            if (rv == null)
                rv = new RecommendedVideoStored {VideoId = v.VideoId, VideoTitle = v.VideoTitle, Updated = DateTime.UtcNow};

            var created = await Yt.GetRelatedVideos(v.VideoId);
            rv.Recommended.Add(new RecommendedVideos {Updated = DateTime.UtcNow, Top = Cfg.Related, Recommended = created});
            await RecommendedVideosCollection.Set(rv);
            return rv;
        }
    }

    public class RecommendedVideoStored {
        public string VideoId { get; set; }
        public string VideoTitle { get; set; }
        public ICollection<RecommendedVideos> Recommended { get; set; } = new List<RecommendedVideos>();
        public DateTime Updated { get; set; }
    }

    public class RecommendedVideos {
        public DateTime Updated { get; set; }
        public int Top { get; set; }
        public ICollection<RecommendedVideoListItem> Recommended { get; set; } = new List<RecommendedVideoListItem>();
    }

    public class ChannelVideosStored {
        public string ChannelId { get; set; }
        public string ChannelTitle { get; set; }
        public IKeyedCollection<string, ChannelVideoListItem> Videos { get; set; } = new KeyedCollection<string, ChannelVideoListItem>(v => v.VideoId);
    }


    public class VideoStored {
        public string VideoId => Latest?.VideoId;
        public string VideoTitle => Latest?.VideoTitle;
        public VideoData Latest { get; set; }
        public ICollection<VideoStats> History { get; set; } = new List<VideoStats>();


        public void SetLatest(VideoData v) {
            History.Add(Latest.Stats);
            Latest = v;
        }
    }

    public class ChannelStored {
        public string ChannelId => Latest?.Id;
        public string ChannelTitle => Latest?.Title;

        public ChannelData Latest { get; set; }
        public ICollection<ChannelStats> History { get; set; } = new List<ChannelStats>();

        public void SetLatest(ChannelData c) {
            History.Add(Latest.Stats);
            Latest = c;
        }

        public override string ToString() => $"{ChannelTitle}";
    }


    public class ChannelRecommendations {
        string ChannelId { get; set; }
        string ChannelTitle { get; set; }
        public ICollection<Recommendation> Recomendations { get; set; } = new List<Recommendation>();
    }

    public class Recommendation {
        public Recommendation() { }

        public Recommendation(VideoItem from, RecommendedVideoListItem to) {
            From = from;
            To = to;
        }

        public VideoItem From { get; set; }
        public RecommendedVideoListItem To { get; set; }
        public DateTime Updated { get; set; }
    }
}