using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using LiteDB;
using SysExtensions.Collections;
using SysExtensions.Threading;

namespace YouTubeReader {

    public static class TyDbExtentions {
        public static LiteCollection<ChannelData> Channels(this LiteDatabase db) => db.GetCollection<ChannelData>("Channels");
        public static LiteCollection<VideoData> Videos(this LiteDatabase db) => db.GetCollection<VideoData>("Videos");
        public static LiteCollection<ChannelVideos> ChannelVideos(this LiteDatabase db) => db.GetCollection<ChannelVideos>("ChannelVideos");
        public static LiteCollection<VideoRecommended> VideoRecommended(this LiteDatabase db) => db.GetCollection<VideoRecommended>("VideoRecommended");
    }

    public class YtCacheDb {
        public YtCacheDb(LiteDatabase db, YtReader yt) {
            Db = db;
            Yt = yt;
            ChannelsCollection = db.Channels();
            ChannelVideosCollection = db.ChannelVideos();
            ChannelVideosCollection.EnsureIndex(c => c.ChannelId);
            VideosCollection = db.Videos();
            VideoRecommendedCollection = db.VideoRecommended();
            VideoRecommendedCollection.EnsureIndex(v => v.VideoId);
        }

        LiteDatabase Db { get; }
        YtReader Yt { get; }

        public LiteCollection<ChannelData> ChannelsCollection { get; }
        public LiteCollection<VideoData> VideosCollection { get; }
        public LiteCollection<VideoRecommended> VideoRecommendedCollection { get; }
        public LiteCollection<ChannelVideos> ChannelVideosCollection { get; }

        IKeyedCollection<string, ChannelData> ChannelCache { get; } = new KeyedCollection<string, ChannelData>(c => c.Id, theadSafe: true);
        IKeyedCollection<string, VideoData> VideoCache { get; } = new KeyedCollection<string, VideoData>(v => v.Id, theadSafe: true);


        readonly SemaphoreSlim _videoLock = new SemaphoreSlim(1, 1);

        public async Task<VideoData> Video(string id) {
            if (VideoCache.ContainsKey(id)) return VideoCache[id];
            using (await _videoLock.LockAsync()) {
                if (VideoCache.ContainsKey(id)) return VideoCache[id];
                var v = VideosCollection.FindById(id);
                if (v == null) {
                    v = await Yt.GetVideoData(id);
                    VideosCollection.Upsert(v);
                }
                VideoCache.Add(v);
                return v;
            }
        }

        readonly SemaphoreSlim _channelLock = new SemaphoreSlim(1, 1);

        public async Task<ChannelData> Channel(string id) {
            if (ChannelCache.ContainsKey(id)) return ChannelCache[id];
            using (await _channelLock.LockAsync()) {
                if (ChannelCache.ContainsKey(id)) return ChannelCache[id];
                var c = ChannelsCollection.FindById(id);
                if (c == null) {
                    c = await Yt.ChannelData(id);
                    ChannelsCollection.Upsert(c);
                }
                ChannelCache.Add(c);
                return c;
            }
        }

        readonly SemaphoreSlim _videoRecommendedLock = new SemaphoreSlim(1, 1);

        public async Task<VideoRecommended> VideoRecommended(string id, int top, DateTime? validFrom) {
            VideoRecommended Query() => VideoRecommendedCollection.Find(r => r.VideoId == id && r.Top == top && (validFrom == null || r.Updated > validFrom))
                .OrderByDescending(r => r.Updated).FirstOrDefault();

            using (await _videoRecommendedLock.LockAsync()) {
                // only allow one thread at a time to prevent double records
                var res = Query();
                if (res != null) return res;

                res = new VideoRecommended {
                    VideoId = id,
                    Updated = DateTime.UtcNow,
                    Top = top
                };
                res.Recommended.AddRange(await Yt.GetRelatedVideos(id));
                VideoRecommendedCollection.Insert(res);
                return res;
            }
        }

        readonly SemaphoreSlim _channelVideosLock = new SemaphoreSlim(1, 1);

        public async Task<ChannelVideos> ChannelVideos(ChannelData c, DateTime from, DateTime to) {
            ChannelVideos Query() => ChannelVideosCollection.Find(v => v.ChannelId == c.Id && v.From == from && v.To == to)
                .OrderByDescending(r => r.Updated).FirstOrDefault();

            using (await _channelVideosLock.LockAsync()) {
                // only allow one thread at a time to prevent double records
                var res = Query();
                if (res != null) return res;

                res = new ChannelVideos {
                    ChannelId = c.Id,
                    Updated = DateTime.UtcNow,
                    From = from,
                    To = to,
                    ChannelTitle = c.Title
                };
                res.Videos.AddRange(await Yt.VideosInChannel(c.Id, from, to));
                ChannelVideosCollection.Insert(res);
                return res;
            }
        }
    }

    public class VideoRecommended {
        public int Id { get; set; }

        public string VideoId { get; set; }
        public DateTime Updated { get; set; } // use this to invalidate cache
        public int Top { get; set; }
        public ICollection<RecommendedVideoListItem> Recommended { get; set; } = new List<RecommendedVideoListItem>();
    }

    public class ChannelVideos {
        public int Id { get; set; }

        public string ChannelId { get; set; }
        public string ChannelTitle { get; set; }
        public DateTime From { get; set; }
        public DateTime To { get; set; }
        public DateTime Updated { get; set; }
        public ICollection<ChannelVideoListItem> Videos { get; set; } = new List<ChannelVideoListItem>();
    }
}