using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Newtonsoft.Json;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;

namespace YouTubeReader {
    public class YtStore {
        public YtStore(YtReader reader, ISimpleFileStore store) {
            Yt = reader;
            Store = store;

            Videos = new FileCollection<VideoStored>(Store, v => v.VideoId, "Videos", Yt.Cfg.CacheType, CacheDataDir);
            Channels = new FileCollection<ChannelStored>(Store, v => v.ChannelId, "Channels", Yt.Cfg.CacheType, CacheDataDir);
            RecommendedVideosCollection = new FileCollection<RecommendedVideoStored>(Store, v => v.VideoId, "RecommendedVideos", Yt.Cfg.CacheType, CacheDataDir);
            ChannelVideosCollection = new FileCollection<ChannelVideosStored>(Store, c => c.ChannelId, "ChannelVideos", Yt.Cfg.CacheType, CacheDataDir);
        }

        FPath CacheDataDir => "Data".AsPath().InAppData(Setup.AppName);

        public ISimpleFileStore Store { get; }
        YtReader Yt { get; }
        AppCfg Cfg => Yt.Cfg;

        public FileCollection<VideoStored> Videos { get; }
        public FileCollection<ChannelStored> Channels { get; }
        public FileCollection<ChannelVideosStored> ChannelVideosCollection { get; }

        public FileCollection<RecommendedVideoStored> RecommendedVideosCollection { get; }

        /// <summary>
        ///     Gets the video with that ID. Caches in S3 (including historical information) with this
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<VideoStored> GetAndUpdateVideo(string id) {
            var v = await Videos.Get(id);
            if (v != null && v.Latest.Updated == default(DateTime))
                v.Latest.Updated = v.Latest.Stats.Updated;

            var needsNewStats = v == null || Expired(v.Latest.Updated, VideoRefreshAge(v.Latest));
            if (!needsNewStats) return v;

            var videoData = await Yt.VideoData(id);
            if (videoData != null) {
                if (v == null)
                    v = new VideoStored {Latest = videoData};
                else
                    v.SetLatest(videoData);
                v.Latest.Updated = DateTime.UtcNow;
            }
            if(v != null)
                await Videos.Set(v);

            return v;
        }

        TimeSpan VideoRefreshAge(ChannelVideoListItem v) {
            if (Expired(v.PublishedAt, Cfg.VideoDead)) return TimeSpan.MaxValue;
            return Expired(v.PublishedAt, Cfg.VideoOld) ? Cfg.RefreshOldVideos : Cfg.RefreshYoungVideos;
        }

        public async Task<ChannelStored> GetAndUpdateChannel(string id) {
            var c = await Channels.Get(id);

            var needsNewStats = c == null || Expired(c.Latest.Stats.Updated, Cfg.RefreshChannel);
            if (!needsNewStats) return c;

            var channelData = await Yt.ChannelData(id);
            if (c == null)
                c = new ChannelStored {Latest = channelData};
            else
                c.SetLatest(channelData);

            await Channels.Set(c);

            return c;
        }

        bool Expired(DateTime updated, TimeSpan refreshAge) => (Cfg.To ?? DateTime.UtcNow) - updated > refreshAge;

        public async Task<ChannelVideosStored> GetAndUpdateChannelVideos(ChannelData c) {
            var cv = await ChannelVideosCollection.Get(c.Id);

            // fix updated if missing. Remove once all records have been updated
            var mostRecent = cv?.Vids.OrderByDescending(v => v.Updated).FirstOrDefault();
            if (cv != null && mostRecent != null && cv.Updated == default(DateTime))
                cv.Updated = mostRecent.Updated;

            var needsUpdate = cv == null || Expired(cv.Updated, Cfg.RefreshChannelVideos)
                                         || cv.From != Cfg.From; // when from is chaged, update all videos
            if (!needsUpdate) return cv;

            if (cv == null)
                cv = new ChannelVideosStored {ChannelId = c.Id, ChannelTitle = c.Title, Updated = DateTime.UtcNow};
            else
                cv.Updated = DateTime.UtcNow;

            var queryForm = cv.From != Cfg.From ? Cfg.From : mostRecent?.PublishedAt ?? Cfg.From;
            var created = await Yt.VideosInChannel(c, queryForm, Cfg.To);

            cv.Vids.AddRange(created);
            cv.From = Cfg.From;
            await ChannelVideosCollection.Set(cv);

            return cv;
        }

        public async Task<ChannelVideosStored> ChannelVideosStored(ChannelData c) => await ChannelVideosCollection.Get(c.Id);

        public async Task<RecommendedVideoStored> GetAndUpdateRecommendedVideos(ChannelVideoListItem v) {
            var rv = await RecommendedVideosCollection.Get(v.VideoId);

            if (Expired(v.PublishedAt, Cfg.VideoDead))
                return rv;

            //var mostRecent = rv?.Recommended.OrderByDescending(r => r.Updated).FirstOrDefault();
            var needsUpdate = rv == null || Expired(rv.Updated, Cfg.RefreshRelatedVideos);

            if (!needsUpdate) return rv;

            if (rv == null)
                rv = new RecommendedVideoStored {VideoId = v.VideoId, VideoTitle = v.VideoTitle, Updated = DateTime.UtcNow};

            var created = await Yt.GetRelatedVideos(v.VideoId);
            rv.Recommended.Add(new RecommendedVideos {Updated = DateTime.UtcNow, Top = Cfg.Related, Recommended = created});
            rv.Updated = DateTime.UtcNow;
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
        public DateTime Updated { get; set; }
        public DateTime From { get; set; }

        [JsonIgnore]
        public IKeyedCollection<string, ChannelVideoListItem> Vids { get; set; } = new KeyedCollection<string, ChannelVideoListItem>(v => v.VideoId);

        [JsonProperty("videos")]
        public ChannelVideoListItem[] SerializedVideos {
            get => Vids.OrderBy(v => v.PublishedAt).ToArray();
            set => Vids.Init(value);
        }
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