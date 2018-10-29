using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using LiteDB;

namespace YouTubeReader
{
    public static class YTStore
    {
        public static LiteCollection<ChannelData> Channels(this LiteDatabase db) {
            var r = db.GetCollection<ChannelData>("Channels");
            return r;
        }

        public static LiteCollection<VideoAndRecommended> Videos(this LiteDatabase db)
        {
            var r = db.GetCollection<VideoAndRecommended>("Videos");
            return r;
        }
    }

    public class VideoAndRecommended {
        public string Id {
            get => Video.Id;
        }

        public VideoData Video { get; set; }
        public ICollection<RecommendedVideoData> Recommended { get; set; }
    }
}
