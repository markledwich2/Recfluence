using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;

namespace YouTubeReader
{
    public static class YTStore
    {
        public static async Task<IMongoCollection<ChannelData>> Channels(this IMongoDatabase db) {
            var r = db.GetCollection<ChannelData>("Channels");
            //var keyModel = new CreateIndexModel<ChannelData>(Builders<ChannelData>.IndexKeys.Hashed(c => c.Id));
            //await r.Indexes.CreateOneAsync(keyModel);
            return r;
        }

        public static async Task<IMongoCollection<VideoAndRecommended>> Videos(this IMongoDatabase db)
        {
            var r = db.GetCollection<VideoAndRecommended>("Videos");
           // var keyModel = new CreateIndexModel<VideoAndRelatedData>(Builders<VideoAndRelatedData>.IndexKeys.Hashed(c => c.Video.Id));
            //await r.Indexes.CreateOneAsync(keyModel);
            return r;
        }

        public static async Task<T> FirstOrDefaultAsync<T>(this IMongoCollection<T> collection, Expression<Func<T, bool>> filter) {
            var r = await collection.FindAsync(filter);
            return await r.FirstOrDefaultAsync();
        }
    }

    public class VideoAndRecommended {
        [BsonId]
        public string Id {
            get => Video.Id;
        }

        public VideoData Video { get; set; }
        public ICollection<RecommendedVideoData> Recommended { get; set; }
    }
}
