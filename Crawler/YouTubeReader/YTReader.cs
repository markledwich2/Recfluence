using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Apis.Services;
using Google.Apis.YouTube.v3;
using Google.Apis.YouTube.v3.Data;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;

namespace YouTubeReader {
    public class YtReader {
        public YtReader(Cfg cfg) {
            Cfg = cfg;
            YTService = new YouTubeService(new BaseClientService.Initializer {
                ApiKey = Cfg.YTApiKey,
                ApplicationName = "YouTubeNetworks"
            });
        }

        Cfg Cfg { get; }
        YouTubeService YTService { get; }

        #region Trending

        public async Task SaveTrendingCsv() {
            var trending = await Trending(100);
            trending.AddRange(await Trending(100, UsCategoryEnum.Entertainment));

            var videos = trending
                .Select(i =>
                    new {
                        Include = 0,
                        i.Id,
                        i.ChannelTitle,
                        i.Title,
                        CategoryName = ((UsCategoryEnum) int.Parse(i.CategoryId)).EnumString(),
                        i.ChannelId
                    });

            TrendingDir.CreateDirectories();
            videos.WriteToCsv(TrendingDir.Combine(DateTime.UtcNow.FileSafeTimestamp() + " Trending.csv"));
        }

        async Task<ICollection<VideoData>> Trending(int max, UsCategoryEnum? category = null) {
            var s = YTService.Videos.List("snippet");
            s.Chart = VideosResource.ListRequest.ChartEnum.MostPopular;
            s.RegionCode = "us";
            s.MaxResults = 50;
            if (category != null)
                s.VideoCategoryId = ((int) category).ToString();

            var videos = new List<VideoData>();
            while (videos.Count < max) {
                var res = await s.ExecuteAsync();
                var trending = res.Items.Where(i => !IsExcluded(i.Snippet.CategoryId));
                videos.AddRange(trending.Take(max - videos.Count).Select(ToVideoData));
                if (res.NextPageToken == null)
                    break;
                s.PageToken = res.NextPageToken;
            }

            return videos;
        }


        FPath TrendingDir => "Trending".AsPath().InAppData("YoutubeNetworks");

        enum UsCategoryEnum {
            FilmAnimation = 1,
            AutoVehicles = 2,
            Music = 10,
            PetsAnimals = 15,
            Sports = 17,
            ShortMovies = 18,
            TravelEvents = 19,
            Gaming = 20,
            VideoBlogging = 21,
            PplBlogs = 22,
            Comedy = 23,
            Entertainment = 24,
            NewsPolitics = 25,
            HowToStyle = 26,
            Education = 27,
            ScienceTech = 28,
            NonprofitsActivism = 29,
            Movies = 20,
            Animation = 31,
            ActionAdventure = 23,
            Classics = 33,
            Comedy2 = 34,
            Doco = 35,
            Drama = 36,
            Family = 37,
            Foreign = 38,
            Horror = 39,
            SciFi = 40,
            Thriller = 41,
            Shorts = 42,
            Trailers = 44
        }

        public static readonly HashSet<string> ExcludeCategories = new[] {
            UsCategoryEnum.FilmAnimation, UsCategoryEnum.AutoVehicles, UsCategoryEnum.Music,
            UsCategoryEnum.PetsAnimals, UsCategoryEnum.Sports, UsCategoryEnum.ShortMovies,
            UsCategoryEnum.TravelEvents, UsCategoryEnum.Gaming,
            UsCategoryEnum.HowToStyle, UsCategoryEnum.Movies, UsCategoryEnum.Animation, UsCategoryEnum.ActionAdventure,
            UsCategoryEnum.Classics, UsCategoryEnum.Comedy2, UsCategoryEnum.Drama, UsCategoryEnum.Family,
            UsCategoryEnum.Foreign, UsCategoryEnum.Horror, UsCategoryEnum.SciFi, UsCategoryEnum.Thriller,
            UsCategoryEnum.Shorts, UsCategoryEnum.Trailers
        }.Select(i => ((int) i).ToString()).ToHashSet();

        public static bool IsExcluded(string catId) => ExcludeCategories.Contains(catId);

        #endregion

        #region Videos

        VideoData ToVideoData(Video v) {
            var r = new VideoData {
                Id = v.Id,
                Title = v.Snippet.Title,
                Description = v.Snippet.Description,
                ChannelTitle = v.Snippet.ChannelTitle,
                ChannelId = v.Snippet.ChannelId,
                Language = v.Snippet.DefaultLanguage,
                PublishedAt = v.Snippet.PublishedAtRaw,
                CategoryId = v.Snippet.CategoryId,
                Views = v.Statistics?.ViewCount,
                Likes = v.Statistics?.LikeCount,
                Dislikes = v.Statistics?.DislikeCount
            };
            if (v.Snippet.Tags != null)
                r.Tags.AddRange(v.Snippet.Tags);
            if (v.TopicDetails?.RelevantTopicIds != null)
                r.Topics.AddRange(v.TopicDetails.RelevantTopicIds);

            return r;
        }

        public async Task<VideoData> GetVideoData(string id) {
            var s = YTService.Videos.List("snippet,topicDetails,statistics");
            s.Id = id;
            s.CreateRequest();
            var response = await s.ExecuteAsync();
            var v = response.Items.FirstOrDefault();
            if (v == null) return null;

            var data = ToVideoData(v);
            data.Updated = DateTime.UtcNow;

            return data;
        }

        public async Task<ICollection<RecommendedVideoListItem>> GetRelatedVideos(string id) {
            var s = YTService.Search.List("snippet");
            s.RelatedToVideoId = id;
            s.Type = "video";
            s.MaxResults = Cfg.CacheRelated;

            var response = await s.ExecuteAsync();
            var vids = new List<RecommendedVideoListItem>();

            var rank = 1;
            foreach (var item in response.Items) {
                vids.Add(new RecommendedVideoListItem {
                    Id = item.Id.VideoId,
                    Title = item.Snippet.Title,
                    ChannelId = item.Snippet.ChannelId,
                    ChannelTitle = item.Snippet.ChannelTitle,
                    Rank = rank
                });

                rank++;
            }

            return vids;
        }

        #endregion

        #region Channels

        /// <summary>
        ///     The most popular in that channel. Video's do not include related data.
        /// </summary>
        public async Task<ICollection<ChannelVideoListItem>> VideosInChannel(string channelId, DateTime publishedAfter, DateTime? publishBefore) {
            var s = YTService.Search.List("snippet");
            s.ChannelId = channelId;
            s.PublishedAfter = publishedAfter;
            s.PublishedBefore = publishBefore;
            s.MaxResults = 50;
            s.Order = SearchResource.ListRequest.OrderEnum.ViewCount;
            s.Type = "video";

            var topVideos = new List<ChannelVideoListItem>();
            while (true) {
                var res = await s.ExecuteAsync();
                topVideos.AddRange(res.Items.Select(v => new ChannelVideoListItem {
                    Id = v.Id.VideoId,
                    Title = v.Snippet.Title,
                    PublishedAt = v.Snippet.PublishedAt
                }));
                if (res.NextPageToken == null)
                    break;
                s.PageToken = res.NextPageToken;
            }

            return topVideos;
        }

        public async Task<ChannelData> ChannelData(string id) {
            var s = YTService.Channels.List("snippet,statistics");
            s.Id = id;
            var r = await s.ExecuteAsync();
            var c = r.Items.FirstOrDefault();
            if (c == null) return new ChannelData {Id = id, Title = "N/A"};

            var data = new ChannelData {
                Id = id,
                Title = c.Snippet.Title,
                Description = c.Snippet.Description,
                Country = c.Snippet.Country,
                ViewCount = c.Statistics.ViewCount,
                SubCount = c.Statistics.SubscriberCount,
                Updated = DateTime.UtcNow
            };

            return data;
        }

        #endregion
    }

    public class ChannelData {
        public string Id { get; set; }
        public string Title { get; set; }
        public string Country { get; set; }
        public ulong? ViewCount { get; set; }
        public ulong? SubCount { get; set; }
        public DateTime Updated { get; set; }
        public string Description { get; set; }

        public override string ToString() => Title;
    }

    public class VideoData {
        public string Id { get; set; }
        public string Title { get; set; }
        public string Description { get; set; }
        public string ChannelTitle { get; set; }
        public string ChannelId { get; set; }
        public string Language { get; set; }
        public string PublishedAt { get; set; }
        public string CategoryId { get; set; }
        public ulong? Views { get; set; }
        public ulong? Likes { get; set; }
        public ulong? Dislikes { get; set; }
        public DateTime Updated { get; set; }

        public ICollection<string> Topics { get; } = new List<string>();

        public ICollection<string> Tags { get; } = new List<string>();

        public override string ToString() => $"{ChannelTitle} {Title}";
    }

    public class RecommendedVideoListItem {
        public string Id { get; set; }
        public string Title { get; set; }
        public string ChannelId { get; set; }
        public string ChannelTitle { get; set; }
        public int Rank { get; set; }

        public override string ToString() => $"{Rank}. {ChannelTitle}: {Title}";
    }

    public class ChannelVideoListItem {
        public string Id { get; set; }
        public string Title { get; set; }
        public DateTime? PublishedAt { get; set; }
        public override string ToString() => Title;
    }
}