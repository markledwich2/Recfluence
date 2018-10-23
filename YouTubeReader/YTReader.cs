using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.Web;
using Google.Apis.Services;
using Google.Apis.YouTube.v3;
using Google.Apis.YouTube.v3.Data;
using HtmlAgilityPack;
using ScrapySharp.Extensions;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;

namespace YouTubeReader {
    public class YTReader {
        public static readonly HashSet<string> ExcludeCategories = new[] {
            UsCategoryEnum.FilmAnimation, UsCategoryEnum.AutoVehicles, UsCategoryEnum.Music,
            UsCategoryEnum.PetsAnimals, UsCategoryEnum.Sports, UsCategoryEnum.ShortMovies,
            UsCategoryEnum.TravelEvents, UsCategoryEnum.Gaming,
            UsCategoryEnum.HowToStyle, UsCategoryEnum.Movies, UsCategoryEnum.Animation, UsCategoryEnum.ActionAdventure,
            UsCategoryEnum.Classics, UsCategoryEnum.Comedy2, UsCategoryEnum.Drama, UsCategoryEnum.Family,
            UsCategoryEnum.Foreign, UsCategoryEnum.Horror, UsCategoryEnum.SciFi, UsCategoryEnum.Thriller,
            UsCategoryEnum.Shorts, UsCategoryEnum.Trailers
        }.Select(i => ((int) i).ToString()).ToHashSet();

        public YTReader(Cfg cfg) {
            Cfg = cfg;
            YTService = new YouTubeService(new BaseClientService.Initializer {
                ApiKey = Cfg.YTApiKey,
                ApplicationName = "YouTubeNetworks"
            });
        }

        Cfg Cfg { get; }
        YouTubeService YTService { get; }


        FPath TrendingDir => "Trending".AsPath().InAppData("YoutubeNetworks");

        public static bool IsExcluded(string catId) {
            return ExcludeCategories.Contains(catId);
        }

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

        VideoData ToVideoData(Video v) {
            var r = new VideoData {
                Id = v.Id,
                Title = v.Snippet.Title,
                Description = v.Snippet.Description,
                ChannelTitle = v.Snippet.ChannelTitle,
                ChannelId = v.Snippet.ChannelId,
                Language = v.Snippet.DefaultLanguage,
                PublishedAt = v.Snippet.PublishedAtRaw,
                CategoryId = v.Snippet.CategoryId
            };

            r.Tags.AddRange(v.Snippet.Tags ?? new string[] { });
            return r;
        }

        VideoData ToVideoData(SearchResult v) {
            var r = new VideoData {
                Id = v.Id.VideoId,
                Title = v.Snippet.Title,
                Description = v.Snippet.Description,
                ChannelTitle = v.Snippet.ChannelTitle,
                ChannelId = v.Snippet.ChannelId,
                PublishedAt = v.Snippet.PublishedAtRaw
            };
            return r;
        }

        public async Task<VideoData> GetVideoData(string id) {
            var s = YTService.Videos.List("snippet,topicDetails");
            s.Id = id;
            s.CreateRequest();
            var response = await s.ExecuteAsync();
            var v = response.Items.FirstOrDefault();
            if (v == null) return null;
            
            var data = ToVideoData(v);

            if(v.Snippet.Tags != null)
                data.Tags.AddRange(v.Snippet.Tags);
            if (v.TopicDetails != null)
                data.Topics.AddRange(v.TopicDetails.RelevantTopicIds);

            return data;
        }

        public async Task<ICollection<RelatedVideoData>> GetRelatedVideos(string id) {
            var s = YTService.Search.List("snippet");
            s.RelatedToVideoId = id;
            s.Type = "video";
            s.MaxResults = Cfg.CacheRelated;


            var response = await s.ExecuteAsync();
            var vids = new List<RelatedVideoData>();

            int rank = 1;
            foreach (var item in response.Items) {
                vids.Add(new RelatedVideoData {
                    Id = item.Id.VideoId,
                    Title = item.Snippet.Title,
                    ChannelTitle =  item.Snippet.ChannelTitle,
                    Rank = rank
                });

                rank++;
            }

            return vids;
        }

        public async Task<ICollection<RelatedVideoData>> ScrapeRelatedVideos(string id) {
            // todo replace with search https://developers.google.com/apis-explorer/#p/youtube/v3/youtube.search.list?part=snippet&maxResults=50&relatedToVideoId=NMYJ7UCHSuo&type=video&_h=28&
            var url = $"https://www.youtube.com/watch?v={id}";

            var web = new HtmlWeb();

            var doc = await web.LoadFromWebAsync(url);
            //var browser = new ScrapingBrowser();
            //var p = browser.NavigateToPage(new Uri(url));
            var thumbs = doc.DocumentNode.CssSelect("#watch-related li");

            var vids = new List<RelatedVideoData>();

            var rank = 1;
            foreach (var n in thumbs) {
                var a = n.CssSelect("a.content-link").FirstOrDefault();
                if (a == null) continue;
                var href = a.GetAttributeValue("href");

                var vId = HttpUtility.ParseQueryString(href.Split("?")[1])["v"];
                var v = new RelatedVideoData {
                    Id = vId,
                    Title = a.GetAttributeValue("title"),
                    ChannelTitle = a.CssSelect("span.attribution").FirstOrDefault()?.InnerText,
                    Rank = rank
                };
                vids.Add(v);
                rank++;
            }

            return vids;
        }

        /// <summary>
        ///     The most popular in that channel. Video's do not include related data.
        /// </summary>
        /// <param name="channelId"></param>
        /// <param name="topNumber"></param>
        /// <param name="publishedAfter"></param>
        /// <returns></returns>
        public async Task<ICollection<VideoData>> TopInChannel(string channelId, int topNumber, DateTime publishedAfter) {
            var s = YTService.Search.List("snippet");
            s.ChannelId = channelId;
            s.PublishedAfter = publishedAfter;
            s.MaxResults = topNumber;
            s.Order = SearchResource.ListRequest.OrderEnum.ViewCount;
            var res = await s.ExecuteAsync();
            var topVideos = res.Items.Select(ToVideoData).ToList();
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
                SubCount = c.Statistics.SubscriberCount
            };
            return data;
        }

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
    }

    public class ChannelData {
        public string Id { get; set; }
        public string Title { get; set; }
        public string Description { get; set; }
        public string Country { get; set; }
        public ulong? ViewCount { get; set; }
        public ulong? SubCount { get; set; }

        //public ICollection<VideoData> Top { get; } = new List<VideoData>();

        public override string ToString() {
            return $"{Title} ({SubCount} subs)";
        }
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

        public ICollection<string> Topics { get; } = new List<string>();

        public ICollection<string> Tags { get; } = new List<string>();
        //public ICollection<RelatedVideoData> Related { get; } = new List<RelatedVideoData>();

        public override string ToString() {
            return $"{ChannelTitle} {Title}";
        }
    }

    public class ScrapedData {
        public ICollection<RelatedVideoData> Related { get; } = new List<RelatedVideoData>();
    }

    public class RelatedVideoData {
        public string Id { get; set; }
        public string Title { get; set; }
        public string ChannelTitle { get; set; }
        public int Rank { get; set; }

        public override string ToString() {
            return $"{Rank}. {ChannelTitle}: {Title}";
        }
    }
}