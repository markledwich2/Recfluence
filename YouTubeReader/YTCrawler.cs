using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using MongoDB.Driver;
using Serilog.Core;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YouTubeReader {
    public class YTCrawler {
        public YTCrawler(MongoClient mongoClient, YTReader yt, Cfg cfg, Logger log) {
            MongoClient = mongoClient;
            Yt = yt;
            Cfg = cfg;
            Log = log;
        }

        MongoClient MongoClient { get; }
        YTReader Yt { get; }
        Cfg Cfg { get; }
        Logger Log { get; }

        FPath DataDir => "Data".AsPath().InAppData(Setup.AppName);
        FPath CacheDir => "Cache".AsPath().InAppData(Setup.AppName);
        KeyedCollection<string, ChannelData> ChannelCache { get; } = new KeyedCollection<string, ChannelData>(c => c.Id, theadSafe:true);
        KeyedCollection<string, VideoAndRelatedData> VidCache { get; } = new KeyedCollection<string, VideoAndRelatedData>(v => v.Video.Id, theadSafe:true);
        FPath ChannelCacheFile => CacheDir.Combine("Channels.json");
        FPath VidCacheFile => CacheDir.Combine("Videos.json");

        async Task<VideoAndRelatedData> GetVideo(string id) {
            async Task<VideoAndRelatedData> Create() {
                return new VideoAndRelatedData {
                    Video = await Yt.GetVideoData(id),
                    Related = await Yt.GetRelatedVideos(id)
                };
            }
                 
            return await VidCache.GetOrAdd(id, () => Create());
        }

        async Task<ChannelData> GetChannel(string id) => await ChannelCache.GetOrAdd(id, () => Yt.ChannelData(id));

        public async Task<CrawlResult> Crawl() {
            var res = new CrawlResult();

            Log.Information("Started crawl with {@Config}", Cfg);

            LoadCache();

            var seedData = Cfg.SeedPath.ReadFromCsv<SeedChannel>();
            if (Cfg.LimitSeedChannels.HasValue)
                seedData = seedData.Take(Cfg.LimitSeedChannels.Value).ToList();

            var channels = await seedData.BlockTransform(c => GetChannel(c.Id), Cfg.Parallel);
            var crawlResults = await channels.BlockTransform(c => CrawlFromChannel(c), Cfg.Parallel);

            res.Channels.AddRange(crawlResults.SelectMany(r => r.Channels));
            res.Recommends.AddRange(crawlResults.SelectMany(r => r.Recommends));

            SaveCache();
            return res;
        }

        async Task<CrawlResult> CrawlFromChannel(ChannelData channel) {
            var log = Log.ForContext("SeedChannel", channel.Title);
            var r = new CrawlResult {Channels = {channel}};
            var top = await Yt.TopInChannel(channel.Id, Cfg.TopInChannel, Cfg.SeedFromDate);
            var toCrawl = (await top.BlockTransform(v => GetVideo(v.Id), Cfg.Parallel)).NotNull().ToList();

            var estimatedVisit = 0;
            var toCrawlEstimate = toCrawl.Count;
            for (var i = 1; i <= Cfg.StepsFromSeed; i++) {
                estimatedVisit += toCrawlEstimate * Cfg.Related;
                toCrawlEstimate = toCrawlEstimate * Cfg.Related;
            }

            int visitCount = 0, lastVisitCount = 0;
            var totalTimer = Stopwatch.StartNew();
            var lastElapsed = TimeSpan.Zero;

            for (var i = 1; i <= Cfg.StepsFromSeed; i++) {
                var crawling = toCrawl.ToList();
                toCrawl.Clear();

                foreach (var fromV in crawling)
                foreach (var related in fromV.Related.Take(Cfg.Related)) {

                    var v = await GetVideo(related.Id);
                    if (v == null) {
                        log.Error("Video unavailable '{Channel}': '{Video}' {VideoId}", related.ChannelTitle, related.Title, related.Id);
                        continue;
                    }

                    var c = await GetChannel(v.Video.ChannelId);
                    if (!r.Channels.ContainsKey(c.Id))
                        log.Information("New channel '{Channel}'", c.ToString());
                    r.Channels.Add(c);
                    toCrawl.Add(v);
                    var rec = new Recommended {
                        VideoId = v.Video.Id,
                        Title = v.Video.Title,
                        ChannelId = v.Video.ChannelId,
                        ChannelTitle = v.Video.ChannelTitle,
                        FromVideoId = fromV.Video.Id,
                        FromTitle = fromV.Video.Title,
                        FromChannelId = fromV.Video.ChannelId,
                        FromChannelTitle = fromV.Video.ChannelTitle,
                        Rank = related.Rank,
                        DistanceFromSeed = i
                    };

                    if (!r.Recommends.Contains(rec)) {
                        r.Recommends.Add(rec);
                        log.Verbose("Recommended '{Channel}: {Title}' from '{FromChannel}: {FromTitle}'",
                            rec.ChannelTitle, rec.Title, rec.FromChannelTitle, rec.FromTitle);
                    }

                    visitCount++;

                    if (totalTimer.Elapsed - lastElapsed > 10.Seconds()) {
                        var elapsed = totalTimer.Elapsed;
                        var periodElapsed = elapsed - lastElapsed;
                        var periodVisits = visitCount - lastVisitCount;

                        log.Information("'{SeedChannel}' {Visited}/{Estimated} visited/total (estimate). {VisitSpeed}", 
                            channel.Title, visitCount, estimatedVisit, periodVisits.Speed("visits", periodElapsed).Humanize());
                        lastElapsed = elapsed;
                        lastVisitCount = visitCount;
                    }
                }
            }

            return r;
        }

        public void SaveResult(CrawlResult result) {
            var tsDir = DataDir.Combine(DateTime.UtcNow.FileSafeTimestamp());
            tsDir.EnsureDirectoryExists();
            Cfg.ToJsonFile(tsDir.Combine("cfg.json"));
            result.Recommends.WriteToCsv(tsDir.Combine("recommends.csv"));
            result.Channels.ToJsonFile(tsDir.Combine("channels.json"));
            Log.Information("Saved results. {Reccomends} recomends, {Channels} channels", 
                result.Recommends.Count(), result.Channels.Count());
        }

        int vidCacheLoaded, channelCacheLoaded = 0;
        void LoadCache() {
            if (VidCacheFile.Exists) {
                VidCache.Init(VidCacheFile.ToObject<List<VideoAndRelatedData>>());
                Log.Verbose("Loaded {Videos} vids from Cache", VidCache.Count);
                vidCacheLoaded = VidCache.Count();
            }

            if (ChannelCacheFile.Exists) {
                ChannelCache.Init(ChannelCacheFile.ToObject<List<ChannelData>>());
                Log.Verbose("Loaded {Channels} channels from Cache", VidCache.Count);
            }
        }

        void SaveCache() {
            VidCacheFile.EnsureDirectoryExists();
            VidCache.ToJsonFile(VidCacheFile);
            ChannelCache.ToJsonFile(ChannelCacheFile);
            Log.Verbose("Saved {Videos} vids, {Channels} channels to cache. {NewVideos} new videos, {NewChannels} loaded this crawl",
                VidCache.Count, ChannelCache.Count, vidCacheLoaded, channelCacheLoaded);
        }

        class CrawlCache {

        }

        public class CrawlResult {
            public IKeyedCollection<string, ChannelData> Channels { get; } = new KeyedCollection<string, ChannelData>(c => c.Id);
            public IKeyedCollection<string, Recommended> Recommends { get; } = new KeyedCollection<string, Recommended>(r => $"{r.FromVideoId}.{r.VideoId}");
        }
    }

    

    /// <summary>
    ///     Need to record visits separately to record the different way to get to the same video and re-use a video cache
    /// </summary>
    public class Recommended {
        public string VideoId { get; set; }
        public string Title { get; set; }
        public string ChannelId { get; set; }
        public string ChannelTitle { get; set; }
        public string FromVideoId { get; set; }
        public string FromTitle { get; set; }
        public string FromChannelId { get; set; }
        public string FromChannelTitle { get; set; }
        public int Rank { get; set; }
        public int DistanceFromSeed { get; set; }


        public override string ToString() {
            return $"{FromChannelTitle}: {FromTitle} > {Rank}. {ChannelTitle}: {Title}";
        }
    }

    public class SeedChannel {
        public string Title { get; set; }
        public string Id { get; set; }
        public string Tags { get; set; }
    }

    public class VideoAndRelatedData {
        public VideoData Video { get; set; }
        public ICollection<RelatedVideoData> Related { get; set; }
    }
}