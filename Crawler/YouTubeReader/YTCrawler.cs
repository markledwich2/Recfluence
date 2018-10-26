using System;
using System.Collections;
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
        public YTCrawler(IMongoDatabase db, YTReader yt, Cfg cfg, Logger log) {
            Db = db;
            Yt = yt;
            Cfg = cfg;
            Log = log;

            Channels = new AsyncLazy<IMongoCollection<ChannelData>>(async () => await Db.Channels());
            Videos = new AsyncLazy<IMongoCollection<VideoAndRecommended>>(async () => await Db.Videos());
        }

        IMongoDatabase Db { get; }
        YTReader Yt { get; }
        Cfg Cfg { get; }
        Logger Log { get; }

        AsyncLazy<IMongoCollection<ChannelData>> Channels { get; }
        AsyncLazy<IMongoCollection<VideoAndRecommended>> Videos { get; }

        IKeyedCollection<string, ChannelData> ChannelCache { get; } = new KeyedCollection<string, ChannelData>(
            c => c.Id, theadSafe: true);

        IKeyedCollection<string, VideoAndRecommended> VideoCache { get; } = new KeyedCollection<string, VideoAndRecommended>(
            v => v.Video.Id, theadSafe: true);

        async Task<VideoAndRecommended> GetVideo(string id) {
            if (VideoCache.ContainsKey(id)) return VideoCache[id];
            var videos = await Videos.GetOrCreate();
            var v = await videos.FirstOrDefaultAsync(_ => _.Video.Id == id);
            if (v == null) {
                v = new VideoAndRecommended();
                async Task VideoData() => v.Video = await Yt.GetVideoData(id);
                async Task RelatedVideoData() => v.Recommended = await Yt.GetRelatedVideos(id);
                await Task.WhenAll(VideoData(), RelatedVideoData());
                if (!VideoCache.ContainsKey(id)) {
                    try {
                        await videos.InsertOneAsync(v);
                    }
                    catch (MongoDuplicateKeyException e) {
                        Log.Verbose("duplicate video. expected with high parrralelism: {Exception}", e.Message);
                    }
                }
            }

            VideoCache.Add(v);

            return v;
        }

        async Task<ChannelData> GetChannel(string id) {
            if (ChannelCache.ContainsKey(id)) return ChannelCache[id];
            var channels = await Channels.GetOrCreate();
            var c = await channels.FirstOrDefaultAsync(_ => _.Id == id);
            if (c == null) {
                c = await Yt.ChannelData(id);
                await channels.InsertOneAsync(c);
            }

            ChannelCache.Add(c);
            return c;
        }

        public async Task<CrawlResult> Crawl() {
            var res = new CrawlResult();

            Log.Information("Started crawl with {@Config}", Cfg);

            var seedData = Cfg.SeedPath.ReadFromCsv<SeedChannel>();
            if (Cfg.LimitSeedChannels.HasValue)
                seedData = seedData.Take(Cfg.LimitSeedChannels.Value).ToList();

            var channels = await seedData.BlockTransform(c => GetChannel(c.Id), Cfg.Parallel);
            var crawlResults = await channels.BlockTransform(CrawlFromChannel, 1); // sufficiently parallel inside

            res.Channels.AddRange(crawlResults.SelectMany(r => r.Channels));
            res.Recommends.AddRange(crawlResults.SelectMany(r => r.Recommends));

            return res;
        }

        async Task<CrawlResult> CrawlFromChannel(ChannelData channel) {
            var log = Log.ForContext("SeedChannel", channel.Title);
            var res = new CrawlResult {Channels = {channel}};
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

                async Task<(VideoAndRecommended v, Visit visit)> Visit(VideoAndRecommended f, RecommendedVideoData rec) {
                    var v = await GetVideo(rec.Id);
                    if (v == null) {
                        log.Error("Video unavailable '{Channel}': '{Video}' {VideoId}", rec.ChannelTitle, rec.Title, rec.Id);
                        return (null,null);
                    }

                    var vis = new Visit {
                        VideoId = v.Video.Id,
                        Title = v.Video.Title,
                        ChannelId = v.Video.ChannelId,
                        ChannelTitle = v.Video.ChannelTitle,
                        FromVideoId = f.Video.Id,
                        FromTitle = f.Video.Title,
                        FromChannelId = f.Video.ChannelId,
                        FromChannelTitle = f.Video.ChannelTitle,
                        Rank = rec.Rank,
                        DistanceFromSeed = i
                    };

                    log.Information("Visited '{Channel}: {Title}' from '{FromChannel}: {FromTitle}'",
                        vis.ChannelTitle, vis.Title, vis.FromChannelTitle, vis.FromTitle);

                    return (v,vis);
                }
                
                var crawlTask = toCrawl
                    .SelectMany(from => from.Recommended.Take(Cfg.Related).Select(rec => (from, rec)))
                    .BlockTransform(_ => Visit(_.Item1, _.Item2), Cfg.Parallel, progressUpdate: _ => visitCount = _.Count);


                while (!crawlTask.IsCompleted) {
                    Task.WaitAny(crawlTask, Task.Delay(30.Seconds()));
                    if (crawlTask.IsCompleted) continue;

                    var elapsed = totalTimer.Elapsed;
                    var periodElapsed = elapsed - lastElapsed;
                    var periodVisits = visitCount - lastVisitCount;

                    log.Information("'{SeedChannel}' {Visited}/{Estimated} visited/total (estimate). {VisitSpeed}",
                        channel.Title, visitCount, estimatedVisit, periodVisits.Speed("visits", periodElapsed).Humanize());
                    lastElapsed = elapsed;
                    lastVisitCount = visitCount;
                }

                var newVisits = (await crawlTask).Where(r => r.v != null && !res.Recommends.Contains(r.visit)).ToList();
                res.Recommends.AddRange(newVisits.Select(_ => _.visit));
                res.Channels.AddRange(await newVisits.Select(_ => _.visit.ChannelId).Distinct()
                    .BlockTransform(GetChannel, Cfg.Parallel));

                toCrawl = newVisits.Select(_ => _.v).ToList();
            }

            return res;
        }

        FPath DataDir => "Data".AsPath().InAppData(Setup.AppName);

        public void SaveResult(CrawlResult result) {
            var tsDir = DataDir.Combine(DateTime.UtcNow.FileSafeTimestamp());
            tsDir.EnsureDirectoryExists();
            Cfg.ToJsonFile(tsDir.Combine("cfg.json"));
            result.Recommends.WriteToCsv(tsDir.Combine("recommends.csv"));
            result.Channels.ToJsonFile(tsDir.Combine("channels.json"));
            Log.Information("Saved results. {Reccomends} recomends, {Channels} channels",
                result.Recommends.Count(), result.Channels.Count());
        }

        public class CrawlResult {
            public IKeyedCollection<string, ChannelData> Channels { get; } = new KeyedCollection<string, ChannelData>(c => c.Id, theadSafe:true);
            public IKeyedCollection<string, Visit> Recommends { get; } = new KeyedCollection<string, Visit>(r => $"{r.FromVideoId}.{r.VideoId}", theadSafe:true);
        }
    }


    /// <summary>
    /// A visit is a unique video recommendation from a crawl starting at a single seed channel
    /// Need to record visits separately to record the different way to get to the same video and re-use a video cache
    /// </summary>
    public class Visit {
        public string FromVideoId { get; set; }
        public string VideoId { get; set; }

        public string FromChannelTitle { get; set; }
        public string FromTitle { get; set; }

        public string ChannelTitle { get; set; }
        public string Title { get; set; }

        public string FromChannelId { get; set; }
        public string ChannelId { get; set; }

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
}