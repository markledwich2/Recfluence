using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using LiteDB;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using Logger = Serilog.Core.Logger;

namespace YouTubeReader {
    public class YTCrawler {
        public YTCrawler(LiteDatabase db, YTReader yt, Cfg cfg, Logger log) {
            Db = db;
            Yt = yt;
            Cfg = cfg;
            Log = log;

            Channels = Db.Channels();
            Videos = Db.Videos();
        }

        LiteDatabase Db { get; }
        YTReader Yt { get; }
        Cfg Cfg { get; }
        Logger Log { get; }

        LiteCollection<ChannelData> Channels { get; }
        LiteCollection<VideoAndRecommended> Videos { get; }

        IKeyedCollection<string, ChannelData> ChannelCache { get; } = new KeyedCollection<string, ChannelData>(
            c => c.Id, theadSafe: true);

        IKeyedCollection<string, VideoAndRecommended> VideoCache { get; } = new KeyedCollection<string, VideoAndRecommended>(
            v => v.Video.Id, theadSafe: true);

        async Task<VideoAndRecommended> GetVideo(string id) {
            if (VideoCache.ContainsKey(id)) return VideoCache[id];
            var v = Videos.FindById(id);
            if (v == null) {
                v = new VideoAndRecommended();

                async Task VideoData() {
                    v.Video = await Yt.GetVideoData(id);
                }

                async Task RelatedVideoData() {
                    v.Recommended = await Yt.GetRelatedVideos(id);
                }

                await Task.WhenAll(VideoData(), RelatedVideoData());

                if (v.Video == null)
                    return null;

                if (!VideoCache.ContainsKey(id))
                    Videos.Upsert(v);
            }

            VideoCache.Add(v);
            return v;
        }

        async Task<ChannelData> GetChannel(string id) {
            if (ChannelCache.ContainsKey(id)) return ChannelCache[id];
            var c = Channels.FindById(id);
            if (c == null) {
                c = await Yt.ChannelData(id);
                Channels.Upsert(c);
            }

            ChannelCache.Add(c);
            return c;
        }

        public async Task<CrawlResult> Crawl() {
            Log.Information("Crawling seeds {@Config}", Cfg);
            var crawlId = DateTime.UtcNow.ToString("yyyy-MM-dd_HH-mm");
            var dir = DataDir.Combine(crawlId);

            var seedData = Cfg.SeedPath.ReadFromCsv<SeedChannel>();
            if (Cfg.LimitSeedChannels.HasValue)
                seedData = seedData.Take(Cfg.LimitSeedChannels.Value).ToList();

            var seedChannels = (await seedData.BlockTransform(c => GetChannel(c.Id), Cfg.Parallel)).ToKeyedCollection(c => c.Id);
            var firstResult = await CrawlFromChannels(seedChannels);
            SaveResult(firstResult, dir.Combine("FirstPass"));

            var influencers = firstResult.Channels.Where(c => c.Status == CrawlChannelStatus.Influencer)
                .Select(c => c.Channel).ToKeyedCollection(c => c.Id);
            Log.Information("Crawling {Influencers} influencers", influencers.Count);
            var secondResult = await CrawlFromChannels(influencers, firstResult);

            Log.Information("Completed crawl. {Channels} channels, {Visits} visits.", secondResult.Channels.Count, secondResult.Visits.Count);

            SaveResult(secondResult, dir);

            return secondResult;
        }

        IEnumerable<CrawlChannelData> ChannelWithStatus(MultiValueDictionary<string,Visit> visitsByTo, IKeyedCollection<string, ChannelData> channels, 
            Func<ChannelData, bool> isSeed) {
            foreach (var c in channels) {
                var cc = new CrawlChannelData {
                    Channel = c,
                    Status = isSeed(c) ? CrawlChannelStatus.Seed : CrawlChannelStatus.Ignored
                };

                var incoming = visitsByTo.ContainsKey(c.Id) ? visitsByTo[c.Id] : new List<Visit>();
                cc.Recommends = incoming.Count;
                cc.RecommendingChannels.Init(incoming.GroupBy(i => i.FromChannelId).Select(g => channels[g.Key]));
                if (cc.Status != CrawlChannelStatus.Seed
                        && cc.RecommendingChannels.Count >= Cfg.InfuenceMinimumUniqueIncoming
                        && cc.Recommends >= Cfg.InfluenceMinimumIncoming
                        && (c.SubCount == 0 || c.SubCount >= Cfg.InfluenceMinimumSubs)) // 0 = we don't have access to that stat. Allow it.
                    cc.Status = CrawlChannelStatus.Influencer;

                yield return cc;
            }
        }

        async Task<CrawlResult> CrawlFromChannels(IKeyedCollection<string, ChannelData> seedChannels, CrawlResult priorResult = null) {
            var perChannelResults = await seedChannels.BlockTransform(CrawlFromChannel, 1); // sufficiently parallel inside

            var allChannels = perChannelResults.SelectMany(r => r.Channels)
                .Concat(priorResult?.Channels.Select(c => c.Channel) ?? new ChannelData[] {})
                .ToKeyedCollection(c => c.Id);
            var allVisits = (priorResult?.Visits.Select(v => v) ?? new Visit[] {}).Concat(perChannelResults.SelectMany(r => r.Visits)).ToList();
            var visitsByTo = allVisits.ToMultiValueDictionary(k => k.ChannelId, v => v);


            var res = new CrawlResult();
            bool IsSeed(ChannelData c) => seedChannels.Contains(c) || priorResult?.Channels[c.Id]?.Status == CrawlChannelStatus.Seed;
            res.Channels.AddRange(allChannels.SelectMany(r => ChannelWithStatus(visitsByTo, allChannels, IsSeed)));
            res.Visits.AddRange(allVisits);

            return res;
        }

        async Task<ChannelCrawlResult> CrawlFromChannel(ChannelData channel) {
            Log.Information("Crawling top {Top} from channel {SeedChannel}, {Related} related, {steps} steps",
                Cfg.TopInChannel, channel.Title, Cfg.Related, Cfg.StepsFromSeed);
            var log = Log.ForContext("SeedChannel", channel.Title);
            var res = new ChannelCrawlResult {Channels = {channel}};
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
                        return (null, null);
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

                    log.Verbose("Visited '{Channel}: {Title}' from '{FromChannel}: {FromTitle}'",
                        vis.ChannelTitle, vis.Title, vis.FromChannelTitle, vis.FromTitle);

                    return (v, vis);
                }

                var recommendedToCrawl = toCrawl.SelectMany(from => RecommendedToCrawl(from).Select(rec => (from: from, rec: rec))).ToList();
                var crawlTask = recommendedToCrawl
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

                var newVisits = (await crawlTask).Where(r => r.v != null && !res.Visits.Contains(r.visit)).ToList();
                res.Visits.AddRange(newVisits.Select(_ => _.visit));

                var channels = await newVisits.Select(_ => _.visit.ChannelId).Distinct()
                    .BlockTransform(GetChannel, Cfg.Parallel);


                res.Channels.AddRange(channels);

                toCrawl = newVisits.Select(_ => _.v).ToList();
            }

            return res;
        }

        IEnumerable<RecommendedVideoData> RecommendedToCrawl(VideoAndRecommended from) {
            return from.Recommended.Where(r => r.ChannelId != from.Video.ChannelId).Take(Cfg.Related);
        }

        FPath DataDir => "Data".AsPath().InAppData(Setup.AppName);

        public void SaveResult(CrawlResult result, FPath dir) {
            dir.EnsureDirectoryExists();
            Cfg.ToJsonFile(dir.Combine("Cfg.json"));
            result.Visits.WriteToCsv(dir.Combine("Visits.csv"));
            result.Channels.Select(c => new {
                c.Channel.Id,
                c.Channel.Title,
                c.Status,
                c.Channel.ViewCount,
                c.Channel.SubCount,
                c.Recommends,
                RecommendingChannels = c.RecommendingChannels.Join("|", r => r.Id)
            }).WriteToCsv(dir.Combine("Channels.csv"));
            result.Channels.ToJsonFile(dir.Combine("Channels.json"));

            Log.Information("Saved results. {Reccomends} recomends, {Channels} channels",
                result.Visits.Count(), result.Channels.Count());
        }

        public class CrawlResult {
            public IKeyedCollection<string, CrawlChannelData> Channels { get; } = new KeyedCollection<string, CrawlChannelData>(c => c.Channel.Id);
            public IKeyedCollection<string, Visit> Visits { get; } = new KeyedCollection<string, Visit>(r => $"{r.FromVideoId}.{r.VideoId}", theadSafe: true);
        }

        public class ChannelCrawlResult {
            public IKeyedCollection<string, ChannelData> Channels { get; } = new KeyedCollection<string, ChannelData>(c => c.Id, theadSafe: true);
            public IKeyedCollection<string, Visit> Visits { get; } = new KeyedCollection<string, Visit>(r => $"{r.FromVideoId}.{r.VideoId}", theadSafe: true);
        }
    }


    /// <summary>
    ///     A visit is a unique video recommendation from a crawl starting at a single seed channel
    ///     Need to record visits separately to record the different way to get to the same video and re-use a video cache
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

    public enum CrawlChannelStatus {
        Ignored,
        Seed,
        SeedInfluencer,
        Influencer
    }

    public class CrawlChannelData {
        public ChannelData Channel { get; set; }

        public CrawlChannelStatus Status { get; set; }

        /// <summary>
        ///     If false, this channel has not been crawled
        /// </summary>
        public int Recommends { get; set; }

        public ICollection<ChannelData> RecommendingChannels { get; } = new List<ChannelData>();


        public override string ToString() {
            return $"{Channel.Title} ({Status})";
        }
    }

    public class SeedChannel {
        public string Title { get; set; }
        public string Id { get; set; }
        public string Tags { get; set; }
    }
}