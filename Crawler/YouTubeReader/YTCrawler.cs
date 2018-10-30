using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using LiteDB;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using Logger = Serilog.Core.Logger;

namespace YouTubeReader {
    public class YtCrawler {
        public YtCrawler(LiteDatabase db, YtReader yt, Cfg cfg, Logger log) {
            Yt = new YtCacheDb(db, yt);
            Cfg = cfg;
            Log = log;
        }

        Cfg Cfg { get; }
        Logger Log { get; }
        YtCacheDb Yt { get; }

        public async Task Crawl() {
            Log.Information("Crawling seeds {@Config}", Cfg);
            var crawlId = DateTime.UtcNow.ToString("yyyy-MM-dd_HH-mm");
            var dir = DataDir.Combine(crawlId);

            var seedData = Cfg.SeedPath.ReadFromCsv<SeedChannel>();
            if (Cfg.LimitSeedChannels.HasValue)
                seedData = seedData.Take(Cfg.LimitSeedChannels.Value).ToList();

            var seedChannels = (await seedData.BlockTransform(c => Yt.Channel(c.Id), Cfg.Parallel)).ToKeyedCollection(c => c.Id);
            var firstResult = await Crawl(seedChannels);
            SaveResult(firstResult, dir.Combine("FirstPass"));

            /*
            var influencers = firstResult.Channels.Where(c => c.Status == CrawlChannelStatus.Influencer)
                .Select(c => c.Channel).ToKeyedCollection(c => c.Id);
            Log.Information("Crawling {Influencers} influencers", influencers.Count);
            var secondResult = await Crawl(influencers, firstResult);

            Log.Information("Completed crawl. {Channels} channels, {Visits} visits.", secondResult.Channels.Count, secondResult.Visits.Count);

            SaveResult(secondResult, dir);*/
        }

        async Task<ChannelCrawlResult> Crawl(IKeyedCollection<string, ChannelData> seedChannels, ChannelCrawlResult priorResult = null) {
            void ProgressUpdate(BulkProgressInfo<ChannelCrawlResult> p) 
                => Log.Information("Crawling channels {Channels}/{Total} {Speed}", 
                    p.Results.Count, seedChannels.Count, p.NewItems.Count.Speed("channels", p.Elapsed).Humanize());

            var perChannelResults = await seedChannels.BlockTransform(Crawl, 2, null, ProgressUpdate); // sufficiently parallel inside

            var allChannels = perChannelResults.SelectMany(r => r.Channels)
                .Concat(priorResult?.Channels.Select(c => c) ?? new CrawlChannelData[] { })
                .ToKeyedCollection(c => c.Channel.Id);
            var allVisits = (priorResult?.Visits.Select(v => v) ?? new Visit[] { })
                .Concat(perChannelResults.SelectMany(r => r.Visits)).ToList();

            await PostCrawlChannelUpdate(allVisits, allChannels);
            var res = new ChannelCrawlResult(allChannels, allVisits);
            return res;
        }

        async Task<ChannelCrawlResult> Crawl(ChannelData channel) {
            Log.Information("Crawling from channel {SeedChannel}. {Related} related, {steps} steps",
                channel.Title, Cfg.Related, Cfg.StepsFromSeed);
            var log = Log.ForContext("SeedChannel", channel.Title);
            var cc = CrawlChannelData(channel, true);
            var toCrawl = cc.ChannelVideoData = await ChannelVideoData(cc);

            var res = new ChannelCrawlResult {Channels = {cc}};
            for (var i = 1; i <= Cfg.StepsFromSeed; i++) {
                async Task<ICollection<Visit>> Visits(VideoData fromV) {
                    var visits = new List<Visit>();

                    var recommended = (await Yt.VideoRecommended(fromV.Id, Cfg.CacheRelated, Cfg.From))
                        .Recommended.Where(r => r.ChannelId != fromV.ChannelId).Take(Cfg.Related);

                    foreach (var r in recommended) {
                        var v = await Yt.Video(r.Id);
                        if (v == null) {
                            log.Error("Video unavailable '{Channel}': '{Video}' {VideoId}", r.ChannelTitle, r.Title, r.Id);
                            continue;
                        }

                        var vis = new Visit(fromV, v, r, i);
                        visits.Add(vis);

                        log.Verbose("Visited '{Channel}: {Title}' from '{FromChannel}: {FromTitle}'",
                            vis.To.ChannelTitle, vis.To.Title, vis.From.ChannelTitle, vis.From.Title);
                    }

                    return visits;
                }

                var toCrawlCount = toCrawl.Count;
                var crawlResults = await toCrawl.BlockTransform(Visits, Cfg.Parallel, null,
                    p => log.Information("'{SeedChannel}' {Visited}/{Total} recommended video's visited. {Speed}",
                        channel.Title, p.Results.Sum(r => r.Count), toCrawlCount * Cfg.Related, p.NewItems.Sum(r => r.Count).Speed("visits", p.Elapsed).Humanize()),
                    10.Seconds());
                log.Information("'{SeedChannel}' completed {Visits} video visits {Step}/{Steps} steps", 
                    channel.Title, crawlResults.Sum(v => v.Count), i, Cfg.StepsFromSeed);

                var newVisits = crawlResults.SelectMany(v => v).Where(r => r.To != null && !res.Visits.Contains(r)).ToList();
                res.Visits.AddRange(newVisits);
                toCrawl = newVisits.Select(_ => _.To).ToList();
            }

            var newChannelIds = res.Visits.Select(v => v.To.ChannelId).Distinct().Where(c => !res.Channels.ContainsKey(c)).ToList();
            var newCrawlChannels = await newChannelIds.BlockTransform(id => CrawlChannelData(id), Cfg.Parallel, null,
                p => log.Information("'{SeedChannel}' channel info for new visits {Channels}/{ChannelsTotal}. {Speed}",
                    channel.Title, p.Results.Count, newChannelIds.Count, p.NewItems.Count.Speed("channels", p.Elapsed).Humanize()), 10.Seconds());

            res.Channels.AddRange(newCrawlChannels);
            return res;
        }

        async Task<CrawlChannelData> CrawlChannelData(string id, bool seed = false) {
            var c = await Yt.Channel(id);
            return CrawlChannelData(c, seed);
        }

        CrawlChannelData CrawlChannelData(ChannelData channel, bool seed = false) {
            var cc = new CrawlChannelData {
                Channel = channel,
                Status = seed ? CrawlChannelStatus.Seed : CrawlChannelStatus.Default
            };
            return cc;
        }

        /// <summary>
        ///     adds channel videos if it is empty and the status is not default
        /// </summary>
        async Task<IReadOnlyCollection<VideoData>> ChannelVideoData(CrawlChannelData cc) {
            var vids = await Yt.ChannelVideos(cc.Channel, Cfg.From, Cfg.To);
            return await vids.Videos.BlockTransform(v => Yt.Video(v.Id), Cfg.Parallel);
        }

        async Task PostCrawlChannelUpdate(ICollection<Visit> visits, IKeyedCollection<string, CrawlChannelData> channels) {
            var visitsByTo = visits.ToMultiValueDictionary(v => v.To.ChannelId);

            foreach (var c in channels) {
                var recommendations = visitsByTo[c.Channel.Id] ?? new List<Visit>();
                c.Recommends = recommendations.Count;
                c.ViewedRecommends = recommendations.Sum(r => r.From.Views ?? 0);
                c.RecommendingChannels.Init(recommendations.GroupBy(i => i.From.ChannelId).Select(g => channels[g.Key]));
            }

            var minRecommending = (int) channels.Percentile(c => c.RecommendingChannels.Count, Cfg.InfluenceMinRecommendingChannelsPercentile);
            var minRecommendingViews = (ulong) channels.Percentile(c => c.ViewedRecommends, Cfg.InfluenceMinViewedRecommendsPercentile);

            var potentialInfluencers = channels.Where(c =>
                c.Status != CrawlChannelStatus.Seed && c.RecommendingChannels.Count >= minRecommending
                                                    && c.ViewedRecommends >= minRecommendingViews).ToList();

            var channelsToPopulate = potentialInfluencers.Where(c => c.ChannelVideoData == null).ToList();
            if (channelsToPopulate.Count < 500) {
                var channelVideos = await channelsToPopulate.BlockTransform(async c => (channel: c, videos: await ChannelVideoData(c)), Cfg.Parallel, null,
                    p => Log.Information("reading channel data from potential influencers {Channels}/{ChannelsTotal}. {Speed}",
                        p.Results.Count, channelsToPopulate.Count, p.NewItems.Count.Speed("channels", p.Elapsed).Humanize()), 10.Seconds());
                foreach (var c in channelVideos)
                    c.channel.ChannelVideoData = c.videos;


                var minViews = channels.Where(c => c.ChannelVideoViews.HasValue).Percentile(c => c.ChannelVideoViews ?? 0, Cfg.InfluenceMinViewsPercentile);
                foreach (var c in potentialInfluencers)
                    c.Status = (c.ChannelVideoViews == 0 || c.ChannelVideoViews >= minViews) ? CrawlChannelStatus.Influencer : CrawlChannelStatus.Default;
            }
            else {
                Log.Error("Too many potential influencers to populate their channel data");
            }
        }

        FPath DataDir => "Data".AsPath().InAppData(Setup.AppName);

        public void SaveResult(ChannelCrawlResult result, FPath dir) {
            dir.EnsureDirectoryExists();
            Cfg.ToJsonFile(dir.Combine("Cfg.json"));

            result.Visits.Select(v => new {
                VideoId = v.To.Id, v.To.Title,
                v.To.ChannelId, v.To.ChannelTitle,
                FromVideoId = v.From.Id, FromTitle = v.From.Title,
                FromChannelId = v.From.ChannelId, FromChannelTitle = v.From.ChannelTitle,
                FromVideoViews = v.From.Views, ToVideoViews = v.To.Views,
                v.ToListItem.Rank, v.DistanceFromSeed
            }).WriteToCsv(dir.Combine("Visits.csv"));

            result.Channels.Select(c => new {
                c.Channel.Id,
                c.Channel.Title,
                c.Status,
                c.Channel.ViewCount,
                c.Channel.SubCount,
                c.Recommends,
                c.ChannelVideoViews,
                RecommendingChannels = c.RecommendingChannels.Join("|", r => r.Channel.Title),
                VideoTitles = c.ChannelVideoData?.Join("|", v => v.Title) ?? "",
                c.ViewedRecommends
            }).WriteToCsv(dir.Combine("Channels.csv"));

            result.Channels.ToJsonFile(dir.Combine("Channels.json"));

            Log.Information("Saved results. {Recommends} recommends, {Channels} channels",
                result.Visits.Count, result.Channels.Count);
        }
    }

    public class Visit {
        public Visit() { }

        public Visit(VideoData from, VideoData to, RecommendedVideoListItem toListItem, int distanceFromSeed) {
            From = from;
            To = to;
            ToListItem = toListItem;
            DistanceFromSeed = distanceFromSeed;
        }

        public VideoData From { get; set; }
        public VideoData To { get; set; }
        public RecommendedVideoListItem ToListItem { get; set; }
        public int DistanceFromSeed { get; set; }
    }

    public enum CrawlChannelStatus {
        Default,
        Seed,
        Influencer
    }

    public class ChannelCrawlResult {
        public ChannelCrawlResult(IEnumerable<CrawlChannelData> channels, IEnumerable<Visit> visits) {
            Channels.AddRange(channels);
            Visits.AddRange(visits);
        }

        public ChannelCrawlResult() { }

        public IKeyedCollection<string, CrawlChannelData> Channels { get; } =
            new KeyedCollection<string, CrawlChannelData>(c => c.Channel.Id, theadSafe: true);

        public IKeyedCollection<string, Visit> Visits { get; } =
            new KeyedCollection<string, Visit>(v => $"{v.From.Id}.{v.To.Id}", theadSafe: true);
    }

    public class CrawlChannelData {
        public string Id => Channel.Id;

        public ChannelData Channel { get; set; }
        public CrawlChannelStatus Status { get; set; }
        public int Recommends { get; set; }

        // null if not populated yet
        public IReadOnlyCollection<VideoData> ChannelVideoData { get; set; }
        public ICollection<CrawlChannelData> RecommendingChannels { get; } = new List<CrawlChannelData>();
        public ulong? ChannelVideoViews => ChannelVideoData?.Sum(v => v.Views ?? 0);
        public ulong ViewedRecommends { get; set; }

        public override string ToString() => $"{Channel.Title} ({Status})";
    }

    public class SeedChannel {
        public string Title { get; set; }
        public string Id { get; set; }
        public string Tags { get; set; }
    }
}