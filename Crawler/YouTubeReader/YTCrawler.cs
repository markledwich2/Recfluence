using System;
using System.Collections.Generic;
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
using static YouTubeReader.YtCrawler.CrawlChannelStatus;

namespace YouTubeReader
{
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
            var localCrawlDir = LocalDataDir.Combine(crawlId);

            var channelCfg = new ChannelConfig();
            var seedData = Cfg.CrawlConfigDir.Combine("SeedChannels.csv").ReadFromCsv<SeedChannel>();
            channelCfg.Seeds.AddRange(Cfg.LimitSeedChannels.HasValue ? seedData.Take(Cfg.LimitSeedChannels.Value) : seedData);
            channelCfg.Excluded.AddRange(Cfg.CrawlConfigDir.Combine("ChannelExclude.csv").ReadFromCsv<InfluencerOverride>());


            async Task<CrawlChannelData> ChannelFromSeed(SeedChannel seed) {
                var c = await Yt.Channel(seed.Id);
                return ToCrawlChannelData(c, channelCfg);
            }
            var seedChannels = (await channelCfg.Seeds.BlockTransform(ChannelFromSeed, Cfg.Parallel)).ToKeyedCollection(c => c.Id);
            
            var firstResult = await Crawl(seedChannels, channelCfg);
            Log.Information("Completed first pass crawl. {Channels} channels, {Visits} visits.", firstResult.Channels.Count, firstResult.Visits.Count);
            SaveResult(firstResult, localCrawlDir);

            // with a comprehensive seed list, the discovered channels are only good for reviewing for new seeds, but aren't included in the analysis
            /*
            var influencers = firstResult.Channels
                .Where(c => c.Status.In(Detected, Included))
                .ToKeyedCollection(c => c.Id);
            Log.Information("Crawling {Influencers} influencers", influencers.Count);
            var secondResult = await Crawl(influencers, channelCfg, firstResult);
            
             SaveResult(secondResult, localCrawlDir);
             */
        }

        class ChannelConfig {
            public IKeyedCollection<string, SeedChannel> Seeds { get; } = new KeyedCollection<string, SeedChannel>(c => c.Id);
            public IKeyedCollection<string, InfluencerOverride> Excluded { get; } = new KeyedCollection<string, InfluencerOverride>(c => c.Id);
        }

        FPath LocalDataDir => "Data".AsPath().InAppData(Setup.AppName);

        void SaveResult(ChannelCrawlResult result, FPath dir) {
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

            result.Channels.OrderByDescending(c => c.Channel.SubCount).Select(c => new {
                c.Channel.Id,
                c.Channel.Title,
                c.Status,
                c.Type,
                c.LR,
                c.Channel.ViewCount,
                c.Channel.SubCount,
                c.Recommends,
                c.RecommendsRatio,
                c.ChannelVideoViews,
                c.ViewedRecommends,
                RecommendingChannels = c.RecommendingChannels.Join("|", r => r.Channel.Title),
                VideoTitles = c.ChannelVideoData?.Join("|", v => v.Title) ?? "",
                
            }).WriteToCsv(dir.Combine("Channels.csv"));

            Log.Information("Saved results. {Recommends} recommends, {Channels} channels",
                result.Visits.Count, result.Channels.Count);
        }

        async Task<ChannelCrawlResult> Crawl(IKeyedCollection<string, CrawlChannelData> seedChannels, ChannelConfig channelCfg,
            ChannelCrawlResult priorResult = null) {
            void ProgressUpdate(BulkProgressInfo<ChannelCrawlResult> p)
                => Log.Information("Crawling channels {Channels}/{Total} {Speed}",
                    p.Results.Count, seedChannels.Count, p.Speed("channels").Humanize());

            var perChannelResults = await seedChannels.BlockTransform(c => Crawl(c, channelCfg), 2, null, ProgressUpdate); // sufficiently parallel inside

            var allChannels = perChannelResults.SelectMany(r => r.Channels)
                .Concat((priorResult?.Channels).NotNull()).ToKeyedCollection(c => c.Id);

            var allVisits = (priorResult?.Visits).NotNull().Concat(perChannelResults.SelectMany(r => r.Visits)).ToList();

            if (priorResult == null) {
                PostCrawUpdateStats(allVisits, allChannels);

                var detectedInfluencers = allChannels.Where(c => c.Status == CrawlChannelStatus.Default)
                    .OrderByDescending(c => c.RecommendsRatio).Take(Cfg.InfluencersToDetect).ToList();
                foreach (var i in detectedInfluencers)
                    i.Status = CrawlChannelStatus.Detected;

                // add to channel videos that have been cached for the sake of reviewing influencer detection
                foreach (var c in allChannels.Where(c => c.ChannelVideoData == null)) {
                    var v = Yt.ChannelVideosCached(c.Channel, Cfg.From, Cfg.To);
                    if (v != null)
                        c.ChannelVideoData = await ChannelVideoData(c);
                }

                // populate any recommended channel videos for the sake of reviewing influencer detection
                var toPopulate = allChannels.Where(c => c.ChannelVideoData == null && c.InfluencerStatus).ToList();
                var channelVideos = await toPopulate.BlockTransform(async c => (channel: c, videos: await ChannelVideoData(c)), Cfg.Parallel, null,
                    p => Log.Information("reading channel data from potential influencers {Channels}/{ChannelsTotal}. {Speed}",
                        p.Results.Count, toPopulate.Count, p.NewItems.Count.Speed("channels", p.Elapsed).Humanize()), 30.Seconds());
                foreach (var c in channelVideos)
                    c.channel.ChannelVideoData = c.videos;
            }
            else {
                PostCrawUpdateStats(allVisits, allChannels);
            }

            var res = new ChannelCrawlResult(allChannels, allVisits);
            return res;
        }

        void PostCrawUpdateStats(ICollection<Visit> visits, IKeyedCollection<string, CrawlChannelData> channels) {
            var visitsByTo = visits.ToMultiValueDictionary(v => v.To.ChannelId);
            var visitsByFrom = visits.ToMultiValueDictionary(v => v.From.ChannelId);
            foreach (var c in channels) {
                var recommendations = visitsByTo[c.Channel.Id] ?? new List<Visit>();
                c.Recommends = recommendations.Count;
                c.ViewedRecommends = recommendations.Sum(r => r.From.Views ?? 0);
                c.RecommendingChannels.Init(recommendations.GroupBy(i => i.From.ChannelId).Select(g => channels[g.Key]));
                c.RecommendsRatio = recommendations.GroupBy(r => r.From.ChannelId)
                    .Sum(g => (double)g.Count() / visitsByFrom[g.Key].Count);
            }
        }

        async Task<ChannelCrawlResult> Crawl(CrawlChannelData channel, ChannelConfig channelCfg) {
            Log.Information("Crawling from channel {Channel}. {Related} related, {steps} steps",
                channel.Title, Cfg.Related, Cfg.StepsFromSeed);
            var log = Log.ForContext("Channel", channel.Title);

            if (channel.ChannelVideoData == null)
                channel.ChannelVideoData = await ChannelVideoData(channel);

            var toCrawl = channel.ChannelVideoData.ToList();

            var res = new ChannelCrawlResult {Channels = {channel}};
            for (var i = 1; i <= Cfg.StepsFromSeed; i++) {
                async Task<ICollection<Visit>> Visits(VideoData fromV) {
                    var visits = new List<Visit>();

                    var recommended = (await Yt.VideoRecommended(fromV.Id, Cfg.CacheRelated, Cfg.From))
                        .Recommended.Where(r => r.ChannelId != fromV.ChannelId && !channelCfg.Excluded.ContainsKey(r.ChannelId))
                        .Take(Cfg.Related);

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
                    p => log.Information("'{Channel}' {Visited}/{Total} recommended video's visited. {Speed}",
                        channel.Title, p.Results.Sum(r => r.Count), toCrawlCount * Cfg.Related,
                        p.NewItems.Sum(r => r.Count).Speed("visits", p.Elapsed).Humanize()),
                    10.Seconds());
                log.Information("'{Channel}' completed {Visits} video visits {Step}/{Steps} steps",
                    channel.Title, crawlResults.Sum(v => v.Count), i, Cfg.StepsFromSeed);

                var newVisits = crawlResults.SelectMany(v => v).Where(r => r.To != null && !res.Visits.Contains(r)).ToList();
                res.Visits.AddRange(newVisits);
                toCrawl = newVisits.Select(_ => _.To).ToList();
            }

            async Task<CrawlChannelData> GetCrawlChannelData(string id) => ToCrawlChannelData(await Yt.Channel(id), channelCfg);

            var newChannelIds = res.Visits.Select(v => v.To.ChannelId).Distinct().Where(c => !res.Channels.ContainsKey(c)).ToList();
            var newCrawlChannels = await newChannelIds.BlockTransform(GetCrawlChannelData, Cfg.Parallel, null,
                p => log.Information("'{Channel}' channel info for new visits {Channels}/{ChannelsTotal}. {Speed}",
                    channel.Title, p.Results.Count, newChannelIds.Count, p.Speed("channels").Humanize()), 10.Seconds());

            res.Channels.AddRange(newCrawlChannels);
            return res;
        }


        CrawlChannelData ToCrawlChannelData(ChannelData channel, ChannelConfig channelCfg) {
            var cc = new CrawlChannelData { Channel = channel };

            var seed = channelCfg.Seeds[channel.Id];
            if (channelCfg.Seeds.ContainsKey(cc.Id)) {
                cc.Status = Seed;
                cc.Type = seed.Type;
                cc.LR = seed.LR;
            }
            else if (channelCfg.Excluded.ContainsKey(cc.Id))
                cc.Status = Excluded;

            return cc;
        }

        /// <summary>
        ///     adds channel videos if it is empty and the status is not default
        /// </summary>
        async Task<IReadOnlyCollection<VideoData>> ChannelVideoData(CrawlChannelData cc) {
            var vidItems = await Yt.ChannelVideos(cc.Channel, Cfg.From, Cfg.To);
            var vids = await vidItems.Videos.BlockTransform(v => Yt.Video(v.Id), Cfg.Parallel, progressUpdate:
                p => Log.Information("Getting '{Channel}' channel video data {Videos}/{VideosTotal} {Speed}",
                    cc.Title, p.Results.Count, vidItems.Videos.Count, p.Speed("videos").Humanize()));
            return vids.NotNull().ToList();
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
            Detected,
            Excluded
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
            public string Title => Channel.Title;

            public ChannelData Channel { get; set; }
            public CrawlChannelStatus Status { get; set; }
            public int Recommends { get; set; }

            // null if not populated yet
            public IReadOnlyCollection<VideoData> ChannelVideoData { get; set; }
            public ICollection<CrawlChannelData> RecommendingChannels { get; } = new List<CrawlChannelData>();
            public ulong? ChannelVideoViews => ChannelVideoData?.Sum(v => v?.Views ?? 0);
            public ulong ViewedRecommends { get; set; }

            public bool InfluencerStatus => Status.In(Detected, Seed);
            public double RecommendsRatio { get; set; }
            public string Type { get; set; }
            public string LR { get; set; }

            public override string ToString() => $"{Channel.Title} ({Status})";
        }

        public class SeedChannel {
            public string Id { get; set; }
            public string Title { get; set; }
            public string Type { get; set; }
            public string LR { get; set; }
        }

        public class InfluencerOverride {
            public string Id { get; set; }
            public string Title { get; set; }
        }
    }
}