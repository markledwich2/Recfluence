using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Parquet;
using Serilog.Core;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using static YouTubeReader.YtCrawler.CrawlChannelStatus;

namespace YouTubeReader {
    public class YtCrawler {
        public YtCrawler(YtStore store, Cfg cfg, Logger log) {
            Yt = store;
            Cfg = cfg;
            Log = log;
        }

        Cfg Cfg { get; }
        Logger Log { get; }
        YtStore Yt { get; }

        public async Task Crawl() {
            Log.Information("Crawling seeds {@Config}", Cfg);
            var crawlId = DateTime.UtcNow.ToString("yyyy-MM-dd_HH-mm");

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
            await SaveResults(firstResult, crawlId);

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
        FPath LocalResultsDir => "Results".AsPath().InAppData(Setup.AppName);

        class VisitRow {
            public string VideoId { get; set; }
            public string Title { get; set; }
            public string ChannelId { get; set; }
            public string ChannelTitle { get; set; }
            public string FromVideoId { get; set; }
            public string FromTitle { get; set; }
            public string FromChannelId { get; set; }
            public string FromChannelTitle { get; set; }
            public long FromVideoViews { get; set; }
            public long ToVideoViews { get; set; }
            public int Rank { get; set; }
        }

        class ChannelRow {
            public string Id { get; set; }
            public string Title { get; set; }
            public string Status { get; set; }
            public string Type { get; set; }
            public string LR { get; set; }
            public long ViewCount { get; set; }
            public long SubCount { get; set; }
            public int Recommends { get; set; }
            public double RecommendsRatio { get; set; }
            public long ChannelVideoViews { get; set; }
            public long ViewedRecommends { get; set; }
            public string[] RecommendingChannels { get; set; }
            public string[] VideoTitles { get; set; }
        }

        async Task SaveResults(ChannelCrawlResults result, string crawlId) {
            var localDir = LocalResultsDir.Combine(crawlId);
            localDir.EnsureDirectoryExists();
            var s3Dir = StringPath.Relative("Results", crawlId);
            var localCfgFile = localDir.Combine("Cfg.json");
            Cfg.ToJsonFile(localCfgFile);
            await Yt.S3.Save(s3Dir.Add("Cfg.json"), localCfgFile);

            var visits = result.Visits.Select(v => new VisitRow {
                VideoId = v.To.Id, Title = v.To.Title,
                ChannelId = v.To.ChannelId, ChannelTitle = v.To.ChannelTitle,
                FromVideoId = v.From.Id, FromTitle = v.From.Title,
                FromChannelId = v.From.ChannelId, FromChannelTitle = v.From.ChannelTitle,
                FromVideoViews = (long) (v.From.Views ?? 0), ToVideoViews = (long) (v.To.Views ?? 0),
                Rank = v.ToListItem.Rank
            });
            await SaveParquet(visits, "Recommends");

            var channels = result.Channels.OrderByDescending(c => c.Channel.SubCount).Select(c => new ChannelRow {
                Id = c.Channel.Id,
                Title = c.Channel.Title,
                Status = c.Status.EnumString(),
                Type = c.Type,
                LR = c.LR,
                ViewCount = (long) (c.Channel.ViewCount ?? 0),
                SubCount = (long) (c.Channel.SubCount ?? 0),
                Recommends = c.Recommends,
                RecommendsRatio = c.RecommendsRatio,
                ChannelVideoViews = (long) (c.ChannelVideoViews ?? 0),
                ViewedRecommends = (long) c.ViewedRecommends,
                RecommendingChannels = c.RecommendingChannels.Select(r => r.Channel.Title).ToArray(),
                VideoTitles = c.ChannelVideoData?.Select(v => v.Title).ToArray() ?? new string[] { }
            });
            await SaveParquet(channels, "Channels");

            async Task SaveParquet<T>(IEnumerable<T> rows, string name) where T : new() {
                var localFile = localDir.Combine($"{name}.parquet");
                ParquetConvert.Serialize(rows, localFile.FullPath);
                await Yt.S3.Save(s3Dir.Add(localFile.FileName), localFile);
            }

            Log.Information("Saved results. {Recommends} recommends, {Channels} channels",
                result.Visits.Count, result.Channels.Count);
        }

        async Task<ChannelCrawlResults> Crawl(IKeyedCollection<string, CrawlChannelData> seedChannels, ChannelConfig channelCfg) {
            void ProgressUpdate(BulkProgressInfo<ChannelCrawlResult> p)
                => Log.Information("Crawling channels {Channels}/{Total} {Speed}",
                    p.Results.Count, seedChannels.Count, p.Speed("channels").Humanize());

            var perChannelResults = await seedChannels.BlockTransform(c => Crawl(c, channelCfg), Cfg.Parallel, null, ProgressUpdate); // sufficiently parallel inside
            var allChannels = perChannelResults.SelectMany(r => r.Channels).NotNull().ToKeyedCollection(c => c.Id);
            var allVisits = perChannelResults.SelectMany(r => r.Visits).ToList();

            PostCrawUpdateStats(allVisits, allChannels);

            var detectedInfluencers = allChannels.Where(c => c.Status == Default)
                .OrderByDescending(c => c.RecommendsRatio).Take(Cfg.InfluencersToDetect).ToList();
            foreach (var i in detectedInfluencers)
                i.Status = Detected;

            // populate any recommended channel videos for the sake of reviewing influencer detection
            var toPopulate = allChannels.Where(c => c.ChannelVideoData == null && c.Status == Seed).ToList();
            var channelVideos = await toPopulate.BlockTransform(async c => (channel: c, videos: await ChannelVideoData(c)), Cfg.Parallel, null,
                p => Log.Information("reading channel data from potential influencers {Channels}/{ChannelsTotal}. {Speed}",
                    p.Results.Count, toPopulate.Count, p.NewItems.Count.Speed("channels", p.Elapsed).Humanize()), 30.Seconds());
            foreach (var c in channelVideos)
                c.channel.ChannelVideoData = c.videos;

            var res = new ChannelCrawlResults(allChannels, allVisits);
            return res;
        }

        void PostCrawUpdateStats(ICollection<Visit> visits, IKeyedCollection<string, CrawlChannelData> channels) {
            var visitsByTo = visits.ToMultiValueDictionary(v => v.To.ChannelId);
            var visitsByFrom = visits.ToMultiValueDictionary(v => v.From.ChannelId);
            foreach (var c in channels) {
                var recommendations = visitsByTo[c.Channel.Id] ?? new List<Visit>();
                c.Recommends = recommendations.Count;
                c.ViewedRecommends = recommendations.Sum(r => r.From.Views ?? 0);
                c.RecommendingChannels = recommendations.GroupBy(i => i.From.ChannelId).Select(g => channels[g.Key]).ToList();
                c.RecommendsRatio = recommendations.GroupBy(r => r.From.ChannelId)
                    .Sum(g => (double) g.Count() / visitsByFrom[g.Key].Count);
            }
        }

        async Task<ChannelCrawlResult> Crawl(CrawlChannelData channel, ChannelConfig channelCfg) {
            Log.Information("Crawling from channel {Channel}. {Related} related, {steps} steps",
                channel.Title, Cfg.Related, Cfg.StepsFromSeed);
            var log = Log.ForContext("Channel", channel.Title);

            if (channel.ChannelVideoData == null)
                channel.ChannelVideoData = await ChannelVideoData(channel);


            var result = await Yt.ChannelCrawls.Get(channel.Id);
            if (result != null) {
                log.Information("Using cached channel crawl result {Channel}", channel.Title);
                return result;
            }

            var toCrawl = channel.ChannelVideoData.ToList();

            var res = new ChannelCrawlResult {ChannelId  = channel.Id, Channels = {channel}};
            for (var i = 1; i <= Cfg.StepsFromSeed; i++) {
                async Task<ICollection<Visit>> Visits(VideoData fromV) {
                    var visits = new List<Visit>();

                    var recommended = (await Yt.RecommendedVideos(fromV.Id))?
                        .Recommended.Where(r => r.ChannelId != fromV.ChannelId && !channelCfg.Excluded.ContainsKey(r.ChannelId))
                        .Take(Cfg.Related);
                    if (recommended == null) return visits;

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

            await Yt.ChannelCrawls.Set(res);
            return res;
        }


        CrawlChannelData ToCrawlChannelData(ChannelData channel, ChannelConfig channelCfg) {
            var cc = new CrawlChannelData {Channel = channel};

            var seed = channelCfg.Seeds[channel.Id];
            if (channelCfg.Seeds.ContainsKey(cc.Id)) {
                cc.Status = Seed;
                cc.Type = seed.Type;
                cc.LR = seed.LR;
            }
            else if (channelCfg.Excluded.ContainsKey(cc.Id)) {
                cc.Status = Excluded;
            }

            return cc;
        }

        /// <summary>
        ///     adds channel videos if it is empty and the status is not default
        /// </summary>
        async Task<IReadOnlyCollection<VideoData>> ChannelVideoData(CrawlChannelData cc) {
            var vidItems = await Yt.ChannelVideos(cc.Channel);
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
            public string ChannelId { get; set; }

            public IKeyedCollection<string, CrawlChannelData> Channels { get; } =
                new KeyedCollection<string, CrawlChannelData>(c => c.Channel.Id, theadSafe: true);

            public IKeyedCollection<string, Visit> Visits { get; } =
                new KeyedCollection<string, Visit>(v => $"{v.From.Id}.{v.To.Id}", theadSafe: true);
        }

        public class ChannelCrawlResults {
            public ChannelCrawlResults(IEnumerable<CrawlChannelData> channels, IEnumerable<Visit> visits) {
                Channels.AddRange(channels);
                Visits.AddRange(visits);
            }

            public ChannelCrawlResults() { }

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



            public ICollection<CrawlChannelData> RecommendingChannels { get; set; }
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