using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Serilog;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YouTubeReader {
    public class YtDataUpdater {
        public YtDataUpdater(YtStore store, Cfg cfg, ILogger log) {
            Yt = store;
            Cfg = cfg;
            Log = log;
        }

        Cfg Cfg { get; }
        ILogger Log { get; }
        YtStore Yt { get; }

        public async Task UpdateData() {
            Log.Information("Starting incremental data update {@Config}", Cfg);

            var channelCfg = Cfg.LoadConfig();
            var seeds = channelCfg.Seeds;
            var res = await seeds.BlockTransform(UpdateChannel, Cfg.Parallel, progressUpdate:
                p => Log.Information("Channel update progress {Channels}/{Total} {Speed}",
                    p.Results.Count, seeds.Count, p.Speed("channels").Humanize())).WithDuration(); // sufficiently parallel inside

            Log.Information("Completed updates successfully in {Duration}. {Channels} channels, {Videos} videos.", 
                res.Duration.Humanize(), res.Result.Count, res.Result.Sum(r => r.videos.Count));
        }


        async Task<(ChannelStored channel, ICollection<VideoStored> videos)> UpdateChannel(SeedChannel seed) {
            Log.Information("Updating channel '{Channel}' with new data", seed.Title);
            var channel = await Yt.GetAndUpdateChannel(seed.Id);
            var log = Log.ForContext("Channel", channel.Latest.Title);

            async Task<(VideoStored video, RecommendedVideoStored recommended)> UpdateVideo(ChannelVideoListItem fromV) {
                var video = await Yt.GetAndUpdateVideo(fromV.VideoId);
                var allRecommended = await Yt.GetAndUpdateRecommendedVideos(fromV);
                return (video, allRecommended);
            }

            var channelVideos = await Yt.GetAndUpdateChannelVideos(channel.Latest);
            var updateResults = await channelVideos.Vids.BlockTransform(UpdateVideo, Cfg.Parallel, null,
                p => log.Information("'{Channel}' {Videos}/{Total} channel video's visited. {Speed}",
                    channel.ChannelTitle, p.Results.Count, channelVideos.Vids.Count, p.NewItems.Count.Speed("videos", p.Elapsed).Humanize()));

            log.Information("'{Channel}' updated", channel.ChannelTitle, updateResults.Count);

            return (channel, updateResults.Select(r => r.video).ToList());
        }
    }
}