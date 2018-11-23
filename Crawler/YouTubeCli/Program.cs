using System;
using System.Linq;
using System.Threading.Tasks;
using CommandLine;
using Microsoft.Azure.Batch;
using Microsoft.Azure.Batch.Auth;
using Microsoft.Azure.Batch.FileStaging;
using SysExtensions.Collections;
using SysExtensions.Text;
using YouTubeReader;

namespace YouTubeCli {
    [Verb("collect", HelpText = "read all channel, video, recommendation data and flatten into parquet files")]
    public class CollectOption : CommonOption {
        [Option("batch", HelpText = "When in development, launch as a batch service")]
        public bool LaunchBatch { get; set; }
    }

    [Verb("update", HelpText = "refresh new data from YouTube")]
    public class UpdateOption : CommonOption { }

    public class CommonOption {
        [Option('c', "channels", HelpText = "optional '|' separated list of channels to process")]
        public string ChannelIds { get; set; }

        [Option('p', "parallelism", HelpText = "The number of operations to run at once")]
        public int Parallel { get; set; }
    }


    class Program {
        static int Main(string[] args) {
            var res = Parser.Default.ParseArguments<CollectOption, UpdateOption>(args).MapResult(
                (CollectOption c) => Collect(c),
                (UpdateOption u) => Update(u),
                errs => (int) ExitCode.UnknownError
            );



            return res;
        }

        static void UpdateCfgFromOptions(Cfg cfg, CommonOption c) {
            if (c.ChannelIds.HasValue())
                cfg.App.LimitedToSeedChannels = c.ChannelIds.UnJoin('|').ToList();

            if (c.Parallel > 0) cfg.App.Parallel = c.Parallel;
        }

        static int Collect(CollectOption c) {
            var cfg = Setup.LoadCfg().Result;
            UpdateCfgFromOptions(cfg, c);

            var log = Setup.CreateCliLogger(cfg.App);

            if (c.LaunchBatch) {
                BatchHelper.RunCollectBatch(cfg).GetAwaiter().GetResult();
            }
            else {
                var ytStore = cfg.YtStore(log);
                var ytCollect = new YtCollect(ytStore, cfg.FileStore(cfg.App.AnalysisPath), cfg.App, log);
                ytCollect.SaveChannelRelationData().GetAwaiter().GetResult();
            }

            return (int) ExitCode.Success;
        }

        static int Update(UpdateOption u) => (int) ExitCode.Success;
    }


    public static class BatchHelper {
        public static async Task RunCollectBatch(Cfg cfg) {
            var batchCfg = cfg.App.Batch;
            var client = BatchClient.Open(new BatchSharedKeyCredentials(batchCfg.Url, batchCfg.Account, batchCfg.Key));
            var pool = await client.PoolOperations.GetPoolAsync("win");
            var job = client.JobOperations.CreateJob();
            job.Id = Guid.NewGuid().ToString();
            job.PoolInformation = new PoolInformation {PoolId = pool.Id};

           

            await job.CommitAsync();
        }
    }

    enum ExitCode {
        Success = 0,
        InvalidLogin = 1,
        InvalidFilename = 2,
        UnknownError = 10
    }
}