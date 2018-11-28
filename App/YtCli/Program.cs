using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Management;
using Microsoft.Azure;
using CommandLine;
using YtReader;
using SysExtensions.Text;

namespace YouTubeCli
{
    [Verb("collect", HelpText = "read all channel, video, recommendation data and flatten into parquet files")]
    public class CollectOption : CommonOption {
        [Option('z', "cloudinstance", HelpText = "launch a container instance")]
        public bool LaunchContainer { get; set; }
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
                (CollectOption c) => Collect(c, args),
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

        static int Collect(CollectOption c, string[] args) {
            var cfg = Setup.LoadCfg().Result;
            UpdateCfgFromOptions(cfg, c);
            var log = Setup.CreateCliLogger(cfg.App);

            if (c.LaunchContainer) {
                YtCollectRunner.Start(log, cfg, args.Where(a => a != "-z").ToArray()).GetAwaiter().GetResult();
            }
            else {
                var ytStore = cfg.YtStore(log);
                var ytCollect = new YtCollect(ytStore, cfg.DataStore(cfg.App.Storage.AnalysisPath), cfg.App, log);
                ytCollect.SaveChannelRelationData().GetAwaiter().GetResult();
            }

            return (int) ExitCode.Success;
        }

        static int Update(UpdateOption u) => (int) ExitCode.Success;
    }

    enum ExitCode {
        Success = 0,
        InvalidLogin = 1,
        InvalidFilename = 2,
        UnknownError = 10
    }
}