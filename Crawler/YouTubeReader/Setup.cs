using System;
using System.ComponentModel;
using System.Security.Authentication;
using LiteDB;
using Serilog;
using Serilog.Events;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using Logger = Serilog.Core.Logger;

namespace YouTubeReader {
    public static class Setup {
        public static string AppName = "YouTubeNetworks";
        public static FPath SolutionDir => typeof(Setup).LocalAssemblyPath().ParentWithFile("YouTubeNetworks.sln");
        public static FPath SolutionDataDir => typeof(Setup).LocalAssemblyPath().DirOfParent("Data");
        public static FPath LocalDataDir => "Data".AsPath().InAppData(AppName);

        public static Logger CreateLogger() {
            return new LoggerConfiguration()
                .WriteTo.Seq("http://localhost:5341", restrictedToMinimumLevel: LogEventLevel.Verbose)
                .WriteTo.Console()
                .CreateLogger();
        }

        static FPath CfgPath => "cfg.json".AsPath().InAppData(AppName);

        public static Cfg LoadCfg(ILogger log) {
            var path = CfgPath;
            Cfg cfg;
            if (!path.Exists) {
                cfg = new Cfg();
                cfg.ToJsonFile(path);
                log.Error($"No config found at '{CfgPath}'. Created with default settings, some settings will need to be configured manually");
            }
            else {
                cfg = CfgPath.ToObject<Cfg>();
            }

            if (cfg.CrawlConfigDir.IsEmtpy())
                cfg.CrawlConfigDir = SolutionDataDir.Combine("1.Crawl");

            return cfg;
        }

        public static LiteDatabase Db() {
            var db = new LiteDatabase(LocalDataDir.Combine("YouTubeCache.db").FullPath);
            return db;
        }

        public static YtCacheDb CacheDb(LiteDatabase db = null, YtReader yt = null) => 
            new YtCacheDb(db ?? Db(), yt ?? new YtReader(LoadCfg(null)));
    }

    public class Cfg {
        public int CacheRelated = 40;
        public int StepsFromSeed { get; set; } = 1;
        public int Related { get; set; } = 10;
        public DateTime From { get; set; }
        public DateTime To { get; set; }

        [TypeConverter(typeof(StringConverter<FPath>))]
        public FPath CrawlConfigDir { get; set; }

        public string YTApiKey { get; set; } = "YoutubeAPI key here";
        public int Parallel { get; set; } = 8;
        public int? LimitSeedChannels { get; set; }

        public int InfluencersToDetect { get; set; } = 0;

        /// <summary>
        /// The minimum percentile of the channel's video vies for the given period
        /// </summary>
        //public double InfluenceMinViewsPercentile { get; set; } = 0.3;
    }
}