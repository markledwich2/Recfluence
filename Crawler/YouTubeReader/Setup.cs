using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Security.Authentication;
using Amazon;
using Amazon.Runtime;
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
    }

    public class Cfg {
        public int CacheRelated = 40;
        public int StepsFromSeed { get; set; } = 1;
        public int Related { get; set; } = 10;
        public DateTime From { get; set; }
        public DateTime To { get; set; }

        [TypeConverter(typeof(StringConverter<FPath>))]
        public FPath CrawlConfigDir { get; set; }

        public ICollection<string> YTApiKeys { get; set; }
        public int Parallel { get; set; } = 8;
        public int? LimitSeedChannels { get; set; }

        public int InfluencersToDetect { get; set; } = 0;
        public S3Cfg S3 { get; set; } = new S3Cfg {
            Bucket = "ytnetworks", Credentials = new NameSecret("yourkey", "yoursecret"), Region = RegionEndpoint.APSoutheast2.SystemName
        };

        /// <summary>
        /// Use this when you want to re-use recommendation cache from a previous day
        /// </summary>
        public string RecommendationCacheDayOverride { get; set; }

        /// <summary>
        /// The minimum percentile of the channel's video vies for the given period.
        /// </summary>
        //public double InfluenceMinViewsPercentile { get; set; } = 0.3;
    }
}