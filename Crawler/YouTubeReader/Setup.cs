using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using Amazon;
using Humanizer;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Security;
using SysExtensions.Serialization;

namespace YouTubeReader {
    public static class Setup {
        public static string AppName = "YouTubeNetworks";
        public static FPath SolutionDir => typeof(Setup).LocalAssemblyPath().ParentWithFile("YouTubeNetworks.sln");
        public static FPath SolutionDataDir => typeof(Setup).LocalAssemblyPath().DirOfParent("Data");
        public static FPath LocalDataDir => "Data".AsPath().InAppData(AppName);

        public static Logger CreateLogger() => new LoggerConfiguration()
            .WriteTo.Seq("http://localhost:5341", LogEventLevel.Verbose)
            .WriteTo.Console()
            .CreateLogger();

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
                cfg.CrawlConfigDir = SolutionDataDir;

            return cfg;
        }
    }

    public class Cfg {
        public int CacheRelated = 40;
        public int Related { get; set; } = 10;
        public DateTime From { get; set; }
        public DateTime? To { get; set; }


        public TimeSpan VideoDead { get; set; } = 365.Days();
        public TimeSpan VideoOld { get; set; } = 30.Days();
        public TimeSpan RefreshOldVideos { get; set; } = 7.Days();
        public TimeSpan RefreshYoungVideos { get; set; } = 24.Hours();
        public TimeSpan RefreshChannel { get; set; } = 7.Days();
        public TimeSpan RefreshRelatedVideos { get; set; } = 30.Days();
        public TimeSpan RefreshChannelVideos { get; set; } = 24.Hours();

        [TypeConverter(typeof(StringConverter<FPath>))]
        public FPath CrawlConfigDir { get; set; }

        public ICollection<string> YTApiKeys { get; set; }
        public int Parallel { get; set; } = 8;
        public ICollection<string> LimitedToSeedChannels { get; set; }

        public CollectionCacheType CacheType { get; set; } = CollectionCacheType.Memory;

        public S3Cfg S3 { get; set; } = new S3Cfg {
            Bucket = "ytnetworks", Credentials = new NameSecret("yourkey", "yoursecret"), Region = RegionEndpoint.APSoutheast2.SystemName
        };
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

    public class ChannelConfig {
        public IKeyedCollection<string, SeedChannel> Seeds { get; } = new KeyedCollection<string, SeedChannel>(c => c.Id);
        public IKeyedCollection<string, InfluencerOverride> Excluded { get; } = new KeyedCollection<string, InfluencerOverride>(c => c.Id);
    }

    public static class ChannelConfigExtensions {
        public static ChannelConfig LoadConfig(this Cfg cfg) {
            var channelCfg = new ChannelConfig();
            var seedData = SeedChannels(cfg);
            channelCfg.Seeds.AddRange(cfg.LimitedToSeedChannels != null ? seedData.Where(s => cfg.LimitedToSeedChannels.Contains(s.Id)) : seedData);
            channelCfg.Excluded.AddRange(cfg.CrawlConfigDir.Combine("ChannelExclude.csv").ReadFromCsv<InfluencerOverride>());
            return channelCfg;
        }

        static IEnumerable<SeedChannel> SeedChannels(this Cfg cfg) => cfg.CrawlConfigDir.Combine("SeedChannels.csv").ReadFromCsv<SeedChannel>();
    }
}