using System;
using System.ComponentModel;
using System.Security.Authentication;
using MongoDB.Driver;
using Serilog;
using Serilog.Core;
using Serilog.Events;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;

namespace YouTubeReader {
    public static class Setup {
        public static string AppName = "YouTubeNetworks";
        public static FPath SolutionDir => typeof(Setup).LocalAssemblyPath().ParentWithFile("YouTubeNetworks.sln");
        public static FPath DataDir => SolutionDir.Combine("Data");

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

            if (cfg.SeedPath.IsEmtpy())
                cfg.SeedPath = DataDir.Combine("SeedChannels.csv");

            return cfg;
        }

        public static MongoClient MongoClient(Cfg cfg) {
            var settings = MongoClientSettings.FromUrl(new MongoUrl(cfg.MongoCS));
            settings.SslSettings = new SslSettings {EnabledSslProtocols = SslProtocols.Tls12};
            return new MongoClient(settings);
        }
    }

    public class Cfg {
        public int CacheRelated = 40;
        public int TopInChannel { get; set; } = 10;
        public int StepsFromSeed { get; set; } = 2;
        public int Related { get; set; } = 10;
        public DateTime SeedFromDate { get; set; } = DateTime.UtcNow.AddYears(-1);

        [TypeConverter(typeof(StringConverter<FPath>))]
        public FPath SeedPath { get; set; }

        public string YTApiKey { get; set; } = "YoutubeAPI key here";
        public int Parallel { get; set; } = 8;
        public int? LimitSeedChannels { get; set; } = 2;

        public string MongoCS { get; set; } = "mongo connection string here";
    }
}