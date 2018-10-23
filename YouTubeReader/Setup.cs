using Serilog;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Security.Authentication;
using System.Text;
using MongoDB.Driver;
using Serilog.Core;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;

namespace YouTubeReader
{
    public static class Setup
    {
        public static FPath SolutionDir => typeof(Setup).LocalAssemblyPath().ParentWithFile("YouTubeNetworks.sln");
        public static FPath DataDir => SolutionDir.Combine("Data");

        public static Logger CreateLogger() => new LoggerConfiguration()
                .WriteTo.Seq("http://localhost:5341")
                .WriteTo.Console()
                .CreateLogger();

        public static Cfg Cfg() =>
            new Cfg {
                SeedPath = DataDir.Combine("SeedChannels.csv")
            };

        public static string AppName = "YouTubeNetworks";

        public static MongoClient MongoClient(Cfg cfg) {
            var settings = MongoClientSettings.FromUrl(new MongoUrl(cfg.MongoCS));
            settings.SslSettings = new SslSettings { EnabledSslProtocols = SslProtocols.Tls12 };
            return new MongoClient(settings);
        }
    }

    public class Cfg {
        public int TopInChannel { get; set; } = 5;
        public int StepsFromSeed { get; set; } = 3;
        public int Related { get; set; } = 5;
        public int CacheRelated = 20;
        public DateTime SeedFromDate { get; set; } = DateTime.UtcNow.AddYears(-1);

        [TypeConverter(typeof(StringConverter<FPath>))]
        public FPath SeedPath { get; set; }

        public string YTApiKey { get; set; } = "AIzaSyCroVkMyy4jGLm0kQ1o56Jzt9mVaMGho3Q";
        public int Parallel { get; set; } = 1;
        public int? LimitSeedChannels { get; set; } = 2;


        public string MongoCS { get; set; } = @"mongodb://ytnetworks:W6KrJRQUxHUj9x3t9f39KdmsmR9651bH3SHCalMGejIhq1T1nDACaXAqSOXo1UzDMLlBB4Rm4FfgJKEs9CMiKA==@ytnetworks.documents.azure.com:10255/?ssl=true&replicaSet=globaldb";
    }
}
