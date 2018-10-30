using System;
using YouTubeReader;

namespace YouTubeNetworks {
    class Program {
        static void Main(string[] args) {
            using (var log = Setup.CreateLogger()) {
                var cfg = Setup.LoadCfg(log);
                var reader = new YtReader(cfg);
                var db = Setup.Db();
                var crawler = new YtCrawler(db, reader, cfg, log);

                var task = crawler.Crawl();
                try {
                    task.GetAwaiter().GetResult();
                }
                catch (Exception e) {
                    log.Error("Crawl failed: {e}", e);
                }
            }
        }

        static class VideoIds {
            public static string RobWrightKavanaugh = "TkGaYSPSuoU";
            public static string TimPoolFalseAccusations = "NMYJ7UCHSuo";
        }
    }
}