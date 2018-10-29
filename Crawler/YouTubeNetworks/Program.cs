using YouTubeReader;

namespace YouTubeNetworks {
    class Program {
        static void Main(string[] args) {
            using (var log = Setup.CreateLogger()) {
                var cfg = Setup.LoadCfg(log);
                var reader = new YTReader(cfg);
                var db = Setup.Db(cfg);
                var crawler = new YTCrawler(db, reader, cfg, log);

                var task = crawler.Crawl();
                var r = task.GetAwaiter().GetResult();
            }
        }

        static class VideoIds {
            public static string RobWrightKavanaugh = "TkGaYSPSuoU";
            public static string TimPoolFalseAccusations = "NMYJ7UCHSuo";
        }
    }
}