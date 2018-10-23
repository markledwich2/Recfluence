using System;
using Serilog;
using YouTubeReader;

namespace YouTubeNetworks
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var log = Setup.CreateLogger()) {
                var cfg = Setup.Cfg();
                var reader = new YTReader(cfg);
                var crawler =new YTCrawler(Setup.MongoClient(cfg), reader, cfg, log);
                
                var task = crawler.Crawl();
                var r = task.GetAwaiter().GetResult();
                
               crawler.SaveResult(r);
            }
        }

        static class VideoIds
        {
            public static string RobWrightKavanaugh = "TkGaYSPSuoU";
            public static string TimPoolFalseAccusations = "NMYJ7UCHSuo";
        }
    }
}
