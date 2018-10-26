using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SysExtensions.Fluent.IO;
using SysExtensions.Serialization;
using YouTubeReader;

namespace YouTubeReaderTests
{
    [TestClass]
    public class YTReaderTests
    {
        [TestMethod]
        public async Task SaveTrendingCsvTest()
        {
            //var yt = new YTReader();
            //await yt.SaveTrendingCsv();
        }

        [TestMethod]
        public void TestSerializer() {

            var path2 = new FPath() {StringValue = @".\teting\one\two"};


            var cfg = Setup.LoadCfg();
            var json = cfg.ToJson();
            var cfg2 = json.ToObject<Cfg>();

        }
      
    }
}
