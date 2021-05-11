using System.Threading.Tasks;
using Nest;
using Serilog;
using YtReader.Db;
using YtReader.Store;

namespace YtReader {
  public record MediaDownload(YtStore Store, SnowflakeConnectionProvider Sf) {
    public async Task DownloadMissing(ILogger log, Platform? platform = null) {
      using var db = await Sf.Open(log);
      //var videos = db.Query<(string Id, string url)>("");
    }
  }
}