using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Humanizer;
using Humanizer.Bytes;
using Mutuo.Etl.Blob;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;

namespace YtReader.Store {
  public class YtIndexResults {
    readonly SnowflakeConnectionProvider Sf;
    readonly BlobIndex                   BlobIndex;

    public YtIndexResults(YtStores stores, SnowflakeConnectionProvider sf) {
      Sf = sf;
      BlobIndex = new BlobIndex(stores.Store(DataStoreType.Results));
    }

    public async Task Run(IReadOnlyCollection<string> include, ILogger log, CancellationToken cancel = default) {
      var toRun = new[] {
        TopVideoRes("top_videos", rank: 1000, new[] {"period_type", "period_value"}, 50.Kilobytes()),
        TopVideoRes("top_channel_videos", rank: 50, new[] {"channel_id", "period_type", "period_value"}, 200.Kilobytes()),
      }.Where(i => include == null || include.Contains(i.Name));

      await toRun.BlockAction(async r => {
        using var con = await Sf.OpenConnection(log);
        var rows = con.QueryBlocking<dynamic>(r.Name, r.Sql)
          .Select(d => (JObject) JObject.FromObject(d))
          .Select(j => j.ToCamelCase()).GetEnumerator();

        var camelIndex = r.IndexCols.Select(i => i.ToCamelCase()).ToArray();
        await BlobIndex.SaveIndexedJsonl(StringPath.Relative("index", r.Name), rows, camelIndex, r.Size, log);
      }, cancel: cancel);
    }

    IndexResult TopVideoRes(string name, int rank, string[] index, ByteSize size) =>
      new IndexResult(name, index, $@"select video_id
     , channel_id
     , period_type
     , period_value::string period_value
     , views
     , watch_hours
     , rank() over (partition by {index.Join(",")} order by views desc) rank
from ttube_top_videos
  qualify rank<{rank}
order by {index.Join(",")}, rank", size);
  }

  class IndexResult {
    public IndexResult(string name, string[] indexCols, string sql, ByteSize size) {
      Name = name;
      IndexCols = indexCols;
      Sql = sql;
      Size = size;
    }

    public string   Name      { get;  }
    public string[] IndexCols { get;  }
    public string   Sql       { get;  }
    public ByteSize Size      { get;  }
  }
}