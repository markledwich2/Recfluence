using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Humanizer.Bytes;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Blob {
  public class BlobIndex {
    readonly ISimpleFileStore Store;
    public BlobIndex(ISimpleFileStore store) => Store = store;

    /// <summary>Indexes into blob storage the given data. Reader needs to be ordered by the index columns.</summary>
    public async Task<BlobIndexMeta> SaveIndexedJsonl(StringPath path, IEnumerator<JObject> rows, string[] indexNames, 
      ByteSize size, ILogger log, Action<JObject> onProcessed = null) {
      var runId = DateTime.UtcNow.FileSafeTimestamp();
      var files = await IndexFiles(rows, indexNames, size, log, onProcessed)
        .Select((b, i) => (b.first, b.last, b.stream, i))
        .BlockTrans(async b => {
          var file = new StringPath($"{runId}/{b.i:000000}.{JValueString(b.first)}.{JValueString(b.last)}.jsonl.gz");
          await Store.Save(path.Add(file), b.stream);
          return new BlobIndexFileMeta {
            File = file,
            First = b.first,
            Last = b.last
          };
        }, parallel: 4).ToListAsync();
      var index = new BlobIndexMeta {KeyFiles = files.ToArray()};
      await Store.Set(path.Add("index"), index);
      return index;
    }

    string JValueString(JObject j) => j.JStringValues().Join("|");

    async IAsyncEnumerable<(Stream stream, JObject first, JObject last)> IndexFiles(IEnumerator<JObject> rows, string[] indexNames, ByteSize size, ILogger log,
      Action<JObject> onProcessed) {
      
      var hasRows = true;
      while (hasRows) {
        var memStream = new MemoryStream();
        JObject first = null;
        JObject last = null;
        using (var gz = new GZipStream(memStream, CompressionLevel.Optimal, true))
        using (var tw = new StreamWriter(gz))
        using (var jw = new JsonTextWriter(tw) {
          Formatting = Formatting.None
        })
          while (true) {
            hasRows = rows.MoveNext();
            if (!hasRows || rows.Current == null) break;
            var r = rows.Current;
            first ??= r;
            last = r;

            r.WriteTo(jw);
            await tw.WriteLineAsync();
            onProcessed?.Invoke(r);

            if (memStream.Position > size.Bytes)
              break;
          }
        memStream.Seek(0, SeekOrigin.Begin);
        yield return (memStream, JCopy(first), JCopy(last));
      }

      JObject JCopy(JObject j) => j.JCloneProps(indexNames);
    }
  }

  public static class BlobIndexEx {

    public static JObject JCloneProps(this JObject j, params string[] props) {
      var k = new JObject();
      foreach (var p in props)
        k[p] = j[p];
      return k;
    }
    
    public static IEnumerable<string> JStringValues(this JObject j, params string[] props) {
      return j.Properties()
        .Where(p => props.None() || props.Contains(p.Name))
        .Select(p => p.Value.Value<string>());
    }
  }

  public class BlobIndexMeta {
    public BlobIndexFileMeta[] KeyFiles { get; set; }
  }

 public class BlobIndexFileMeta {
    public string  File  { get; set; }
    public JObject First { get; set; }
    public JObject Last  { get; set; }
  }
}