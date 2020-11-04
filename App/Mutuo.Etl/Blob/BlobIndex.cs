using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
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
      var indexPath = path.Add("index");
      var (oldIndex, _) = await Store.Get<BlobIndexMeta>(indexPath).Try(new BlobIndexMeta());
      oldIndex.RunIds ??= new RunId[] { };
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
        }, parallel: 16).ToListAsync();

      var toDelete = oldIndex.RunIds
        .OrderByDescending(r => r.Created).Skip(1) // leave latest
        .Where(r => DateTime.UtcNow - r.Created > 12.Hours()) // 1 older than latest if its old enough
        .Select(r => r.Id).ToArray();
      
      await toDelete.BlockAction(async id => {
        var deletePath = path.Add(id);
        await Store.List(deletePath).SelectMany().BlockTrans(f => Store.Delete(f.Path, log)).ToListAsync();
      });
      
      log.Debug("deleted expired {Files}", toDelete);
      
      var index = new BlobIndexMeta {
        KeyFiles = files.ToArray(),
        RunIds = oldIndex.RunIds.Where(r => !toDelete.Contains(r.Id))
          .Concat(new RunId { Id = runId, Created = DateTime.UtcNow }).ToArray()
      };
      await Store.Set(indexPath, index);
      
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

    public static IEnumerable<string> JStringValues(this JObject j, params string[] props) =>
      j.Properties()
        .Where(p => props.None() || props.Contains(p.Name))
        .Select(p => {
          var v = p.Value;
          if (v.Type == JTokenType.Date)
            return v.Value<DateTime?>()?.FileSafeTimestamp() ?? "";
          return v.Value<string>();
        });
  }

  public class BlobIndexMeta {
    public BlobIndexFileMeta[] KeyFiles { get; set; }
    public RunId[]             RunIds   { get; set; }
  }

  public class RunId {
    public string   Id      { get; set; }
    public DateTime Created { get; set; }
  }

  public class BlobIndexFileMeta {
    public string  File  { get; set; }
    public JObject First { get; set; }
    public JObject Last  { get; set; }
  }
}