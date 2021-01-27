using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
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
  public class IndexCol {
    public string Name          { get; set; }
    public bool   InIndex       { get; set; }
    public bool   WriteDistinct { get; set; }
    public string DbName        { get; set; }
  }

  public class BlobIndexResult {
    public BlobIndexResult(BlobIndexMeta index, StringPath indexPath, StringPath indexFilesPath, StringPath[] toDelete) {
      Index = index;
      IndexPath = indexPath;
      ToDelete = toDelete;
      IndexFilesPath = indexFilesPath;
    }

    public BlobIndexMeta Index     { get; set; }
    public StringPath    IndexPath { get; set; }
    public StringPath    IndexFilesPath { get; set; }
    public StringPath[]  ToDelete  { get; set; }
  }

  public class BlobIndexWork {
    public StringPath                Path        { get; }
    public IAsyncEnumerable<JObject> Rows        { get; }
    public IndexCol[]                Cols        { get; }
    public ByteSize                  Size        { get; }
    public Action<JObject>           OnProcessed { get; }

    public BlobIndexWork(StringPath path, IndexCol[] cols, IAsyncEnumerable<JObject> rows,
      ByteSize size, Action<JObject> onProcessed = null) {
      Path = path;
      Rows = rows;
      Cols = cols;
      Size = size;
      OnProcessed = onProcessed;
    }
  }

  public class BlobIndex {
    readonly ISimpleFileStore Store;
    public BlobIndex(ISimpleFileStore store) => Store = store;

    /// <summary>Indexes into blob storage the given data. Reader needs to be ordered by the index columns.</summary>
    public async Task<BlobIndexResult> SaveIndexedJsonl(BlobIndexWork work, ILogger log, CancellationToken cancel = default) {
      var indexPath = work.Path.Add("index");
      var (oldIndex, _) = await Store.Get<BlobIndexMeta>(indexPath).Try(new ());
      oldIndex.RunIds ??= new RunId[] { };
      var runId = DateTime.UtcNow.FileSafeTimestamp();

      IDictionary<string,HashSet<string>> colDistinctValues = work.Cols.Where(c => c.WriteDistinct).ToDictionary(c => c.Name, c => new HashSet<string>());

      void OnProcessed(JObject j) {
        work.OnProcessed?.Invoke(j);
        RecordColDistinct(j, colDistinctValues);
      }

      var files = (await IndexFiles(work.Rows, work.Cols, work.Size, log, OnProcessed)
        .Select((b, i) => (b.first, b.last, b.stream, i))
        .BlockTrans(async b => {
          if (cancel.IsCancellationRequested) return null;
          var file = new StringPath($"{runId}/{b.i:000000}.{JValueString(b.first)}.{JValueString(b.last)}.jsonl.gz");
          await Store.Save(work.Path.Add(file), b.stream);
          return new BlobIndexFileMeta {
            File = file,
            First = b.first,
            Last = b.last
          };
        }, parallel: 16, cancel:cancel).ToListAsync()).NotNull();

      var colMd = work.Cols
        .Where(c => c.WriteDistinct)
        .Select(c => new BlobIndexColMeta {
          Name = c.Name,
          DbName = c.DbName,
          InIndex = c.InIndex,
          Distinct = colDistinctValues.TryGet(c.Name)?.ToArray() ?? new string[]{}
        }).ToArray();

      var toDelete = oldIndex.RunIds
        .OrderByDescending(r => r.Created).Skip(1) // leave latest
        .Where(r => DateTime.UtcNow - r.Created > 12.Hours()) // 1 older than latest if its old enough
        .Select(r => (Path: work.Path.Add(r.Id), r.Id)).ToArray();

      var index = new BlobIndexMeta {
        KeyFiles = files.ToArray(),
        RunIds = oldIndex.RunIds.Where(r => toDelete.All(d => d.Id != r.Id))
          .Concat(new RunId {Id = runId, Created = DateTime.UtcNow}).ToArray(),
        Cols = colMd
      };

      var indexFilesPath = work.Path.Add(runId);
      log.Information("Completed saving index files in {Index}. Not committed yet.", indexFilesPath);
      return new (index, indexPath, indexFilesPath, toDelete.Select(d => d.Path).ToArray());
    }

    public async Task CommitIndexJson(BlobIndexResult indexWork, ILogger log) {
      await indexWork.ToDelete.BlockAction(async deletePath => {
        await Store.List(deletePath).SelectMany().BlockTrans(f => Store.Delete(f.Path, log)).ToListAsync();
      });
      log.Debug("deleted expired {Files}", indexWork.ToDelete);
      await Store.Set(indexWork.IndexPath, indexWork.Index);
      log.Information("Committed index {Index}", indexWork.IndexPath);
    }

    string JValueString(JObject j) => j.JStringValues().Join("|");

    async IAsyncEnumerable<(Stream stream, JObject first, JObject last)> IndexFiles(IAsyncEnumerable<JObject> rows, IndexCol[] cols, ByteSize size, ILogger log,
      Action<JObject> onProcessed) {
      var hasRows = true;
      var rowEnum = rows.GetAsyncEnumerator();
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
            hasRows = await rowEnum.MoveNextAsync();
            if (!hasRows || rowEnum.Current == null) break;
            var r = rowEnum.Current;
            first ??= r;
            last = r;
            r.WriteTo(jw);
            await tw.WriteLineAsync();
            onProcessed?.Invoke(r);
            if (memStream.Position > size.Bytes)
              break;
          }
        memStream.Seek(offset: 0, SeekOrigin.Begin);
        yield return (memStream, JCopy(first), JCopy(last));
      }
      
      JObject JCopy(JObject j) => j.JCloneProps(cols.Where(c => c.InIndex).Select(c => c.Name).ToArray());
    }

    static void RecordColDistinct(JObject r, IDictionary<string, HashSet<string>> colDistinctValues) {
      foreach (var (colName, values) in colDistinctValues) {
        var s = r[colName]?.Value<string>();
        if (s != null)
          values.Add(s);
      }
    }
  }

  public static class BlobIndexEx {
    public static string[] DbNames(this IEnumerable<IndexCol> cols) => cols.Where(c => c.InIndex).Select(c => c.DbName).ToArray();
    public static string[] Names(this IEnumerable<IndexCol> cols) => cols.Where(c => c.InIndex).Select(c => c.Name).ToArray();

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
    public BlobIndexColMeta[]  Cols     { get; set; }
  }

  public class BlobIndexColMeta : IndexCol {
    public string[] Distinct { get; set; }
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