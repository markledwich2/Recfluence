using System.IO;
using System.IO.Compression;
using Humanizer.Bytes;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using static System.Array;

namespace Mutuo.Etl.Blob;

public enum ColMeta {
  Distinct,
  MinMax
}

public record IndexCol {
  public string    Name      { get; init; }
  public string    DbName    { get; init; }
  public bool      InIndex   { get; init; }
  public ColMeta[] ExtraMeta { get; init; }
}

public record BlobIndexResult(BlobIndexMeta Index, SPath IndexPath, SPath IndexFilesPath, SPath[] ToDelete);

public record BlobIndexWork(SPath Path, IndexCol[] Cols, IAsyncEnumerable<JObject> Rows, ByteSize Size,
  NullValueHandling NullHandling = NullValueHandling.Include, Action<JObject> OnProcessed = null);

public record BlobIndex(ISimpleFileStore Store) {
  /// <summary>Indexes into blob storage the given data. Reader needs to be ordered by the index columns.</summary>
  public async Task<BlobIndexResult> SaveIndexedJsonl(BlobIndexWork work, ILogger log, CancellationToken cancel = default) {
    var indexPath = work.Path.Add("index");
    var oldIndex = await Store.GetState<BlobIndexMeta>(indexPath).Try().Do(
      idx => idx == null ? null : idx with { RunIds = idx.RunIds ?? Empty<RunId>() },
      _ => null) ?? new() { RunIds = Empty<RunId>() };

    var runId = DateTime.UtcNow.FileSafeTimestamp();

    var colMeta = work.Cols.Select(c => (Meta: new BlobIndexColMeta {
      Name = c.Name,
      DbName = c.DbName,
      InIndex = c.InIndex,
      Distinct = c.ExtraMeta?.Contains(ColMeta.Distinct) == true ? new HashSet<string>() : null
    }, Col: c)).ToArray();


    void OnProcessed(JObject j) {
      work.OnProcessed?.Invoke(j);
      RecordColMeta(j, colMeta);
    }

    var files = (await IndexFiles(work.Rows, work.Cols, work.Size, work.NullHandling, log, OnProcessed)
      .Select((b, i) => (b.first, b.last, b.stream, i))
      .BlockDo(async b => {
        if (cancel.IsCancellationRequested) return null;
        var file = new SPath($"{runId}/{b.i:000000}.{JValueString(b.first)}.{JValueString(b.last)}.jsonl.gz");
        await Store.Save(work.Path.Add(file), b.stream);
        return new BlobIndexFileMeta {
          File = file,
          First = b.first,
          Last = b.last
        };
      }, parallel: 16, cancel: cancel).ToListAsync()).NotNull();

    var toDelete = oldIndex.RunIds
      .OrderByDescending(r => r.Created).Skip(1) // leave latest
      .Where(r => DateTime.UtcNow - r.Created > 12.Hours()) // 1 older than latest if its old enough
      .Select(r => (Path: work.Path.Add(r.Id), r.Id)).ToArray();

    var index = new BlobIndexMeta {
      KeyFiles = files.ToArray(),
      RunIds = oldIndex.RunIds.Where(r => toDelete.All(d => d.Id != r.Id))
        .Concat(new RunId { Id = runId, Created = DateTime.UtcNow }).ToArray(),
      Cols = colMeta.Select(c => c.Meta).ToArray()
    };

    var indexFilesPath = work.Path.Add(runId);
    log.Information("Completed saving index files in {Index}. Not committed yet.", indexFilesPath);
    return new(index, indexPath, indexFilesPath, toDelete.Select(d => d.Path).ToArray());
  }

  public async Task CommitIndexJson(BlobIndexResult indexWork, ILogger log) {
    log.Debug("deleted expired starting - {Files}", indexWork.ToDelete);
    await indexWork.ToDelete.BlockDo(async deletePath => {
      await Store.List(deletePath).SelectMany().BlockDo(f => Store.Delete(f.Path, log), parallel: 16).ToListAsync();
    });
    log.Debug("deleted expired complete - {Files}", indexWork.ToDelete);
    await Store.SetState(indexWork.IndexPath, indexWork.Index);
    log.Information("Committed index {Index}", indexWork.IndexPath);
  }

  string JValueString(JObject j) => j.JStringValues().Join("|");

  async IAsyncEnumerable<(Stream stream, JObject first, JObject last)> IndexFiles(IAsyncEnumerable<JObject> rows, IndexCol[] cols, ByteSize size,
    NullValueHandling nullHandling, ILogger log, Action<JObject> onProcessed) {
    var hasRows = true;
    var rowEnum = rows.GetAsyncEnumerator();
    while (hasRows) {
      var memStream = new MemoryStream();
      JObject first = null;
      JObject last = null;
      using (var gz = new GZipStream(memStream, CompressionLevel.Optimal, leaveOpen: true))
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
          if (nullHandling == NullValueHandling.Ignore)
            r = r.JCloneProps(r.Properties().Where(p => p.Value.Type != JTokenType.Null).Select(p => p.Name).ToArray());
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

  static void RecordColMeta(JObject r, (BlobIndexColMeta Meta, IndexCol Col)[] cols) {
    foreach (var (m, c) in cols) {
      if (c.ExtraMeta.Contains(ColMeta.Distinct)) {
        var s = r[m.Name]?.Value<string>();
        if (s != null)
          m.Distinct.Add(s);
      }

      if (c.ExtraMeta.Contains(ColMeta.MinMax))
        if (r[m.Name] is JValue { Value: IComparable v } j) {
          if (m.Min == null || v.CompareTo(m.Min?.Value) < 0) m.Min = j;
          if (v.CompareTo(m.Max?.Value) > 0) m.Max = j;
        }
    }
  }
}

public static class BlobIndexEx {
  public static string[] DbNames(this IEnumerable<IndexCol> cols) => cols.Where(c => c.InIndex).Select(c => c.DbName).ToArray();
  public static string[] Names(this IEnumerable<IndexCol> cols) => cols.Where(c => c.InIndex).Select(c => c.Name).ToArray();

  public static JObject JCloneProps(this JObject j, params string[] props) {
    var k = new JObject();
    foreach (var p in props.NotNull())
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

public record BlobIndexMeta {
  public BlobIndexFileMeta[] KeyFiles { get; init; }
  public RunId[]             RunIds   { get; init; }
  public BlobIndexColMeta[]  Cols     { get; init; }
}

public record BlobIndexColMeta {
  public string          Name     { get; init; }
  public string          DbName   { get; init; }
  public bool            InIndex  { get; set; }
  public HashSet<string> Distinct { get; init; }
  public JValue          Min      { get; set; }
  public JValue          Max      { get; set; }
}

public record RunId {
  public string   Id      { get; init; }
  public DateTime Created { get; init; }
}

public record BlobIndexFileMeta {
  public string             File  { get; init; }
  public JObject            First { get; init; }
  public JObject            Last  { get; init; }
  public BlobIndexColMeta[] Cols  { get; init; }
}