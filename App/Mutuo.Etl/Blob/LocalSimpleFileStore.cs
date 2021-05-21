using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Serilog;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Net;
using SysExtensions.Text;

namespace Mutuo.Etl.Blob {
  public class LocalSimpleFileStore : ISimpleFileStore {
    readonly FPath Dir;
    public LocalSimpleFileStore(FPath dir) => Dir = dir;

    static void InitDirIfRequired() { }

    public StringPath BasePath => "";

    public Task Save(StringPath path, FPath file, ILogger log = null) {
      file.EnsureDirectoryExists();
      file.Copy(Dir.Combine(path));
      return default;
    }

    public async Task Save(StringPath path, Stream contents, ILogger log = null) {
      var file = Path(path);
      file.EnsureDirectoryExists();
      using var ws = file.Open(FileMode.Create);
      await contents.CopyToAsync(ws);
    }

    FPath Path(StringPath path) => Dir.Combine(path.Tokens.ToArray());

    public Task<Stream> Load(StringPath path, ILogger log = null) {
      Stream s = Path(path).Open(FileMode.Open);
      return Task.FromResult(s);
    }

    public Task LoadToFile(StringPath path, FPath file, ILogger log = null) => throw new NotImplementedException();

#pragma warning disable 1998
    public async IAsyncEnumerable<IReadOnlyCollection<FileListItem>> List(StringPath path, bool allDirectories = false, ILogger log = null) {
#pragma warning restore 1998
      var files = Path(path).Files("*", allDirectories);
      var res = files.Select(AsListItem).ToArray();
      yield return res;
    }

    FileListItem AsListItem(FPath f) => new(new StringPath(f.FullPath).RelativePath(new(Dir.FullPath)), f.LastWriteTime());

    public Task<bool> Delete(StringPath path, ILogger log = null) {
      var p = Path(path);
      var exists = p.Exists;
      if (exists)
        p.Delete();
      return Task.FromResult(exists);
    }

    public Task<Stream> OpenForWrite(StringPath path, ILogger log = null) {
      var p = Path(path);
      p.EnsureDirectoryExists();
      var s = (Stream) p.Open(FileMode.Create, FileAccess.Write);
      return Task.FromResult(s);
    }

    public Task<FileListItem> Info(StringPath path) => Task.FromResult(AsListItem(Path(path)));

    public Uri Url(StringPath path) => $"file://{Path(path).FullPath}".AsUri();
    public Task<bool> Exists(StringPath path) => throw new NotImplementedException();
  }
}