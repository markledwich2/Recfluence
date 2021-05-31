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

    public SPath BasePath => "";

    public Task Save(SPath path, FPath file, ILogger log = null) {
      file.EnsureDirectoryExists();
      file.Copy(Dir.Combine(path));
      return default;
    }

    public async Task Save(SPath path, Stream contents, ILogger log = null) {
      var file = Path(path);
      file.EnsureDirectoryExists();
      using var ws = file.Open(FileMode.Create);
      await contents.CopyToAsync(ws);
    }

    public Task<Stream> Load(SPath path, ILogger log = null) {
      Stream s = Path(path).Open(FileMode.Open);
      return Task.FromResult(s);
    }

    public Task LoadToFile(SPath path, FPath file, ILogger log = null) => throw new NotImplementedException();

#pragma warning disable 1998
    public async IAsyncEnumerable<IReadOnlyCollection<FileListItem>> List(SPath path, bool allDirectories = false, ILogger log = null) {
#pragma warning restore 1998
      var files = Path(path).Files("*", allDirectories);
      var res = files.Select(AsListItem).ToArray();
      yield return res;
    }

    public Task<bool> Delete(SPath path, ILogger log = null) {
      var p = Path(path);
      var exists = p.Exists;
      if (exists)
        p.Delete();
      return Task.FromResult(exists);
    }

    public Task<FileListItem> Info(SPath path) => Task.FromResult(AsListItem(Path(path)));

    public Uri Url(SPath path) => $"file://{Path(path).FullPath}".AsUri();
    public Task<bool> Exists(SPath path) => throw new NotImplementedException();

    static void InitDirIfRequired() { }

    FPath Path(SPath path) => Dir.Combine(path.Tokens.ToArray());

    FileListItem AsListItem(FPath f) => new(new SPath(f.FullPath).RelativePath(new(Dir.FullPath)), f.LastWriteTime());

    public Task<Stream> OpenForWrite(SPath path, ILogger log = null) {
      var p = Path(path);
      p.EnsureDirectoryExists();
      var s = (Stream) p.Open(FileMode.Create, FileAccess.Write);
      return Task.FromResult(s);
    }
  }
}