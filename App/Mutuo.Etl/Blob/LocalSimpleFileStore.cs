using System.IO;

namespace Mutuo.Etl.Blob;

public class LocalSimpleFileStore : ISimpleFileStore {
  readonly FPath Dir;
  public LocalSimpleFileStore(FPath dir) => Dir = dir;

  public SPath BasePath => "";

  public Task Save(SPath path, FPath file, ILogger log = null) {
    Dir.Combine(path).EnsureDirectoryExists();
    File.Copy(file.FullPath, Dir.Combine(path).FullPath, overwrite: true);
    return Task.CompletedTask;
  }

  public async Task Save(SPath path, Stream contents, ILogger log = null) {
    var file = ToFPath(path);
    file.EnsureDirectoryExists();
    using var ws = file.Open(FileMode.Create);
    await contents.CopyToAsync(ws);
  }

  public Task<Stream> Load(SPath path, ILogger log = null) {
    Stream s = ToFPath(path).Open(FileMode.Open);
    return Task.FromResult(s);
  }

  public Task LoadToFile(SPath path, FPath file, ILogger log = null) => throw new NotImplementedException();

#pragma warning disable 1998
  public async IAsyncEnumerable<IReadOnlyCollection<FileListItem>> List(SPath path, bool allDirectories = false) {
#pragma warning restore 1998

    var dir = ToFPath(path);
    if (!dir.Exists) yield break;
    var files = dir.Files(recursive: allDirectories);
    var res = files.Select(AsListItem).ToArray();
    yield return res;
  }

  public Task<bool> Delete(SPath path, ILogger log = null) {
    var p = ToFPath(path);
    var exists = p.Exists;
    if (exists)
      p.Delete();
    return Task.FromResult(exists);
  }

  public Task<FileListItem> Info(SPath path) => Task.FromResult(AsListItem(ToFPath(path)));

  public Uri Url(SPath path) => $"file://{ToFPath(path).FullPath}".AsUri();
  public Task<bool> Exists(SPath path) => throw new NotImplementedException();

  FPath ToFPath(SPath path) => Dir.Combine(path.Tokens.ToArray());
  static SPath ToSPath(FPath f) => new(f.Tokens);

  FileListItem AsListItem(FPath f) => new(ToSPath(f).RelativePath(ToSPath(Dir)), f.LastWriteTime());

  public Task<Stream> OpenForWrite(SPath path, ILogger log = null) {
    var p = ToFPath(path);
    p.EnsureDirectoryExists();
    var s = (Stream)p.Open(FileMode.Create, FileAccess.Write);
    return Task.FromResult(s);
  }
}