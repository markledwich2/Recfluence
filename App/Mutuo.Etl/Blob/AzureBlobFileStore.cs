using System.IO;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;

namespace Mutuo.Etl.Blob;

public class AzureBlobFileStore : ISimpleFileStore {
  readonly string ContainerName;
  readonly string Cs;

  public AzureBlobFileStore(Uri sas, ILogger log, SPath pathSansContainer = null)
    : this(pathSansContainer, log) => Container = new(sas);

  public AzureBlobFileStore(string cs, SPath path, ILogger log) : this(path, log) {
    Cs = cs;
    ContainerName = path?.Tokens.FirstOrDefault() ?? throw new InvalidOperationException("path needs to be provided and start with a container name");
    var storage = new BlobServiceClient(cs);
    Container = storage.GetBlobContainerClient(ContainerName);
  }

  AzureBlobFileStore(SPath path, ILogger log) {
    Log = log;
    H = new() {
      Timeout = 10.Minutes()
    };
    BasePath = path ?? SPath.Emtpy;
  }

  ILogger                    Log       { get; }
  public BlobContainerClient Container { get; }

  //public CloudStorageAccount Storage { get; }
  HttpClient H { get; }

  public Uri ContainerUrl => Container.Uri;

  /// <summary>the Working directory of this storage wrapper. The first part of the path is the container</summary>
  public SPath BasePath { get; }

  public async Task<Stream> Load(SPath path, ILogger log = null) {
    var blob = BlobClient(path);
    try {
      var mem = new MemoryStream();
      await blob.DownloadToAsync(mem);
      mem.Seek(offset: 0, SeekOrigin.Begin);
      return mem;
    }
    catch (Exception ex) {
      throw new InvalidOperationException($"Unable to load blob {blob.Uri}", ex);
    }
  }

  public async Task LoadToFile(SPath path, FPath file, ILogger log = null) {
    var blob = BlobClient(path);
    await blob.DownloadToAsync(file.FullPath).WithWrappedException($"Unable to load blob {blob.Uri}");
  }

  public Task Save(SPath path, FPath file, ILogger log = null) => Save(path, file, headers: null, log: log);

  public async Task Save(SPath path, Stream contents, ILogger log = null) {
    log ??= Log;
    var blob = BlobClient(path);
    await blob.UploadAsync(contents, DefaultProperties(path));
    log?.Debug("saved {Path}", blob.Uri);
  }

  /// <summary>Gets metadata for the given file. Returns null if it doesn't exist</summary>
  public async Task<FileListItem> Info(SPath path) {
    var blob = BlobClient(path);
    try {
      var props = (await blob.GetPropertiesAsync()).Value;
      return new(path, props.LastModified, props.ContentLength);
    }
    catch (RequestFailedException e) {
      if (e.Status == 404)
        return null;
      throw;
    }
  }

  public async Task<bool> Exists(SPath path) {
    var blob = BlobClient(path);
    return await blob.ExistsAsync();
  }

  public Uri Url(SPath path) => BlobClient(path).Uri;

  public async IAsyncEnumerable<IReadOnlyCollection<FileListItem>> List(SPath path, bool allDirectories = false) {
    var p = (path != null ? this.BasePathSansContainer().Add(path) : this.BasePathSansContainer())
      + (allDirectories ? "" : "/"); // if we are searching hierarchically ensure it ends in a "/", otherwise allow partial names
    if (allDirectories)
      await foreach (var page in Container.GetBlobsAsync(BlobTraits.All, BlobStates.None, p).AsPages())
        yield return page.Values.Select(ToFileItem).ToArray();
    else
      await foreach (var page in Container.GetBlobsByHierarchyAsync(BlobTraits.All, BlobStates.None, "/", p).AsPages())
        yield return page.Values.Where(b => b.IsBlob).Select(b => ToFileItem(b.Blob)).ToArray();
  }

  public async Task<bool> Delete(SPath path, ILogger log = null) {
    var blob = BlobClient(path);
    return await blob.DeleteIfExistsAsync();
  }

  public CloudBlobContainer LegacyContainer() {
    var storage = CloudStorageAccount.Parse(Cs);
    var client = new CloudBlobClient(storage.BlobEndpoint, storage.Credentials);
    return client.GetContainerReference(ContainerName);
  }

  public async Task Save(SPath path, FPath file, BlobHttpHeaders headers = null, AccessTier? tier = null, ILogger log = null) {
    log ??= Log;
    var blob = BlobClient(path);

    var mergedHeaders = DefaultProperties(file);
    if (headers != null) mergedHeaders = mergedHeaders.JsonMerge(headers);

    await blob.UploadAsync(file.FullPath, mergedHeaders);

    if (tier.HasValue)
      await blob.SetAccessTierAsync(tier.Value);
    log.Debug("Saved {Path}", blob.Uri);
  }

  public IAsyncEnumerable<SPath> ListDirs(SPath path) =>
    Container.GetBlobsByHierarchyAsync(delimiter: "/", prefix: path).Where(b => b.IsPrefix)
      .Select(b => new SPath(b.Prefix).RelativePath(this.BasePathSansContainer()));

  //FileListItem ToFileItem(BlobHierarchyItem b) => new(new StringPath(b.IsBlob).RelativePath(BasePath), b.Properties.LastModified, b.Properties.ContentLength);
  FileListItem ToFileItem(BlobItem b) {
    FileListItem res = new(new SPath(b.Name).RelativePath(this.BasePathSansContainer()), b.Properties.LastModified, b.Properties.ContentLength);
    return res;
  }

  string ContainerRelativePath(SPath path = null) => path == null ? this.BasePathSansContainer() : this.BasePathSansContainer().Add(path);

  public BlobClient BlobClient(SPath path = null) => Container.GetBlobClient(ContainerRelativePath(path));

  static BlobHttpHeaders DefaultProperties(FPath path) => DefaultProperties(path.ToStringPath());

  /// <summary>autoamtically work set the blob properties based on the extenions. Assumes the format ContentType[.Encoding]
  ///   (e.g. csv.gz or csv)</summary>
  static BlobHttpHeaders DefaultProperties(SPath path) {
    var ext = new Stack<string>(path.Extensions);

    var headers = new BlobHttpHeaders();

    if (ext.Count > 0 && ext.Peek().In("gz", "gzip")) {
      ext.Pop(); // pop so we can work at the content type appropreately
      headers.ContentEncoding = "gzip";
    }

    if (ext.TryPop(out var ex))
      headers.ContentType = ex switch {
        "csv" => "text/css",
        "json" => "application/json",
        "jsonl" => "application/json",
        _ => null
      };

    return headers;
  }
}

public record FileListItem(SPath Path, DateTimeOffset? Modified = null, long? Bytes = null) { }