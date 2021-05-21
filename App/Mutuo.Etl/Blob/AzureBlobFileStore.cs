using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Humanizer;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace Mutuo.Etl.Blob {
  public class AzureBlobFileStore : ISimpleFileStore {
    readonly string            Cs;
    readonly string            ContainerName;
    ILogger                    Log       { get; }
    public BlobContainerClient Container { get; }

    public AzureBlobFileStore(Uri sas, ILogger log, StringPath pathSansContainer = null)
      : this(pathSansContainer, log) => Container = new(sas);

    public AzureBlobFileStore(string cs, StringPath path, ILogger log) : this(path, log) {
      Cs = cs;
      ContainerName = path?.Tokens.FirstOrDefault() ?? throw new InvalidOperationException("path needs to be provided and start with a container name");
      var storage = new BlobServiceClient(cs);
      Container = storage.GetBlobContainerClient(ContainerName);
    }

    AzureBlobFileStore(StringPath path, ILogger log) {
      Log = log;
      H = new() {
        Timeout = 10.Minutes()
      };
      BasePath = path ?? StringPath.Emtpy;
    }

    public CloudBlobContainer LegacyContainer() {
      var storage = CloudStorageAccount.Parse(Cs);
      var client = new CloudBlobClient(storage.BlobEndpoint, storage.Credentials);
      return client.GetContainerReference(ContainerName);
    }

    /// <summary>the Working directory of this storage wrapper. The first part of the path is the container</summary>
    public StringPath BasePath { get; }

    //public CloudStorageAccount Storage { get; }
    HttpClient H { get; }

    public async Task<Stream> Load(StringPath path, ILogger log = null) {
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

    public async Task LoadToFile(StringPath path, FPath file, ILogger log = null) {
      var blob = BlobClient(path);
      await blob.DownloadToAsync(file.FullPath).WithWrappedException($"Unable to load blob {blob.Uri}");
    }

    public Task Save(StringPath path, FPath file, ILogger log = null) => Save(path, file, headers: null, log: log);

    public async Task Save(StringPath path, FPath file, BlobHttpHeaders headers = null, AccessTier? tier = null, ILogger log = null) {
      log ??= Log;
      var blob = BlobClient(path);

      var mergedHeaders = DefaultProperties(file);
      if (headers != null) mergedHeaders = mergedHeaders.JsonMerge(headers);

      await blob.UploadAsync(file.FullPath, mergedHeaders);

      if (tier.HasValue)
        await blob.SetAccessTierAsync(tier.Value);
      log.Debug("Saved {Path}", blob.Uri);
    }

    public async Task Save(StringPath path, Stream contents, ILogger log = null) {
      log ??= Log;
      var blob = BlobClient(path);
      await blob.UploadAsync(contents, DefaultProperties(path));
      log.Debug("Saved {Path}", blob.Uri);
    }

    /// <summary>Gets metadata for the given file. Returns null if it doesn't exist</summary>
    public async Task<FileListItem> Info(StringPath path) {
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

    public async Task<bool> Exists(StringPath path) {
      var blob = BlobClient(path);
      return await blob.ExistsAsync();
    }

    public Uri ContainerUrl => Container.Uri;
    public Uri Url(StringPath path) => BlobClient(path).Uri;

    public IAsyncEnumerable<StringPath> ListDirs(StringPath path) =>
      Container.GetBlobsByHierarchyAsync(delimiter: "/", prefix: path).Where(b => b.IsPrefix)
        .Select(b => new StringPath(b.Prefix).RelativePath(this.BasePathSansContainer()));

    public async IAsyncEnumerable<IReadOnlyCollection<FileListItem>> List(StringPath path, bool allDirectories = false, ILogger log = null) {
      log ??= Log;
      var p = (path != null ? this.BasePathSansContainer().Add(path) : this.BasePathSansContainer()) + "/";
      if (allDirectories)
        await foreach (var page in Container.GetBlobsAsync(BlobTraits.All, BlobStates.None, p).AsPages())
          yield return page.Values.Select(ToFileItem).ToArray();
      else
        await foreach (var page in Container.GetBlobsByHierarchyAsync(BlobTraits.All, BlobStates.None, "/", p).AsPages())
          yield return page.Values.Where(b => b.IsBlob).Select(b => ToFileItem(b.Blob)).ToArray();
    }

    //FileListItem ToFileItem(BlobHierarchyItem b) => new(new StringPath(b.IsBlob).RelativePath(BasePath), b.Properties.LastModified, b.Properties.ContentLength);
    FileListItem ToFileItem(BlobItem b) {
      FileListItem res = new(new StringPath(b.Name).RelativePath(this.BasePathSansContainer()), b.Properties.LastModified, b.Properties.ContentLength);
      return res;
    }

    public async Task<bool> Delete(StringPath path, ILogger log = null) {
      var blob = BlobClient(path);
      return await blob.DeleteIfExistsAsync();
    }

    string ContainerRelativePath(StringPath path = null) => path == null ? this.BasePathSansContainer() : this.BasePathSansContainer().Add(path);

    public BlobClient BlobClient(StringPath path = null) => Container.GetBlobClient(ContainerRelativePath(path));

    static BlobHttpHeaders DefaultProperties(FPath path) => DefaultProperties(path.ToStringPath());

    /// <summary>autoamtically work set the blob properties based on the extenions. Assumes the format ContentType[.Encoding]
    ///   (e.g. csv.gz or csv)</summary>
    static BlobHttpHeaders DefaultProperties(StringPath path) {
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

  public record FileListItem(StringPath Path, DateTimeOffset? Modified = null, long? Bytes = null) { }
}