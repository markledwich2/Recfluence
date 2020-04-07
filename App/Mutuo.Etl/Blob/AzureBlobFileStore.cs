using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.Text;

namespace Mutuo.Etl.Blob {
  public class AzureBlobFileStore : ISimpleFileStore {
     ILogger Log { get; }

    public AzureBlobFileStore(Uri sas, ILogger log, StringPath pathSansContainer = null)
      : this(pathSansContainer, log) => Container = new CloudBlobContainer(sas);

    public AzureBlobFileStore(string cs, StringPath path, ILogger log) : this(path, log) {
      var storage = CloudStorageAccount.Parse(cs);
      var client = new CloudBlobClient(storage.BlobEndpoint, storage.Credentials);
      var containerName = path.Tokens.FirstOrDefault() ?? throw new InvalidOperationException("path needs to at least have a container");
      Container = client.GetContainerReference(containerName);
    }

    AzureBlobFileStore(StringPath path, ILogger log) {
      Log = log;
      H = new HttpClient {
        Timeout = 10.Minutes()
      };
      BasePath = path ?? StringPath.Emtpy;
    }

    CloudBlobContainer Container { get; }

    /// <summary>the Working directory of this storage wrapper. The first part of the bath is the container</summary>
    public StringPath BasePath { get; }

    StringPath BasePathSansContainer => new StringPath(BasePath.Tokens.Skip(1));

    //public CloudStorageAccount Storage { get; }
    HttpClient H { get; }

    public async Task<Stream> Load(StringPath path, ILogger log = null) {
      try {
        var blob = BlobRef(path);
        var mem = new MemoryStream();
        await blob.DownloadToStreamAsync(mem);
        mem.Seek(0, SeekOrigin.Begin);
        return mem;
      }
      catch (Exception ex) {
        throw new InvalidOperationException($"Unable to load blob {path}", ex);
      }
    }

    public CloudBlockBlob BlobRef(StringPath path) => Container.GetBlockBlobReference(BasePathSansContainer.Add(path));

    public async Task Save(StringPath path, FPath file, ILogger log = null) {
      log ??= Log;
      var blob = BlobRef(path);
      AutoPopulateProps(path, blob);
      await blob.UploadFromFileAsync(file.FullPath);
      log.Debug("Saved {Path}", path);
    }

    public async Task Save(StringPath path, Stream contents, ILogger log = null) {
      log ??= Log;
      var blob = BlobRef(path);
      AutoPopulateProps(path, blob);
      await blob.UploadFromStreamAsync(contents);
      log.Debug("Saved {Path}", path);
    }

    public async Task<Stream> OpenForWrite(StringPath path, ILogger log = null) {
      var blob = BlobRef(path);
      await blob.DeleteIfExistsAsync();
      AutoPopulateProps(path, blob);
      return await blob.OpenWriteAsync();
    }

    public async IAsyncEnumerable<IReadOnlyCollection<FileListItem>> List(StringPath path, bool allDirectories = false, ILogger log = null) {
      log ??= Log;
      BlobContinuationToken token = null;
      do {
        var p = path != null ? BasePathSansContainer.Add(path) : BasePathSansContainer;
        var res = await Container.ListBlobsSegmentedAsync(p + "/", allDirectories, BlobListingDetails.None,
          null, token, null, null);

        var items = res.Results.OfType<ICloudBlob>().Select(r =>
          new FileListItem {
            Path = new StringPath(r.Uri.LocalPath).RelativePath(BasePath),
            Modified = r.Properties.LastModified
          });

        yield return items.ToReadOnly();

        token = res.ContinuationToken;
      } while (token != null);
    }

    /// <summary>autoamtically work set the blob properties based on the extenions. Assumes the format ContentType[.Encoding]
    ///   (e.g. csv.gz or csv)</summary>
    static void AutoPopulateProps(StringPath path, CloudBlockBlob blob) {
      var ext = new Stack<string>(path.Extensions);

      if (ext.Peek().In("gz", "gzip")) {
        ext.Pop(); // pop so we can work at the content type appropreately
        blob.Properties.ContentEncoding = "gzip";
      }

      if (ext.TryPop(out var ex))
        blob.Properties.ContentType = ex switch {
          "csv" => "text/css",
          "json" => "application/json",
          _ => null
        };
    }

    public async Task<bool> Delete(StringPath path, ILogger log = null) {
      var blob = BlobRef(path);
      return await blob.DeleteIfExistsAsync();
    }
  }

  public class FileListItem {
    public StringPath      Path     { get; set; }
    public DateTimeOffset? Modified { get; set; }
  }
}