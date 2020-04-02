using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Blob;
using Newtonsoft.Json;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace Mutuo.Etl.Blob {
  public class AzureBlobFileStore : ISimpleFileStore {
    public AzureBlobFileStore(Uri sas, StringPath pathSansContainer = null) : this(pathSansContainer) => Container = new CloudBlobContainer(sas);

    public AzureBlobFileStore(string cs, StringPath path) : this(path) {
      var storage = CloudStorageAccount.Parse(cs);
      var client = new CloudBlobClient(storage.BlobEndpoint, storage.Credentials);
      var containerName = path.Tokens.FirstOrDefault() ?? throw new InvalidOperationException("path needs to at least have a container");
      Container = client.GetContainerReference(containerName);
    }

    AzureBlobFileStore(StringPath path) {
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

    public async Task<Stream> Load(StringPath path) {
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

    public async Task<T> Get<T>(StringPath path, bool zip = true) where T : class {
      var fullPath = path.WithJsonExtention(zip);
      var blob = BlobRef(fullPath);

      await using var mem = new MemoryStream();
      try {
        await blob.DownloadToStreamAsync(mem);
      }
      catch (Exception) {
        var exists = await blob.ExistsAsync();
        if (!exists) return null;
        throw;
      }
      mem.Position = 0;

      Stream stream = mem;
      if (zip) {
        await using var zr = new GZipStream(mem, CompressionMode.Decompress);
        stream = zr;
      }
      using var tr = new StreamReader(stream, Encoding.UTF8);
      return JsonExtensions.DefaultSerializer.Deserialize<T>(tr);
    }

    public CloudBlockBlob BlobRef(StringPath path) => Container.GetBlockBlobReference(BasePathSansContainer.Add(path));

    /// <summary>Serializes item into the object store</summary>
    /// <param name="path">The path to the object (no extensions)</param>
    /// <param name="item"></param>
    /// <param name="zip"></param>
    public async Task Set<T>(StringPath path, T item, bool zip) {
      await using var memStream = new MemoryStream();
      Stream writer = memStream;
      if (zip) {
        await using var zipWriter = new GZipStream(memStream, CompressionLevel.Optimal, true);
        writer = zipWriter;
      }
      await using (var tw = new StreamWriter(writer, Encoding.UTF8)) JsonExtensions.DefaultSerializer.Serialize(new JsonTextWriter(tw), item);

      var fullPath = path.WithJsonExtention(zip);
      memStream.Seek(0, SeekOrigin.Begin);
      var blob = BlobRef(fullPath);
      AutoPopulateProps(fullPath, blob);
      await blob.UploadFromStreamAsync(memStream);
    }

    public async Task Save(StringPath path, FPath file) {
      var blob = BlobRef(path);
      AutoPopulateProps(path, blob);
      await blob.UploadFromFileAsync(file.FullPath);
    }

    public async Task Save(StringPath path, Stream contents) {
      var blob = BlobRef(path);
      AutoPopulateProps(path, blob);
      await blob.UploadFromStreamAsync(contents);
    }

    public async Task<Stream> OpenForWrite(StringPath path) {
      var blob = BlobRef(path);
      await blob.DeleteIfExistsAsync();
      AutoPopulateProps(path, blob);
      return await blob.OpenWriteAsync();
    }

    public async IAsyncEnumerable<IReadOnlyCollection<FileListItem>> List(StringPath path = null, bool allDirectories = false) {
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

    public async Task<bool> Delete(StringPath path) {
      var blob = BlobRef(path);
      return await blob.DeleteIfExistsAsync();
    }
  }

  public class FileListItem {
    public StringPath      Path     { get; set; }
    public DateTimeOffset? Modified { get; set; }
  }
}