using System;
using System.Collections.Async;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace YtReader {
  public class AzureBlobFileStore : ISimpleFileStore {
    public AzureBlobFileStore(string cs, StringPath path) {
      ContainerName = path.Tokens.FirstOrDefault() ?? throw new InvalidOperationException("path needs to at least have a container");
      BasePath = path;
      H = new HttpClient {
        Timeout = 10.Minutes()
      };
      Storage = CloudStorageAccount.Parse(cs);
      Client = new CloudBlobClient(Storage.BlobEndpoint, Storage.Credentials);
      Container = Client.GetContainerReference(ContainerName);
    }

    CloudBlobContainer Container { get; }
    CloudBlobClient Client { get; }
    string ContainerName { get; }

    //path including container
    public StringPath BasePath { get; }

    public StringPath BasePathSansContainer => new StringPath(BasePath.Tokens.Skip(1));

    public CloudStorageAccount Storage { get; }
    HttpClient H { get; }

    public async Task<Stream> Load(StringPath path) {
      var blob = BlobRef(path);
      var mem = new MemoryStream();
      await blob.DownloadToStreamAsync(mem);
      mem.Seek(0, SeekOrigin.Begin);
      return mem;
    }

    public async Task<T> Get<T>(StringPath path) where T : class {
      var blob = BlobRef(path);

      using (var mem = new MemoryStream()) {
        try {
          await blob.DownloadToStreamAsync(mem);
        }
        catch (Exception) {
          var exists = await blob.ExistsAsync();
          if (!exists) return default(T);
          throw;
        }

        mem.Position = 0;
        using (var zr = new GZipStream(mem, CompressionMode.Decompress))
        using (var tr = new StreamReader(zr, Encoding.UTF8)) {
          var jObject = await JObject.LoadAsync(new JsonTextReader(tr));
          var r = jObject.ToObject<T>(JsonExtensions.DefaultSerializer);
          return r;
        }
      }
    }

    CloudBlockBlob BlobRef(StringPath path) => Container.GetBlockBlobReference(BasePathSansContainer.Add(path));

    public async Task Set<T>(StringPath path, T item) {
      using (var memStream = new MemoryStream()) {
        using (var zipWriter = new GZipStream(memStream, CompressionLevel.Optimal, true))
        using (var tw = new StreamWriter(zipWriter, Encoding.UTF8))
          JsonExtensions.DefaultSerializer.Serialize(new JsonTextWriter(tw), item);

        memStream.Seek(0, SeekOrigin.Begin);
        var blob = BlobRef(path);
        await blob.UploadFromStreamAsync(memStream);
      }
    }

    public async Task Save(StringPath path, FPath file) {
      var blob = BlobRef(path);
      await blob.UploadFromFileAsync(file.FullPath);
    }

    public async Task Save(StringPath path, Stream contents) {
      var blob = BlobRef(path);
      await blob.UploadFromStreamAsync(contents);
    }

    public IAsyncEnumerable<IReadOnlyCollection<FileListItem>> List(StringPath path = null, bool allDirectories = false) =>
      new AsyncEnumerable<IReadOnlyCollection<FileListItem>>(async yield => {
        BlobContinuationToken token = null;
        do {
          var p = path != null ? BasePathSansContainer.Add(path) : BasePathSansContainer;
          var res = await Container.ListBlobsSegmentedAsync(p + "/", allDirectories, BlobListingDetails.None,
            null, token, null, null).ConfigureAwait(false);

          var items = res.Results.OfType<ICloudBlob>().Select(r =>
            new FileListItem {
              Path = new StringPath(r.Uri.AbsolutePath).RelativePath(BasePath),
              Modified = r.Properties.LastModified
            });

          await yield.ReturnAsync(items.ToReadOnly()).ConfigureAwait(false);

          token = res.ContinuationToken;
        } while (token != null);
      });

    Uri BlobUri(StringPath path) => Storage.BlobUri(BasePath.Add(path));
  }

  public class FileListItem {
    public StringPath Path { get; set; }
    public DateTimeOffset? Modified { get; set; }
  }
}