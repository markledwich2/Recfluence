using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Humanizer;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
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

    public async Task<T> Get<T>(StringPath path) where T : class {
      var blob = BlobRef(path.WithExtension(".json.gz"));

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
        var blob = BlobRef(path.WithExtension(".json.gz"));
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

    public async Task<ICollection<StringPath>> List(StringPath path) {
      var list = new List<StringPath>();
      BlobContinuationToken token = null;
      do {
        var res = await Container.ListBlobsSegmentedAsync(BasePathSansContainer.Add(path) + "/", null);
        list.AddRange(res.Results.Select(r => new StringPath(r.Uri.AbsolutePath).RelativePath(BasePath)));
        token = res.ContinuationToken;
      } while (token != null);
      return list;
    }

    Uri BlobUri(StringPath path) => Storage.BlobUri(BasePath.Add(path));
  }

  [XmlRoot("EnumerationResults")]
  public class ListBlobsResponse {
    public string Prefix { get; set; }
    public string Delimiter { get; set; }

    public List<Blob> Blobs { get; set; }

    public string NextMarker { get; set; }

    public class Blob {
      public string Name { get; set; }
      public bool Deleted { get; set; }
      public string BlobType { get; set; }
    }

    public class BlobPrefix {
      public string Name { get; set; }
    }
  }
}