using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Serialization;
using Humanizer;
using Microsoft.WindowsAzure.Storage;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.Net;
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
    }

    public string ContainerName { get; }

    //path including container
    public StringPath BasePath { get; }

    public StringPath BasePathSansContainer => new StringPath(BasePath.Tokens.Skip(1));

    public CloudStorageAccount Storage { get; }
    HttpClient H { get; }

    public async Task<T> Get<T>(StringPath path) where T : class {
      var req = BlobUri(path.WithExtension(".json.gz")).Get().WithBlobHeaders(Storage);
      var res = await H.SendAsync(req);
      if (res.StatusCode == HttpStatusCode.NotFound)
        return null;
      res.EnsureSuccessStatusCode();
      using (var stream = await res.Content.ReadAsStreamAsync())
      using (var zr = new GZipStream(stream, CompressionMode.Decompress))
      using (var tr = new StreamReader(zr, Encoding.UTF8)) {
        var jObject = await JObject.LoadAsync(new JsonTextReader(tr));
        var r = jObject.ToObject<T>(JsonExtensions.DefaultSerializer);
        return r;
      }
    }

    public async Task Set<T>(StringPath path, T item) {
      using (var memStream = new MemoryStream()) {
        using (var zipWriter = new GZipStream(memStream, CompressionLevel.Optimal, true))
        using (var tw = new StreamWriter(zipWriter, Encoding.UTF8))
          JsonExtensions.DefaultSerializer.Serialize(new JsonTextWriter(tw), item);

        memStream.Seek(0, SeekOrigin.Begin);
        var req = BlobUri(path.WithExtension(".json.gz")).Put().WithStreamContent(memStream).WithBlobHeaders(Storage);
        var res = await H.SendAsync(req);
        res.EnsureSuccessStatusCode();
      }
    }

    public async Task Save(StringPath path, FPath file) {
      using (var stream = File.OpenRead(file.FullPath)) {
        var req = BlobUri(path).Put().WithStreamContent(stream).WithBlobHeaders(Storage);
        var res = await H.SendAsync(req);
        res.EnsureSuccessStatusCode();
      }
    }

    public async Task Save(StringPath path, Stream contents) {
      var req = BlobUri(path).Put().WithStreamContent(contents).WithBlobHeaders(Storage);
      var res = await H.SendAsync(req);
      res.EnsureSuccessStatusCode();
    }
    
    public async Task<ICollection<StringPath>> List(StringPath path) {
      var basePath = BasePathSansContainer;
      
      var req = new UriBuilder(Storage.BlobEndpoint)
        .WithPathSegment(ContainerName)
        .WithParameter("restype", "container")
        .WithParameter("comp", "list")
        .WithParameter("prefix", basePath.Add(path) + "/")
        .WithParameter("delimiter","/")
        .Uri.Get().WithBlobHeaders(Storage);

      var res = await H.SendAsync(req);
      res.EnsureSuccessStatusCode();

      var sr = await res.ContentAsStream();
      var debugText = sr.ReadToEnd();

      var response = (ListBlobsResponse)new XmlSerializer(typeof(ListBlobsResponse)).Deserialize(new StringReader(debugText));
      if(response.NextMarker.HasValue())
        throw new NotImplementedException("paging for listing blobs not implemented");
      var blobs = response.Blobs.Select(b => new StringPath(b.Name).RelativePath(basePath)).ToList();
      return blobs;
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