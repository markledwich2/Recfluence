using System;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SysExtensions.Fluent.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace YouTubeReader {
    public class AzureBlobFileStore : ISimpleFileStore {
        public string ContainerName { get; }
        public StringPath BasePath { get; }
        public CloudBlobClient Client { get; }

        public AzureBlobFileStore(string cs, StringPath path) {
            ContainerName = path.Tokens.FirstOrDefault() ?? throw new InvalidOperationException("path needs to at least have a container");
            BasePath = new StringPath(path.Tokens.Skip(1));
            Client = CloudStorageAccount.Parse(cs).CreateCloudBlobClient();
        }

        public async Task<T> Get<T>(StringPath path) where T : class {
            var blob = Blob(path.WithExtension(".json.gz"));
            if (!await blob.ExistsAsync())
                return null;
            var stream = await blob.OpenReadAsync();
            using (var zr = new GZipStream(stream, CompressionMode.Decompress))
            using (var tr = new StreamReader(zr, Encoding.UTF8)) {
                var jObject = await JObject.LoadAsync(new JsonTextReader(tr));
                var r = jObject.ToObject<T>(JsonExtensions.DefaultSerializer);
                return r;
            }
        }

        CloudBlockBlob Blob(StringPath path) {
            var container = Container();
            var blob = container.GetBlockBlobReference(BasePath.Add(path).StringValue);
            return blob;
        }

        CloudBlobContainer Container() => Client.GetContainerReference(ContainerName);

        public async Task Set<T>(StringPath path, T item) {
            using (var memStream = new MemoryStream()) {
                using (var zipWriter = new GZipStream(memStream, CompressionLevel.Optimal, true))
                using (var tw = new StreamWriter(zipWriter, Encoding.UTF8)) {
                    JsonExtensions.DefaultSerializer.Serialize(new JsonTextWriter(tw), item);
                }
                var blob = Blob(path.WithExtension(".json.gz"));
                memStream.Seek(0, SeekOrigin.Begin);
                await blob.UploadFromStreamAsync(memStream);
            }
        }

        public async Task Save(StringPath path, FPath file) {
            var blob = Blob(path);
            await blob.UploadFromFileAsync(file.FullPath);
        }
    }
}