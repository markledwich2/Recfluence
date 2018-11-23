using System;
using System.Collections.Async;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Linq.Expressions;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.S3.Transfer;
using Humanizer;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.Security;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace YouTubeReader {

    public interface ISimpleFileStore {
        Task<T> Get<T>(StringPath path) where T : class;
        Task Set<T>(StringPath path, T item);
        Task Save(StringPath path, FPath file);
    }

    public class S3Store : ISimpleFileStore {
        public S3Store(S3Cfg cfg, StringPath basePath) {
            Cfg = cfg;
            BasePath = basePath;
            S3 = new AmazonS3Client(
                Cfg.Credentials.Name, Cfg.Credentials.Secret,
                new AmazonS3Config {
                    //RegionEndpoint = RegionEndpoint.GetBySystemName(Cfg.Region), // with parreleism, this exposes error in the library
                    ServiceURL = "https://s3.us-west-2.amazonaws.com",
                    CacheHttpClient = true,
                    Timeout = 10.Minutes()
                    //UseAccelerateEndpoint = Cfg.AcceleratedEndpoint,
                    //BufferSize = Cfg.BufferSizeBytes,
                }
            );
        }

        S3Cfg Cfg { get; }
        StringPath BasePath { get; }

        readonly AmazonS3Client S3;

        string FilePath(StringPath path) => BasePath.Add(path).WithExtension(".json.gz");
        Policy S3Policy = Policy.Handle<HttpRequestException>()
            .WaitAndRetryAsync(new[] { 1.Seconds(), 4.Seconds(), 30.Seconds() });

        [DebuggerHidden]
        public async Task<T> Get<T>(StringPath path) where T : class {
            GetObjectResponse response = null;
            try {

                response = await S3Policy.ExecuteAsync(() => 
                    S3.GetObjectAsync(new GetObjectRequest {BucketName = Cfg.Bucket, Key = FilePath(path)}));
            }
            catch (AmazonS3Exception e) {
                if (e.ErrorCode == "NoSuchBucket" || e.ErrorCode == "NotFound" || e.ErrorCode == "NoSuchKey")
                    return null;
                throw;
            }

            using (var zr = new GZipStream(response.ResponseStream, CompressionMode.Decompress))
            using (var tr = new StreamReader(zr, Encoding.UTF8)) {
                var jObject = await JObject.LoadAsync(new JsonTextReader(tr));
                var r = jObject.ToObject<T>(JsonExtensions.DefaultSerializer);
                return r;
            }
        }

        public async Task Set<T>(StringPath path, T item) {
            using (var memStream = new MemoryStream()) {
                using (var zipWriter = new GZipStream(memStream, CompressionLevel.Optimal, true))
                using (var tw = new StreamWriter(zipWriter, Encoding.UTF8)) {
                    JsonExtensions.DefaultSerializer.Serialize(new JsonTextWriter(tw), item);
                }

                var res = await S3Policy.ExecuteAsync(() => S3.PutObjectAsync(new PutObjectRequest {
                    BucketName = Cfg.Bucket,
                    Key = FilePath(path),
                    InputStream = memStream, AutoCloseStream = false, ContentType = "application/x-gzip"
                }));
            }
        }

        public async Task Save(StringPath path, FPath file) {
            var tu = new TransferUtility(S3);
            await tu.UploadAsync(file.FullPath, Cfg.Bucket, BasePath.Add(path));
        }

        public IAsyncEnumerable<ICollection<StringPath>> ListKeys(StringPath path) {
            var prefix = BasePath.Add(path);
            return new AsyncEnumerable<ICollection<StringPath>>(async yield => {
                var request = new ListObjectsV2Request {
                    BucketName = Cfg.Bucket,
                    Prefix = prefix
                };

                while (true) {
                    var response = await S3.ListObjectsV2Async(request);
                    var keys = response.S3Objects.Select(f => f.Key);
                    await yield.ReturnAsync(keys.Select(k => new StringPath(k).RelativePath(prefix).WithoutExtension()).ToList());

                    if (response.IsTruncated)
                        request.ContinuationToken = response.NextContinuationToken;
                    else
                        break;
                }
            });
        }
    }

    public sealed class S3Cfg {
        public string Bucket { get; set; }
        public string Region { get; set; }
        public NameSecret Credentials { get; set; }
    }

    public class FileCollection<T> where T : class {
        public FileCollection(ISimpleFileStore s3, Expression<Func<T, string>> getId, StringPath path, CollectionCacheType cacheType = CollectionCacheType.Memory, FPath localCacheDir = null) {
            Store = s3;
            GetId = getId.Compile();
            Path = path;
            CacheType = cacheType;
            LocalCacheDir = localCacheDir;
            Cache = new KeyedCollection<string, T>(getId, theadSafe: true);
        }

        ISimpleFileStore Store { get; }
        Func<T, string> GetId { get; }
        StringPath Path { get; }
        CollectionCacheType CacheType { get; }
        FPath LocalCacheDir { get; }
        IKeyedCollection<string, T> Cache { get; }

        T GetFromCache(string id) {
            switch (CacheType) {
                case CollectionCacheType.None:
                    return null;
                case CollectionCacheType.Memory:
                case CollectionCacheType.MemoryAndDisk:
                    var item = Cache[id];
                    if (item != null) return item;
                    if(CacheType == CollectionCacheType.MemoryAndDisk && LocalCacheDir != null) {
                        var file = GetFilePath(id);
                        if (file.Exists)
                            return file.ToObject<T>();
                    }
                    break;
            }
            return null;
        }

        void SetCache(string id, T item) {
            if (item == null) return;
            switch (CacheType) {
                case CollectionCacheType.Memory:
                case CollectionCacheType.MemoryAndDisk:
                    Cache.Add(item);
                    if (CacheType == CollectionCacheType.MemoryAndDisk && LocalCacheDir != null) {

                        var file = GetFilePath(id);

                        if (!file.Parent().Exists)
                            file.Parent().EnsureDirectoryExists();

                        item.ToJsonFile(file);
                    }
                    break;
            }
        }

        private FPath GetFilePath(string id) => LocalCacheDir.Combine(Path.Add($"{id}.json").Tokens.ToArray());

        public async Task<T> Get(string id) {
            var o = GetFromCache(id);
            if (o != null)
                return o;
            o = await Store.Get<T>(Path.Add(id));
            SetCache(id, o);
            return o;
        }

        public async Task<T> GetOrCreate(string id, Func<string, Task<T>> create) {
            var o = GetFromCache(id);
            if (o != null)
                return o;

            o = await Store.Get<T>(Path.Add(id));
            var missingFromS3 = o == null;
            if (missingFromS3)
                o = await create(id);

            if (o == null)
                return null;

            if (missingFromS3)
                await Store.Set(Path.Add(id), o);

            SetCache(id, o);
            return o;
        }

        public async Task<T> Set(T item) {
            await Store.Set(Path.Add(GetId(item)), item);
            SetCache(GetId(item), item);
            return item;
        }
    }

    public enum CollectionCacheType {
        None,
        Memory,
        MemoryAndDisk
    }
}