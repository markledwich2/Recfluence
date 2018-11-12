using System;
using System.Collections.Async;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Humanizer;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace YouTubeReader {
    public class S3Store {
        public S3Store(S3Cfg cfg, StringPath basePath) {
            Cfg = cfg;
            BasePath = basePath;
            S3 = new AmazonS3Client(
                Cfg.Credentials.Name, Cfg.Credentials.Secret,
                new AmazonS3Config {
                    //RegionEndpoint = RegionEndpoint.GetBySystemName(Cfg.Region), // with parreleism, this exposes error in the library
                    ServiceURL = "https://s3.us-west-2.amazonaws.com",
                    CacheHttpClient = true,
                    //UseAccelerateEndpoint = Cfg.AcceleratedEndpoint,
                    //BufferSize = Cfg.BufferSizeBytes,
                }
            );
        }

        S3Cfg Cfg { get; }
        StringPath BasePath { get; }

        readonly AmazonS3Client S3;

        string FilePath(StringPath path) => BasePath.Add(path).WithExtension(".json.gz");

        [DebuggerHidden]
        public async Task<T> Get<T>(StringPath path) where T : class {
            GetObjectResponse response = null;
            try {
                response = await S3.GetObjectAsync(new GetObjectRequest {BucketName = Cfg.Bucket, Key = FilePath(path)});
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
                using (var zipWriter = new GZipStream(memStream, CompressionLevel.Optimal, leaveOpen:true))
                using (var tw = new StreamWriter(zipWriter, Encoding.UTF8)) {
                    JsonExtensions.DefaultSerializer.Serialize(new JsonTextWriter(tw), item);
                }

                var response = await S3.PutObjectAsync(new PutObjectRequest {
                    BucketName = Cfg.Bucket,
                    Key = FilePath(path),
                    InputStream = memStream, AutoCloseStream = false, ContentType = "application/x-gzip"
                });
            }
        }


        public async Task Save(StringPath path, FPath file) {
            var response = await S3.PutObjectAsync(new PutObjectRequest {
                BucketName = Cfg.Bucket,
                Key = BasePath.Add(path),
                FilePath = file.FullPath
            });
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
        public int BufferSizeBytes { get; set; } = (int)100.Kilobytes().Bytes;
        public bool AcceleratedEndpoint { get; set; } = true;
    }

    /// <summary>
    ///     Credentials for a user (in the format name:secret).
    ///     Be careful not to serialize this. it is not encrypted
    /// </summary>
    [TypeConverter(typeof(StringConverter<NameSecret>))]
    public sealed class NameSecret : IStringConvertableWithPattern {
        public NameSecret() { }

        public NameSecret(string name, string secret) {
            Name = name;
            Secret = secret;
        }

        public string Name { get; set; }
        public string Secret { get; set; }

        public string StringValue {
            get => $"{Name}:{Secret}";
            set {
                var tokens = value.UnJoin(':', '\\').ToQueue();
                Name = tokens.TryDequeue();
                Secret = tokens.TryDequeue();
            }
        }

        public string Pattern => @"([^:\n]+):([^:\n]+)";

        public override string ToString() => StringValue;
    }

    public class S3Collection<T> where T : class {
        public S3Collection(S3Store s3, Expression<Func<T, string>> getId, Func<string, Task<T>> create, StringPath path, bool useCache = true) {
            S3 = s3;
            GetId = getId.Compile();
            Create = create;
            Path = path;
            UseCache = useCache;
            Cache = new KeyedCollection<string, T>(getId, theadSafe: true);
        }

        S3Store S3 { get; }
        Func<T, string> GetId { get; }
        Func<string, Task<T>> Create { get; }
        StringPath Path { get; }
        bool UseCache { get; }
        IKeyedCollection<string, T> Cache { get; }

        public async Task<T> Get(string id) {
            if (UseCache) {
                var cached = Cache[id];
                if (cached != null) return cached;
            }

            var o = await S3.Get<T>(Path.Add(id));
            if (UseCache && o != null)
                Cache.Add(o);
            return o;
        }

        public async Task<T> GetOrCreate(string id) {
            if (UseCache) {
                var cached = Cache[id];
                if (cached != null) return cached;
            }

            var o = await S3.Get<T>(Path.Add(id));
            var missingFromS3 = o == null;
            if (missingFromS3)
                o = await Create(id);

            if (o == null)
                return null;

            if (missingFromS3)
                await S3.Set(Path.Add(id), o);

            if (UseCache)
                Cache.Add(o);
            return o;
        }

        public Task Set(T item) => S3.Set(Path.Add(GetId(item)), item);

       
    }
}