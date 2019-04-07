using System;
using System.Collections.Async;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
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
using Polly.Retry;
using SysExtensions.Fluent.IO;
using SysExtensions.Security;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace YtReader {
  public class S3Store : ISimpleFileStore {
    readonly AmazonS3Client S3;
    readonly AsyncRetryPolicy S3Policy = Policy.Handle<HttpRequestException>()
      .WaitAndRetryAsync(new[] {1.Seconds(), 4.Seconds(), 30.Seconds()});

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
        using (var tw = new StreamWriter(zipWriter, Encoding.UTF8))
          JsonExtensions.DefaultSerializer.Serialize(new JsonTextWriter(tw), item);

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

    public Task Save(StringPath path, Stream contents) => throw new NotImplementedException();

    string FilePath(StringPath path) => BasePath.Add(path).WithExtension(".json.gz");

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
}