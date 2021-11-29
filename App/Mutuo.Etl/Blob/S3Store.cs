using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Amazon;
using Amazon.S3;
using Amazon.S3.Model;
using Humanizer;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.Net;
using SysExtensions.Security;
using SysExtensions.Text;

namespace Mutuo.Etl.Blob; 

public record S3Store(S3Cfg Cfg, SPath BasePath) : ISimpleFileStore {
  readonly AmazonS3Client S3 = new(
    Cfg.Credentials.Name, Cfg.Credentials.Secret,
    new AmazonS3Config {
      RegionEndpoint = RegionEndpoint.GetBySystemName(Cfg.Region), // use to exposes a concurrency error. Gone 🤞 ?
      //ServiceURL = "https://s3.us-west-2.amazonaws.com",
      CacheHttpClient = true,
      Timeout = 10.Minutes()
    }
  );

  public async Task<bool> Exists(SPath path) => await Info(path) != null;

  public async Task Save(SPath path, FPath file, ILogger log = default) =>
    await S3.PutObjectAsync(new() {BucketName = Cfg.Bucket, FilePath = file.FullPath, Key = Key(path)});

  //var tu = new TransferUtility(S3);
  //await tu.UploadAsync(file.FullPath, Cfg.Bucket, BasePath.Add(path));
  public Task Save(SPath path, Stream contents, ILogger log = null) => throw new NotImplementedException();

  public async Task<Stream> Load(SPath path, ILogger log = null) {
    var res = await S3.GetObjectAsync(Cfg.Bucket, Key(path));
    return res.ResponseStream;
  }

  public Task LoadToFile(SPath path, FPath file, ILogger log = null) => throw new NotImplementedException();

  public Task<bool> Delete(SPath path, ILogger log = null) => throw new NotImplementedException();

  public async Task<FileListItem> Info(SPath path) {
    var (res, ex) = await S3.GetObjectMetadataAsync(Cfg.Bucket, Key(path)).Try();
    if (res != null) return new(path, res.LastModified); // todo: get size too
    if (ex is AmazonS3Exception {StatusCode: HttpStatusCode.NotFound}) return null;
    throw ex;
  }

  /// <summary>Full path</summary>
  public Uri Url(SPath path) => throw new NotImplementedException();

  public async IAsyncEnumerable<IReadOnlyCollection<FileListItem>> List(SPath path, bool allDirectories = false, ILogger log = null) {
    var prefix = Key(path);
    var request = new ListObjectsV2Request {BucketName = Cfg.Bucket, Prefix = prefix};
    while (true) {
      var response = await S3.ListObjectsV2Async(request);
      yield return response.S3Objects.Select(f => new FileListItem(new SPath(f.Key).RelativePath(prefix), f.LastModified, f.Size)).ToReadOnly();
      if (response.IsTruncated) request.ContinuationToken = response.NextContinuationToken;
      else break;
    }
  }

  public async Task Rename(SPath from, SPath to) {
    await S3.CopyObjectAsync(Cfg.Bucket, Key(from), Cfg.Bucket, Key(to));
    await S3.DeleteObjectAsync(Cfg.Bucket, Key(from));
  }

  public Task<Stream> OpenForWrite(SPath path, ILogger log = null) => throw new NotImplementedException();

  public Uri S3Uri(SPath path) => $"s3://{Cfg.Bucket}/{Key(path)}".AsUri();

  public SPath Key(SPath path) => BasePath.Add(path);
}

public sealed class S3Cfg {
  public string     Bucket      { get; set; }
  public string     Region      { get; set; }
  public NameSecret Credentials { get; set; }
}