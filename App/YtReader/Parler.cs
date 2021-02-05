using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Models;
using Flurl.Http;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Download;
using Google.Apis.Drive.v3;
using Google.Apis.Services;
using Google.Apis.Util.Store;
using Humanizer;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;
using static System.IO.Compression.CompressionLevel;
using static System.IO.FileMode;
using File = Google.Apis.Drive.v3.Data.File;

namespace YtReader {
  
  
  public class Parler {
    readonly ILogger            Log;
    readonly GoogleCfg          Cfg;
    readonly FPath              Dir;
    readonly AzureBlobFileStore Db;

    public Parler(ILogger log, BlobStores stores, GoogleCfg Cfg) {
      Log = log;
      this.Cfg = Cfg;
      Db = stores.Store(DataStoreType.DbStage);
      Dir = Path.GetTempPath().AsPath().Combine("recfluence", "parler");
    }

    public async Task LoadFromGoogleDrive(string folderId, string folderName, ILogger log) {
      var creds = GoogleCredential.FromJson(Cfg.Creds.ToString()).CreateScoped(DriveService.Scope.DriveReadonly);
      var service = new DriveService(new() {
        HttpClientInitializer = creds,
        ApplicationName = "recfluence"
      });
      var list = service.Files.List();
      list.Q = $"'{folderId}' in parents";
      list.PageSize = 1000;
      var files = await list.ExecuteAsync();
      
      async Task<FPath> Download(File f) {
        var file = Dir.Combine(f.Name.Replace("Copy of ", "").Trim());
        if (file.Exists) return file;
        
        using var sw = file.Open(CreateNew);
        var progress = await service.Files.Get(f.Id).DownloadAsync(sw);
        while (progress.Status.In(DownloadStatus.NotStarted, DownloadStatus.Downloading)) {
          await 1.Seconds().Delay();
        }
        return file;
      }
      
      async Task<FPath> Upload(FPath f) {
        var blobPath = $"parler/{folderName}/{f.FileNameWithoutExtension}.jsonl.gz";
        if (!await Db.Exists(blobPath))
          await Db.Save(blobPath, f, Log);
        return f;
      }

      await files.Files.WithIndex().BlockAction(async f => {
        var localFile = await Upload(await Download(f.item));
        log.Information("Moved {File} {Num}/{Total}", localFile.FileName, f.index+1, files.Files.Count);
      }, 4);
      Log.Information("parler - completed loading {Name}", folderName);
    }
    
    /*public async Task Load(string[] sets = null) {
  Log.Information("Starting load parler users and posts");
  await new[] {
      (name:"users", url:"https://zenodo.org/record/4442460/files/parler_users.zip?download=1"),
      (name:"posts", url:"https://zenodo.org/record/4442460/files/parler_data.zip?download=1")
    }
    .Where(p => sets == null || sets.Contains(p.name))
    .BlockAction(p => LoadParlerArchive(p.name, p.url), 2);
}*/
  }
}