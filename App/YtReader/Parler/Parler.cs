using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Download;
using Google.Apis.Drive.v3;
using Humanizer;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Threading;
using YtReader.Store;
using static System.IO.FileMode;
using File = Google.Apis.Drive.v3.Data.File;

namespace YtReader; 

public class Parler {
  readonly ILogger          Log;
  readonly GoogleCfg        Cfg;
  readonly FPath            Dir;
  readonly ISimpleFileStore Db;

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
    Dir.EnsureDirectoryExists();

    async Task<FPath> Download(File f, FPath localFile) {
      if (localFile.Exists) return localFile;
      using var sw = localFile.Open(CreateNew);
      var progress = await service.Files.Get(f.Id).DownloadAsync(sw);
      while (progress.Status.In(DownloadStatus.NotStarted, DownloadStatus.Downloading))
        await 1.Seconds().Delay();
      if (progress.Status == DownloadStatus.Completed) return localFile;
      if (progress.Exception != null) {
        log.Error(progress.Exception, "error when downloading file {File}: {Message}", f.Name, progress.Exception.Message);
        return null;
      }
      log.Error("download did not complete {File}");
      return null;
    }

    await files.Files.WithIndex().BlockDo(async f => {
      var localFile = Dir.Combine(f.item.Name.Trim().Split(".").First().Replace("Copy of ", "") + ".jsonl.gz");
      var blobPath = $"parler/{folderName}/{localFile.FileName}";
      if (await Db.Exists(blobPath)) {
        log.Information("Skipping existing blob {File}", blobPath);
        return;
      }
      var downloadedFile = await Download(f.item, localFile);
      if (downloadedFile != null)
        await Db.Save(blobPath, downloadedFile, Log);
      log.Information("Moved {File} {Num}/{Total}", localFile.FileName, f.index + 1, files.Files.Count);
      localFile.Delete();
    }, parallel: 2);
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