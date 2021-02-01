using System;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;
using Azure.Storage.Blobs.Models;
using Flurl.Http;
using Humanizer;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;
using static System.IO.Compression.CompressionLevel;
using static System.IO.FileMode;

namespace YtReader {
  
  
  public class Parler {
    readonly ILogger            Log;
    readonly FPath              Dir;
    readonly AzureBlobFileStore Db;

    public Parler(ILogger log, BlobStores stores) {
      Log = log;
      Db = stores.Store(DataStoreType.DbStage);
      Dir = Path.GetTempPath().AsPath().Combine("recfluence", "parler");
    }

    public async Task Load(string[] sets = null) {
      Log.Information("Starting load parler users and posts");
      await new[] {
          (name:"users", url:"https://zenodo.org/record/4442460/files/parler_users.zip?download=1"),
          (name:"posts", url:"https://zenodo.org/record/4442460/files/parler_data.zip?download=1")
        }
        .Where(p => sets == null || sets.Contains(p.name))
        .BlockAction(p => LoadParlerArchive(p.name, p.url), 2);
    }

    async Task LoadParlerArchive(string name, string url) {
      Log.Information("parler - loading {Name}", name);
      
      var localDl = Dir.Combine($"{name}.zip");
      if (!localDl.Exists) {
        Log.Information("parler - starting download of {Name}", name);
        var sw = Stopwatch.StartNew();
        var (_, ex) = await url.WithTimeout(1.Hours()).DownloadFileAsync(Dir.FullPath, localDl.FullPath, bufferSize:(int)10.Megabytes().Bytes).Try();
        if (ex != null) {
          if (localDl.Exists) localDl.Delete();
          throw ex;
        }
        Log.Information("parler - completed download of {Name} in {Duration}", name, sw.HumanizeShort());
        Log.Information("downloaded files");
      }

      var extractDir = Dir.Combine(name);
      extractDir.EnsureDirectoryExists();

      FPath ExtractedFiles() => extractDir.Files("*.ndjson");
      
      if (!ExtractedFiles().Any()) {
        localDl.ExtractZip(extractDir);
        Log.Information("extracted files");
      }

      await extractDir.Files("*.ndjson").BlockAction(async f => {
        var sr = f.Open(Open);

        var jsonlGzPath = f.Parent().Combine($"{f.FileNameWithoutExtension}.jsonl.gz");
        if (!jsonlGzPath.Exists) {
          using var sw = jsonlGzPath.Open(CreateNew);
          using var gz = new GZipStream(sw, Optimal); // read and compress the stream
          await sr.CopyToAsync(gz);
        }

        var blobPath = $"parler/{name}/{jsonlGzPath.FileName}";
        if (!await Db.Exists(blobPath))
          await Db.Save(blobPath, jsonlGzPath, log: Log);
      }, parallel: 4);
      Log.Information("parler - completed loading {Name}", name);
    }
  }
}