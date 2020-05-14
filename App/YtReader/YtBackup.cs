using System;
using System.Diagnostics;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.Azure.Storage.DataMovement;
using Serilog;
using SysExtensions;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Store;

namespace YtReader {
  public class YtBackup {
    readonly YtStores Stores;
    readonly ILogger  Log;

    public YtBackup(YtStores stores, ILogger log) {
      Stores = stores;
      Log = log;
    }

    public async Task Backup() {
      var destPath = StringPath.Relative("db2", DateTime.UtcNow.FileSafeTimestamp());
      Log.Information("Backup {Path} - started", destPath);

      var source = Stores.Store(DataStoreType.Db);
      var dest = Stores.Store(DataStoreType.Backup);

      var sw = Stopwatch.StartNew();
      var logInterval = 5.Seconds();
      
      var context = new DirectoryTransferContext {
        ProgressHandler = new Progress<TransferStatus>(p => {
          if (sw.Elapsed < logInterval) return;
          sw.Restart();
          Log.Debug("Backup {Path} - {Size} copied: {Files} files {Skipped} skipped {Failed} failed", 
            destPath, p.BytesTransferred.Bytes().Humanize("#,#.#"), p.NumberOfFilesTransferred, p.NumberOfFilesSkipped, p.NumberOfFilesFailed);
        })
      };

      var sourceBlob = source.DirectoryRef();
      var destBlob = dest.DirectoryRef(destPath);

      var (res, dur) = await TransferManager.CopyDirectoryAsync(sourceBlob, destBlob, 
        CopyMethod.ServiceSideSyncCopy, new CopyDirectoryOptions { Recursive = true }, 
        context, CancellationToken.None).WithDuration();

      if (res.NumberOfFilesFailed > 0) 
        Log.Error("Backup {Path} - {Files} files failed to copy", destPath, res.NumberOfFilesFailed);
      Log.Information("Backup {Path} - {Size} of {Files} files copied in {Duration}", 
        destPath, res.BytesTransferred.Bytes().Humanize("#,#.#"), res.NumberOfFilesTransferred, dur.HumanizeShort());
    }
  }
}