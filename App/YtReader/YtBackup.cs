using System.Diagnostics;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.Storage.DataMovement;
using Mutuo.Etl.Blob;
using YtReader.Store;

namespace YtReader;

public class YtBackup {
  readonly BlobStores Stores;

  public YtBackup(BlobStores stores) => Stores = stores;

  public static async Task CopyBlobs(string opName, CloudBlobDirectory sourceBlob, CloudBlobDirectory destBlob, ILogger log) {
    var destUrl = destBlob.Uri;
    var sw = Stopwatch.StartNew();
    var logInterval = 5.Seconds();
    var context = new DirectoryTransferContext {
      ProgressHandler = new Progress<TransferStatus>(p => {
        if (sw.Elapsed < logInterval) return;
        sw.Restart();
        log.Debug("{OpName} {Url} - {Size} copied: {Files} files {Skipped} skipped {Failed} failed",
          opName, destUrl, p.BytesTransferred.Bytes().Humanize("#,#.#"), p.NumberOfFilesTransferred, p.NumberOfFilesSkipped, p.NumberOfFilesFailed);
      })
    };

    var (res, dur) = await TransferManager.CopyDirectoryAsync(sourceBlob, destBlob,
      CopyMethod.ServiceSideSyncCopy, new() { Recursive = true },
      context, CancellationToken.None).WithDuration();

    if (res.NumberOfFilesFailed > 0)
      log.Error("{OpName} {Url} -  {Files} files failed to copy",
        opName, destUrl, res.NumberOfFilesFailed);
    log.Information("{OpName} {Url} -  {Size} of {Files} files copied in {Duration}",
      opName, destUrl, res.BytesTransferred.Bytes().Humanize("#,#.#"), res.NumberOfFilesTransferred, dur.HumanizeShort());
  }
}