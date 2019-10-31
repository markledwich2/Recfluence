using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl {
  public static class SyncBlobs {
    public static async Task Sync(string csA, string csB, StringPath pathA, StringPath pathB, int parallel, ILogger log) {
      pathB = pathB ?? pathA;

      var storeA = new AzureBlobFileStore(csA, pathA);
      var storeB = new AzureBlobFileStore(csB, pathB);

      log.Information("Starting async {FromEndpoint} ({FromPath}) > {ToEndpoint} ({ToPath})",
        storeA.Storage.BlobEndpoint, storeA.BasePath, storeB.Storage.BlobEndpoint, storeB.BasePath);

      await SyncDirectory(pathA, pathB, parallel, log, storeA, storeB);
      
    }

    static async Task SyncDirectory(StringPath pathA, StringPath pathB, int parallel, ILogger log, AzureBlobFileStore storeA, AzureBlobFileStore storeB) {
      
      var sw = Stopwatch.StartNew();
      
      var filesATask = storeA.List(allDirectories:true).ToListWithAction(b => log.Debug("Listed {Files} from source {Path}", b, pathA));
      var filesBTask = storeB.List(allDirectories:true).ToListWithAction(b => log.Debug("Listed {Files} from destination {Path}", b, pathB));

      var filesA = (await filesATask).ToDictionary(f => f.Path);
      var filesB = (await filesBTask).ToDictionary(f => f.Path);
      
      log.Information("Listed all files {Source} souce {Dest} dest in {Duration}",
        filesA.Count, filesB.Count, sw.Elapsed.Humanize(2));

      var toCreate = filesA.Values.Where(f => !filesB.ContainsKey(f.Path)).ToList();
      var toUpdate = filesA.Values.Where(f => filesB.TryGet(f.Path)?.Modified < f.Modified).ToList();

      async Task<IReadOnlyCollection<StringPath>> SaveAll(IEnumerable<FileListItem> files, string action) =>
        await files.BlockTransform(async f => {
            using (var content = await storeA.Load(f.Path))
              await storeB.Save(f.Path, content);
            return f.Path;
          },
          parallel,
          progressUpdate: p => log.Debug("{Action} {Files} at {Speed}", action, p.CompletedTotal, p.Speed("files")));

      sw.Restart();
      log.Information("Starting sync. {Update} to update, {Create} to create",
        toCreate.Count, toUpdate.Count);
      
      var created = await SaveAll(toCreate, "Created");
      var updated = await SaveAll(toUpdate, "Updated");

      log.Information("Completed sync. {Updated} updated, {Created} created in {Duration}",
        updated.Count, created.Count, sw.Elapsed.Humanize(2));
    }
  }

  public static class AsyncExtensions {
    public static async Task<IReadOnlyCollection<T>> ToListWithAction<T>(this IAsyncEnumerable<IReadOnlyCollection<T>> enumerable, Action<int> action) {
      var list = new List<T>();
      await foreach (var item in enumerable) {
        list.AddRange(item);
        action(list.Count);
      }
      return list;
    }
  }
}