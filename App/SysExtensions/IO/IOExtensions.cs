using System.IO;
using Humanizer;
using Humanizer.Bytes;
using SysExtensions.Threading;

namespace SysExtensions.IO;

public static class IOExtensions {
  public static IEnumerable<DirectoryInfo> ParentDirectories(this FileInfo file) {
    var dir = file.Directory;
    while (true) {
      yield return dir;
      dir = dir.Parent;
      if (dir == null || !dir.Exists)
        break;
    }
  }

  public static IEnumerable<DirectoryInfo> RelativePath(this FileInfo file, DirectoryInfo root) {
    var path = file.ParentDirectories().TakeWhile(d => d.FullName != root.FullName);
    return path.Reverse();
  }

  public static async Task CopyToAsync(this Stream source, Stream dest, Action<long> onProgress, CancellationToken cancel = default,
    ByteSize? bufferBytes = null, TimeSpan? progressCadence = null) {
    bufferBytes ??= 100.Kilobytes();
    progressCadence ??= 1.Seconds();

    var buffer = new byte[(int)bufferBytes.Value.Bytes];
    int count;
    var transferred = 0L;

    CancellationTokenSource innerCancel = new();
    cancel.Register(() => innerCancel.Cancel());

    async Task Progress() {
      while (!innerCancel.IsCancellationRequested) {
        await progressCadence.Value.Delay(innerCancel.Token).Swallow();
        if (!innerCancel.IsCancellationRequested)
          // ReSharper disable once AccessToModifiedClosure - intentional
          onProgress(transferred);
      }
    }

    var progTask = Progress();
    while ((count = await source.ReadAsync(buffer.AsMemory(start: 0, buffer.Length))) != 0) {
      Interlocked.Add(ref transferred, count);
      await dest.WriteAsync(buffer, offset: 0, count);
    }
    innerCancel.Cancel();
    await progTask;
  }
}