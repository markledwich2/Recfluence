using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace SysExtensions.IO {
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
  }
}