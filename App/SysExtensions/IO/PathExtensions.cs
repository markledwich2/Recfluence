using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using SysExtensions.Fluent.IO;
using SystemIO = System.IO;

namespace SysExtensions.IO {
  public static class PathExtensions {
    public static FPath AsPath(this string path) => new FPath(path);

    public static IEnumerable<FPath> EnumerateParents(this FPath path, bool includeIfDir) {
      if (path.IsDirectory && includeIfDir)
        yield return path;
      while (true) {
        var oldPath = path;
        path = path.Up();
        if (path == oldPath) break;
        yield return path;
      }
    }

    public static FPath Files(this FPath p, string searchPattern) =>
      p.Files(searchPattern, false);

    public static bool IsEmtpy(this FPath path) => path == null || path.ToStringArray().Length == 0;
    public static bool HasValue(this FPath path) => !IsEmtpy(path);

    public static async Task<string> ReadTextAsync(this FPath path) {
      using (var sr = path.OpenText()) return await sr.ReadToEndAsync();
    }

    public static async Task<byte[]> ReadBytesAsync(this FPath path) {
      var mem = new SystemIO.MemoryStream();
      using (var sr = path.Open(SystemIO.FileMode.Open, SystemIO.FileAccess.Read)) await sr.CopyToAsync(mem);
      return mem.ToArray();
    }

    public static SystemIO.StreamReader OpenText(this FPath path) => SystemIO.File.OpenText(path.FullPath);

    public static SystemIO.FileStream Open(this FPath path, SystemIO.FileMode mode,
      SystemIO.FileAccess access = SystemIO.FileAccess.ReadWrite, SystemIO.FileShare share = SystemIO.FileShare.None) =>
      SystemIO.File.Open(path.ToString(), mode, access, share);

    /// <summary>
    ///   Finds the first parent with the given directory name
    /// </summary>
    public static FPath Parent(this FPath path, string directoryName) =>
      path.Parent(p => p.Tokens.Last() == directoryName);

    public static FPath FileOfParent(this FPath path, string searchPattern, bool includeIfDir = false) =>
      path.EnumerateParents(includeIfDir).Select(p => p.Files(searchPattern, false)).FirstOrDefault(f => !f.IsEmtpy());

    public static FPath DirOfParent(this FPath path, string searchPattern, bool includeIfDir = false) =>
      path.EnumerateParents(includeIfDir).Select(p => p.Directories(searchPattern, false)).FirstOrDefault(d => !d.IsEmtpy());

    public static FPath ParentWithFile(this FPath path, string filePattern, bool includeIfDir = false) {
      return path.EnumerateParents(includeIfDir).FirstOrDefault(p => p.Files(filePattern, false).HasValue());
    }

    /// <summary>
    ///   Finds the first parent that satisfies the predicate
    /// </summary>
    public static FPath Parent(this FPath path, Predicate<FPath> predicate) =>
      path.EnumerateParents(false).FirstOrDefault(p => predicate(p));

    /// <summary>
    ///   Finds the child directory of the given name within any of the parent directories (e.g. like node_modules
    ///   resolution)
    /// </summary>
    public static IEnumerable<FPath> SubDirectoriesOfParent(this FPath path, string directoryName, bool includeIfDir) =>
      path.EnumerateParents(includeIfDir).Select(p => p.Directories(directoryName)).Where(p => !p.IsEmtpy());

    public static FPath Directories(this FPath p, Predicate<FPath> predicate, bool recursive, bool shortcutRecursion) {
      if (!recursive || !shortcutRecursion) return p.Directories(predicate, recursive);

      var result = new FPath();
      var dirQueue = new Queue<FPath>();
      dirQueue.Enqueue(p);
      while (dirQueue.Count > 0) {
        var dir = dirQueue.Dequeue();
        foreach (var childDir in dir.Directories(predicate, false)) {
          dirQueue.Enqueue(childDir);
          result = result.Add(childDir);
        }
      }
      return result;
    }

    /// <summary>
    ///   Returns an absolute path with the given one inside the app data folder with the given app name
    /// </summary>
    /// <param name="relativePath"></param>
    /// <param name="appName"></param>
    /// <returns></returns>
    public static FPath InAppData(this FPath relativePath, string appName) {
      if (relativePath.IsRooted)
        throw new InvalidOperationException("The given path must be relative");

      return Environment.OSVersion.Platform.In(PlatformID.Unix)
        ? new FPath("~", appName).Combine(relativePath)
        : new FPath(Environment.GetEnvironmentVariable("LocalAppData")).Combine(appName).Combine(relativePath);
    }

    public static FPath PathWithoutExtension(this FPath pathToMsql) => pathToMsql.Parent().Combine(pathToMsql.FileNameWithoutExtension);

    public static SystemIO.FileInfo FileInfo(this FPath path) => new SystemIO.FileInfo(path.FullPath);

    public static FPath LocalAssemblyPath(this Type type)
      => new Uri(type.GetTypeInfo().Assembly.CodeBase).LocalPath.AsPath();

    static FPath Directories(this FPath p, string searchPattern) => p.Directories(searchPattern, false);
  }
}