using System.IO.Compression;
using System.Reflection;
using System.Text;
using SysExtensions.Collections;
using SysExtensions.Text;
using static System.Environment.SpecialFolder;
using static System.Environment.SpecialFolderOption;
using SystemIO = System.IO;

namespace SysExtensions.IO;

public static class PathExtensions {
  public static FPath AsFPath(this string path) => new(path);

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

  public static bool IsEmpty(this FPath path) => path == null || path.Tokens.None();
  public static bool HasValue(this FPath path) => !IsEmpty(path);

  public static async Task<string> ReadTextAsync(this FPath path) {
    using var sr = path.OpenText();
    return await sr.ReadToEndAsync();
  }

  public static async Task<byte[]> ReadBytesAsync(this FPath path) {
    var mem = new SystemIO.MemoryStream();
    using (var sr = path.Open(SystemIO.FileMode.Open, SystemIO.FileAccess.Read)) await sr.CopyToAsync(mem);
    return mem.ToArray();
  }

  public static SystemIO.StreamWriter CreateText(this FPath path) => SystemIO.File.CreateText(path.FullPath);

  public static SystemIO.StreamReader OpenText(this FPath path) => SystemIO.File.OpenText(path.FullPath);

  public static SystemIO.FileStream Open(this FPath path, SystemIO.FileMode mode,
    SystemIO.FileAccess access = SystemIO.FileAccess.ReadWrite, SystemIO.FileShare share = SystemIO.FileShare.None) =>
    SystemIO.File.Open(path.ToString(), mode, access, share);

  /// <summary>Finds the first parent with the given directory name</summary>
  public static FPath Parent(this FPath path, string directoryName) =>
    path.Parent(p => p.Tokens.Last() == directoryName);

  public static FPath FileOfParent(this FPath path, string searchPattern, bool includeIfDir = false) =>
    path.EnumerateParents(includeIfDir).SelectMany(p => p.Files(searchPattern)).FirstOrDefault(f => !f.IsEmpty());

  public static FPath DirOfParent(this FPath path, string searchPattern, bool includeIfDir = false) =>
    path.EnumerateParents(includeIfDir).SelectMany(p => p.Directories(searchPattern)).FirstOrDefault(d => !d.IsEmpty());

  public static FPath ParentWithFile(this FPath path, string filePattern, bool includeIfDir = false) =>
    path.EnumerateParents(includeIfDir).FirstOrDefault(p => p.Files(filePattern).Any());

  /// <summary>Finds the first parent that satisfies the predicate</summary>
  public static FPath Parent(this FPath path, Predicate<FPath> predicate) =>
    path.EnumerateParents(false).FirstOrDefault(p => predicate(p));

  /// <summary>Finds the child directory of the given name within any of the parent directories (e.g. like node_modules
  ///   resolution)</summary>
  public static IEnumerable<FPath> SubDirectoriesOfParent(this FPath path, string directoryName, bool includeIfDir) =>
    path.EnumerateParents(includeIfDir).SelectMany(p => p.Directories(directoryName)).Where(p => !p.IsEmpty());

  /// <summary>Returns an absolute path with the given one inside the app data folder with the given app name</summary>
  /// <param name="relativePath"></param>
  /// <param name="appName"></param>
  /// <returns></returns>
  public static FPath InAppData(this FPath relativePath, string appName) {
    if (relativePath.IsRooted)
      throw new InvalidOperationException("The given path must be relative");
    return Environment.GetFolderPath(LocalApplicationData, DoNotVerify).AsFPath().Combine(appName).Combine(relativePath);
  }

  public static FPath PathWithoutExtension(this FPath pathToMsql) => pathToMsql.Parent().Combine(pathToMsql.FileNameWithoutExtension);

  public static SystemIO.FileInfo FileInfo(this FPath path) => new(path.FullPath);

  public static FPath LocalAssemblyPath(this Type type)
    => new Uri(type.GetTypeInfo().Assembly.Location).LocalPath.AsFPath();

  static IEnumerable<FPath> Directories(this FPath p, string searchPattern) => p.Directories(searchPattern: searchPattern);

  public static void ExtractZip(this FPath zipFile, FPath dir) => ZipFile.ExtractToDirectory(zipFile.FullPath, dir.FullPath);

  public static SPath ToStringPath(this FPath path) => path.IsRooted ? SPath.Absolute(path.Tokens) : SPath.Relative(path.Tokens);

  public static FPath CreateFile(this FPath file, string content, Encoding encoding = default) {
    encoding ??= Encoding.UTF8;
    using var st = file.Open(SystemIO.FileMode.Create, SystemIO.FileAccess.Write);
    st.Write(encoding.GetBytes(content));
    return file;
  }
}