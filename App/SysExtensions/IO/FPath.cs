// Copyright � 2010-2015 Bertrand Le Roy.  All Rights Reserved.
// This code released under the terms of the 
// MIT License http://opensource.org/licenses/MIT
// https://github.com/bleroy/FluentPath

using System.IO;
using System.Text;
using SysExtensions.Collections;
using SysExtensions.Text;
using static System.IO.Directory;
using static System.IO.Path;
using static System.IO.SearchOption;
using SystemPath = System.IO.Path;

namespace SysExtensions.IO;

public record FPath(string Path) {
  public FPath Root => new(GetPathRoot(Path));

  /// <summary>The name of the directory for the first path in the collection. This is the string representation of the parent
  ///   directory path.</summary>
  public string DirectoryName => GetDirectoryName(Path);

  /// <summary>The extension for the first path in the collection, including the ".".</summary>
  public string Extension => GetExtension(Path);

  /// <summary>The filename or folder name for the first path in the collection, including the extension.</summary>
  public string FileName => GetFileName(Path);

  /// <summary>The filename or folder name for the first path in the collection, without the extension.</summary>
  public string FileNameWithoutExtension => GetFileNameWithoutExtension(Path);

  /// <summary>The fully qualified path string for the first path in the collection.</summary>
  public string FullPath => GetFullPath(Path);

  /// <summary>True all the paths in the collection have an extension.</summary>
  public bool HasExtension => HasExtension(Path);

  /// <summary>True if each path in the set is the path of a directory in the file system.</summary>
  public bool IsDirectory => Exists(Path);

  /// <summary>True if all the files in the collection are encrypted on disc.</summary>
  public bool IsEncrypted => Exists(Path) || (File.GetAttributes(Path) & FileAttributes.Encrypted) != 0;

  /// <summary>True if all the paths in the collection are fully-qualified.</summary>
  public bool IsRooted => IsPathRooted(Path);

  /// <summary>The root directory of the first path of the collection, such as "C:\".</summary>
  public string PathRoot => GetPathRoot(Path);

  /// <summary>The tokens for the first path.</summary>
  public string[] Tokens {
    get {
      var tokens = new List<string>();
      var current = Path;
      while (!string.IsNullOrEmpty(current)) {
        tokens.Add(GetFileName(current));
        current = GetDirectoryName(current);
      }
      tokens.Reverse();
      return tokens.ToArray();
    }
  }

  /// <summary>Tests the existence of the paths in the set.</summary>
  /// <returns>True if all paths exist</returns>
  public bool Exists => Exists(Path) || File.Exists(Path);

  public override string ToString() => Path;

  public static explicit operator string(FPath path) => path.Path;

  public static explicit operator FPath(string path) => new(path);

  /// <summary>The parent paths for the paths in the collection.</summary>
  public FPath Parent() => Up();

  /// <summary>Combines each path in the set with the specified relative path. Does not do any physical change to the file
  ///   system.</summary>
  /// <param name="relativePath">The path to combine. Only the first path is used.</param>
  /// <returns>The combined paths.</returns>
  public FPath Combine(FPath relativePath) => Combine(relativePath.Tokens);

  /// <summary>Combines each path in the set with the specified tokens. Does not do any physical change to the file system.</summary>
  /// <param name="pathTokens">One or several directory and file names to combine</param>
  /// <returns>The new set of combined paths</returns>
  public FPath Combine(params string[] pathTokens) => new(SystemPath.Combine(new[] { Path }.Concat(pathTokens).ToArray()));

  /// <summary>The current path for the application.</summary>
  public static FPath WorkingDir => new(GetCurrentDirectory());

  /// <summary>Deletes all files and folders in the set, including non-empty directories if recursive is true.</summary>
  /// <param name="recursive">If true, also deletes the content of directories. Default is false.</param>
  /// <returns>The set of parent directories of all deleted file system entries.</returns>
  public void Delete(bool recursive = false) {
    if (Exists(Path)) {
      if (recursive)
        foreach (var file in GetFiles(Path, "*", AllDirectories))
          File.Delete(file);
      Directory.Delete(Path, recursive);
    }
    else {
      File.Delete(Path);
    }
  }

  /// <summary>Creates a set from all the subdirectories that satisfy the specified predicate.</summary>
  /// <param name="predicate">A function that returns true if the directory should be included.</param>
  /// <param name="searchPattern">A search pattern such as "*.jpg". Default is "*".</param>
  /// <param name="recursive">True if subdirectories should be recursively included.</param>
  /// <returns>The set of directories that satisfy the predicate.</returns>
  public IEnumerable<FPath> Directories(string searchPattern = "*", bool recursive = false) =>
    GetDirectories(Path, searchPattern, recursive ? AllDirectories : TopDirectoryOnly)
      .Select(d => new FPath(d));

  /// <summary>Creates a set from all the files under the path that satisfy the specified predicate.</summary>
  /// <param name="predicate">A function that returns true if the path should be included.</param>
  /// <param name="searchPattern">A search pattern such as "*.jpg". Default is "*".</param>
  /// <param name="recursive">True if subdirectories should be recursively included.</param>
  /// <returns>The set of paths that satisfy the predicate.</returns>
  public IEnumerable<FPath> Files(string searchPattern = "*", bool recursive = false) =>
    GetFiles(Path, searchPattern, recursive ? AllDirectories : TopDirectoryOnly).Select(f => new FPath(f));

  /// <summary>Makes each path relative to the provided one.</summary>
  /// <param name="parentGenerator">A function that returns a path to which the new one is relative to for each of the paths
  ///   in the set.</param>
  /// <returns>The set of relative paths.</returns>
  public FPath MakeRelativeTo(FPath parent) {
    if (!IsPathRooted(Path)) throw new InvalidOperationException("Path must be rooted to be made relative.");
    var fullPath = GetFullPath(Path);
    var parentFull = parent.FullPath;
    if (parentFull[^1] != DirectorySeparatorChar) parentFull += DirectorySeparatorChar;
    if (!fullPath.StartsWith(parentFull)) throw new InvalidOperationException("Path must start with parent.");
    return new(fullPath[parentFull.Length..]);
  }

  /// <summary>Gets all files under this path.</summary>
  /// <returns>The collection of file paths.</returns>
  public IEnumerable<FPath> AllFiles() => Files(recursive: true);

  /// <summary>Goes up the specified number of levels on each path in the set. Never goes above the root of the drive.</summary>
  /// <param name="levels">The number of levels to go up.</param>
  /// <returns>The new set</returns>
  public FPath Up(int levels = 1) {
    var str = Path;
    for (var i = 0; i < levels; i++) {
      var strUp = GetDirectoryName(str);
      if (strUp == null) break;
      str = strUp;
    }
    return new(str);
  }

  public void EnsureDirectoryExists() {
    var fi = new FileInfo(Path);
    var dir = fi.Extension.HasValue() ? fi.Directory!.FullName : fi.FullName;
    if (dir == null) throw new InvalidOperationException($"Directory {Path} not found.");
    if (!Exists(dir)) Directory.CreateDirectory(dir);
  }

  /// <summary>Gets the last write time of the first path in the set</summary>
  /// <returns>The last write time</returns>
  public DateTime LastWriteTime() => IsDirectory ? GetLastWriteTime(Path) : File.GetLastWriteTime(Path);

  /// <summary
  ///   Creates a directory in the file system.
  /// </summary>
  /// <param name="directoryName">The name of the directory to create.</param>
  /// <returns>The path of the new directory.</returns>
  public void CreateDirectory() => Directory.CreateDirectory(Path);

  /// <summary>Reads all text in files in the set.</summary>
  /// <param name="encoding">The encoding to use when reading the file.</param>
  /// <returns>The string as read from the files.</returns>
  public string Read(Encoding encoding = default) => File.ReadAllText(Path, encoding ?? Encoding.UTF8);
}