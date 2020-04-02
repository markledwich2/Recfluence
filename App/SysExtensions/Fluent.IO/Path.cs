// Copyright � 2010-2015 Bertrand Le Roy.  All Rights Reserved.
// This code released under the terms of the 
// MIT License http://opensource.org/licenses/MIT
// https://github.com/bleroy/FluentPath

using System;
using System.Collections;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SystemPath = System.IO.Path;

namespace SysExtensions.Fluent.IO {
  [TypeConverter(typeof(StringConverter<FPath>))]
  public class FPath : IStringConvertable, IEnumerable<FPath> {
    IEnumerable<string> _paths;
    FPath               _previousPaths;

    /// <summary>Creates an empty Path object.</summary>
    public FPath() : this(new string[] { }) { }

    /// <summary>Creates a collection of paths from a list of path strings.</summary>
    /// <param name="paths">The list of path strings.</param>
    public FPath(params string[] paths) : this((IEnumerable<string>) paths) { }

    /// <summary>Creates a collection of paths from a list of path strings.</summary>
    /// <param name="paths">The list of path strings.</param>
    public FPath(params FPath[] paths)
      : this((IEnumerable<FPath>) paths) { }

    /// <summary>Creates a collection of paths from a list of path strings.</summary>
    /// <param name="paths">The list of path strings.</param>
    public FPath(IEnumerable<string> paths) : this(paths, null) { }

    /// <summary>Creates a collection of paths from a list of paths.</summary>
    /// <param name="paths">The list of paths.</param>
    public FPath(IEnumerable<FPath> paths)
      : this(paths.SelectMany(p => p._paths), null) { }

    /// <summary>Creates a collection of paths from a list of path strings and a previous list of path strings.</summary>
    /// <param name="path">A path string.</param>
    /// <param name="previousPaths">The list of path strings in the previous set.</param>
    protected FPath(string path, FPath previousPaths) {
      _paths = new[] {path};
      _previousPaths = previousPaths;
    }

    /// <summary>Creates a collection of paths from a list of path strings and a previous list of path strings.</summary>
    /// <param name="paths">The list of path strings in the set.</param>
    /// <param name="previousPaths">The list of path strings in the previous set.</param>
    protected FPath(IEnumerable<string> paths, FPath previousPaths) {
      if (paths == null) throw new ArgumentNullException(nameof(paths));
      _paths = paths
        .Where(s => !string.IsNullOrWhiteSpace(s))
        .Select(s => s[s.Length - 1] == SystemPath.DirectorySeparatorChar &&
                     SystemPath.GetPathRoot(s) != s
          ? s.Substring(0, s.Length - 1)
          : s)
        .Distinct(StringComparer.CurrentCultureIgnoreCase);
      _previousPaths = previousPaths;
    }

    /// <summary>The current path for the application.</summary>
    public static FPath Current {
      get => Create(Directory.GetCurrentDirectory());
      set => Directory.SetCurrentDirectory(value.FirstPath());
    }

    public static FPath Root => Create(SystemPath.GetPathRoot(Current.ToString()));

    /// <summary>The name of the directory for the first path in the collection. This is the string representation of the
    ///   parent directory path.</summary>
    public string DirectoryName => SystemPath.GetDirectoryName(FirstPath());

    /// <summary>The extension for the first path in the collection, including the ".".</summary>
    public string Extension => SystemPath.GetExtension(FirstPath());

    /// <summary>The filename or folder name for the first path in the collection, including the extension.</summary>
    public string FileName => SystemPath.GetFileName(FirstPath());

    /// <summary>The filename or folder name for the first path in the collection, without the extension.</summary>
    public string FileNameWithoutExtension => SystemPath.GetFileNameWithoutExtension(FirstPath());

    /// <summary>The fully qualified path string for the first path in the collection.</summary>
    public string FullPath => SystemPath.GetFullPath(FirstPath());

    /// <summary>The fully qualified path strings for all the paths in the collection.</summary>
    public string[] FullPaths {
      get {
        var result = new HashSet<string>();
        foreach (var path in _paths) result.Add(SystemPath.GetFullPath(path));
        return result.ToArray();
      }
    }

    /// <summary>True all the paths in the collection have an extension.</summary>
    public bool HasExtension => _paths.All(SystemPath.HasExtension);

    /// <summary>True if each path in the set is the path of a directory in the file system.</summary>
    public bool IsDirectory => _paths.All(Directory.Exists);

    /// <summary>True if all the files in the collection are encrypted on disc.</summary>
    public bool IsEncrypted
      => _paths.All(p =>
        Directory.Exists(p) ||
        (File.GetAttributes(p) & FileAttributes.Encrypted) != 0);

    /// <summary>True if all the paths in the collection are fully-qualified.</summary>
    public bool IsRooted => _paths.All(SystemPath.IsPathRooted);

    /// <summary>The root directory of the first path of the collection, such as "C:\".</summary>
    public string PathRoot => SystemPath.GetPathRoot(FirstPath());

    /// <summary>The tokens for the first path.</summary>
    public string[] Tokens {
      get {
        var tokens = new List<string>();
        var current = FirstPath();
        while (!string.IsNullOrEmpty(current)) {
          tokens.Add(SystemPath.GetFileName(current));
          current = SystemPath.GetDirectoryName(current);
        }

        tokens.Reverse();
        return tokens.ToArray();
      }
    }

    /// <summary>Tests the existence of the paths in the set.</summary>
    /// <returns>True if all paths exist</returns>
    public bool Exists => _paths.All(path => Directory.Exists(path) || File.Exists(path));

    public IEnumerator<FPath> GetEnumerator() => _paths.Select(path => Create(path, this)).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public string StringValue {
      get => _paths.Join(",", escapeCharacter: '$');
      set => _paths = value.UnJoin(',', '$');
    }

    public override string ToString() => StringValue;

    /// <summary
    ///   Creates a directory in the file system.
    /// </summary>
    /// <param name="directoryName">The name of the directory to create.</param>
    /// <returns>The path of the new directory.</returns>
    public static FPath CreateDirectory(string directoryName) {
      Directory.CreateDirectory(directoryName);
      return Create(directoryName);
    }

    /// <summary>Creates a new path from its string token representation.</summary>
    /// <example>Path.Get("c:", "foo", "bar") will get c:\foo\bar on Windows.</example>
    /// <param name="pathTokens">The tokens for the path.</param>
    /// <returns>The path object.</returns>
    public static FPath Get(params string[] pathTokens) {
      if (pathTokens.Length == 0) throw new ArgumentException("At least one token needs to be specified.", nameof(pathTokens));
      return Create(SystemPath.Combine(pathTokens));
    }

    public static explicit operator string(FPath path) => path.FirstPath();

    public static explicit operator FPath(string path) => new FPath(path);

    public static bool operator ==(FPath path1, FPath path2) {
      if (ReferenceEquals(path1, path2)) return true;
      if ((object) path1 == null || (object) path2 == null) return false;
      return path1.IsSameAs(path2);
    }

    public static bool operator !=(FPath path1, FPath path2) => !(path1 == path2);

    // Overrides
    public override bool Equals(object obj) {
      var paths = obj as FPath;
      if (paths != null) return IsSameAs(paths);
      var str = obj as string;
      if (str == null) return false;
      var enumerator = _paths.GetEnumerator();
      if (!enumerator.MoveNext()) return false;
      if (enumerator.Current != str) return false;
      return !enumerator.MoveNext();
    }

    public override int GetHashCode() => _paths.Aggregate(17, (h, p) => 23 * h + (p ?? "").GetHashCode());

    /// <summary>The parent paths for the paths in the collection.</summary>
    public FPath Parent() => First().Up();

    /// <summary>The parent paths for the paths in the collection.</summary>
    public FPath Parents() => Up();

    /// <summary>The previous set, from which the current one was created.</summary>
    public FPath Previous() => _previousPaths;

    /// <summary>Changes the path on each path in the set. Does not do any physical change to the file system.</summary>
    /// <param name="newExtension">The new extension.</param>
    /// <returns>The set</returns>
    public FPath ChangeExtension(string newExtension) => ChangeExtension(p => newExtension);

    /// <summary>Changes the path on each path in the set. Does not do any physical change to the file system.</summary>
    /// <param name="extensionTransformation">A function that maps each path to an extension.</param>
    /// <returns>The set of files with the new extension</returns>
    public FPath ChangeExtension(Func<FPath, string> extensionTransformation) {
      var result = new HashSet<string>();
      foreach (var path in _paths.Where(p => !Directory.Exists(p)))
        result.Add(
          SystemPath.ChangeExtension(path,
            extensionTransformation(Create(path, this))));
      return Create(result, this);
    }

    /// <summary>Combines each path in the set with the specified file or directory name. Does not do any physical change to
    ///   the file system.</summary>
    /// <param name="directoryNameGenerator">A function that maps each path to a file or directory name.</param>
    /// <returns>The set</returns>
    public FPath Combine(Func<FPath, string> directoryNameGenerator) {
      var result = new HashSet<string>();
      foreach (var path in _paths) result.Add(SystemPath.Combine(path, directoryNameGenerator(Create(path, this))));
      return Create(result, this);
    }

    /// <summary>Combines each path in the set with the specified relative path. Does not do any physical change to the file
    ///   system.</summary>
    /// <param name="relativePath">The path to combine. Only the first path is used.</param>
    /// <returns>The combined paths.</returns>
    public FPath Combine(FPath relativePath) => Combine(relativePath.Tokens);

    /// <summary>Combines each path in the set with the specified tokens. Does not do any physical change to the file system.</summary>
    /// <param name="pathTokens">One or several directory and file names to combine</param>
    /// <returns>The new set of combined paths</returns>
    public FPath Combine(params string[] pathTokens) {
      if (pathTokens.Length == 0) return this;
      if (pathTokens.Length == 1) return Combine(p => pathTokens[0]);
      var result = new HashSet<string>();
      var concatenated = new string[pathTokens.Length + 1];
      pathTokens.CopyTo(concatenated, 1);
      foreach (var path in _paths) {
        concatenated[0] = path;
        result.Add(SystemPath.Combine(concatenated));
      }

      return Create(result, this);
    }

    /// <summary>Copies the file or folder for this path to another location. The copy is not recursive. Existing files won't
    ///   be overwritten.</summary>
    /// <param name="destination">The destination path.</param>
    /// <returns>The destination path.</returns>
    public FPath Copy(FPath destination) => Copy(p => destination, Overwrite.Never, false);

    /// <summary>Copies the file or folder for this path to another location. The copy is not recursive.</summary>
    /// <param name="destination">The destination path.</param>
    /// <param name="overwrite">Overwriting policy. Default is never.</param>
    /// <returns>The destination path.</returns>
    public FPath Copy(FPath destination, Overwrite overwrite) => Copy(p => destination, overwrite, false);

    /// <summary>Copies the file or folder for this path to another location.</summary>
    /// <param name="destination">The destination path.</param>
    /// <param name="overwrite">Overwriting policy. Default is never.</param>
    /// <param name="recursive">True if the copy should be deep and include subdirectories recursively. Default is false.</param>
    /// <returns>The source path.</returns>
    public FPath Copy(FPath destination, Overwrite overwrite, bool recursive) => Copy(p => destination, overwrite, recursive);

    /// <summary>Copies the file or folder for this path to another location. The copy is not recursive. Existing files won't
    ///   be overwritten.</summary>
    /// <param name="destination">The destination path string.</param>
    /// <returns>The destination path.</returns>
    public FPath Copy(string destination) => Copy(p => Create(destination, this), Overwrite.Never, false);

    /// <summary>Copies the file or folder for this path to another location. The copy is not recursive.</summary>
    /// <param name="destination">The destination path string.</param>
    /// <param name="overwrite">Overwriting policy. Default is never.</param>
    /// <returns>The destination path.</returns>
    public FPath Copy(string destination, Overwrite overwrite) => Copy(p => Create(destination, this), overwrite, false);

    /// <summary>Copies the file or folder for this path to another location.</summary>
    /// <param name="destination">The destination path string.</param>
    /// <param name="overwrite">Overwriting policy. Default is never.</param>
    /// <param name="recursive">True if the copy should be deep and include subdirectories recursively. Default is false.</param>
    /// <returns>The destination path.</returns>
    public FPath Copy(string destination, Overwrite overwrite, bool recursive) => Copy(p => Create(destination, this), overwrite, recursive);

    /// <summary>Does a copy of all files and directories in the set.</summary>
    /// <param name="pathMapping">A function that determines the destination path for each source path. If the function returns
    ///   a null path, the file or directory is not copied.</param>
    /// <returns>The set</returns>
    public FPath Copy(Func<FPath, FPath> pathMapping) => Copy(pathMapping, Overwrite.Never, false);

    /// <summary>Does a copy of all files and directories in the set.</summary>
    /// <param name="pathMapping">A function that determines the destination path for each source path. If the function returns
    ///   a null path, the file or directory is not copied.</param>
    /// <param name="overwrite">Destination file overwriting policy. Default is never.</param>
    /// <param name="recursive">True if the copy should be deep and go into subdirectories recursively. Default is false.</param>
    /// <returns>The set</returns>
    public FPath Copy(Func<FPath, FPath> pathMapping, Overwrite overwrite, bool recursive) {
      var result = new HashSet<string>();
      foreach (var sourcePath in _paths) {
        if (sourcePath == null) continue;
        var source = Create(sourcePath, this);
        var dest = pathMapping(source);
        if (dest == null) continue;
        foreach (var destPath in dest._paths) {
          var p = destPath;
          if (Directory.Exists(sourcePath)) {
            // source is a directory
            CopyDirectory(sourcePath, p, overwrite, recursive);
          }
          else {
            // source is a file
            p = Directory.Exists(p)
              ? SystemPath.Combine(p, SystemPath.GetFileName(sourcePath))
              : p;
            CopyFile(sourcePath, p, overwrite);
            result.Add(p);
          }
        }
      }

      return Create(result, this);
    }

    /// <summary>Creates subdirectories for each directory.</summary>
    /// <param name="directoryNameGenerator">A function that returns the new directory name for each path. If the function
    ///   returns null, no directory is created.</param>
    /// <returns>The set</returns>
    public FPath CreateDirectories(Func<FPath, string> directoryNameGenerator) => CreateDirectories(p => Create(directoryNameGenerator(p)));

    /// <summary>Creates subdirectories for each directory.</summary>
    /// <param name="directoryNameGenerator">A function that returns the new directory name for each path. If the function
    ///   returns null, no directory is created.</param>
    /// <returns>The set</returns>
    public FPath CreateDirectories(Func<FPath, FPath> directoryNameGenerator) {
      var result = new HashSet<string>();
      foreach (var destPath in _paths
        .Select(path => Create(path, this))
        .Select(directoryNameGenerator)
        .Where(dest => dest != null)
        .SelectMany(dest => dest._paths)) {
        Directory.CreateDirectory(destPath);
        result.Add(destPath);
      }

      return Create(result, this);
    }

    /// <summary>Creates directories for each path in the set.</summary>
    /// <returns>The set</returns>
    public FPath CreateDirectories() => CreateDirectories(p => p);

    /// <summary>Creates subdirectories for each directory.</summary>
    /// <param name="directoryName">The name of the new directory.</param>
    /// <returns>The set</returns>
    public FPath CreateDirectories(string directoryName) => CreateDirectories(p => p.Combine(directoryName));

    /// <summary>Creates a directory for the first path in the set.</summary>
    /// <returns>The created path</returns>
    public FPath CreateDirectory() => First().CreateDirectories();

    public FPath CreateSubDirectory(string directoryName) => CreateSubDirectories(p => directoryName);

    public FPath CreateSubDirectories(Func<FPath, string> directoryNameGenerator) {
      var combined = Combine(directoryNameGenerator);
      var result = new HashSet<string>();
      foreach (var path in combined._paths) {
        Directory.CreateDirectory(path);
        result.Add(path);
      }

      return Create(result, this);
    }

    /// <summary>Creates a file under the first path in the set.</summary>
    /// <param name="fileName">The name of the file.</param>
    /// <param name="fileContent">The content of the file.</param>
    /// <returns>A set with the created file.</returns>
    public FPath CreateFile(string fileName, string fileContent) => First().CreateFiles(p => Create(fileName, this), p => fileContent);

    /// <summary>Creates a file under the first path in the set.</summary>
    /// <param name="fileName">The name of the file.</param>
    /// <param name="fileContent">The content of the file.</param>
    /// <param name="encoding">The encoding to use.</param>
    /// <returns>A set with the created file.</returns>
    public FPath CreateFile(string fileName, string fileContent, Encoding encoding) =>
      First().CreateFiles(p => Create(fileName, this), p => fileContent, encoding);

    /// <summary>Creates a file under the first path in the set.</summary>
    /// <param name="fileName">The name of the file.</param>
    /// <param name="fileContent">The content of the file.</param>
    /// <returns>A set with the created file.</returns>
    public FPath CreateFile(string fileName, byte[] fileContent) => First().CreateFiles(p => Create(fileName, this), p => fileContent);

    /// <summary>Creates files under each of the paths in the set.</summary>
    /// <param name="fileNameGenerator">A function that returns a file name for each path.</param>
    /// <param name="fileContentGenerator">A function that returns file content for each path.</param>
    /// <returns>The set of created files.</returns>
    public FPath CreateFiles(
      Func<FPath, FPath> fileNameGenerator,
      Func<FPath, string> fileContentGenerator) {
      var result = new HashSet<string>();
      foreach (var path in _paths) {
        var p = Create(path, this);
        var newFilePath = p.Combine(fileNameGenerator(p).FirstPath()).FirstPath();
        EnsureDirectoryExists(newFilePath);
        File.WriteAllText(newFilePath, fileContentGenerator(p));
        result.Add(newFilePath);
      }

      return Create(result, this);
    }

    /// <summary>Creates files under each of the paths in the set.</summary>
    /// <param name="fileNameGenerator">A function that returns a file name for each path.</param>
    /// <param name="fileContentGenerator">A function that returns file content for each path.</param>
    /// <param name="encoding">The encoding to use.</param>
    /// <returns>The set of created files.</returns>
    public FPath CreateFiles(
      Func<FPath, FPath> fileNameGenerator,
      Func<FPath, string> fileContentGenerator,
      Encoding encoding) {
      var result = new HashSet<string>();
      foreach (var path in _paths) {
        var p = Create(path, this);
        var newFilePath = p.Combine(fileNameGenerator(p).FirstPath()).FirstPath();
        EnsureDirectoryExists(newFilePath);
        File.WriteAllText(newFilePath, fileContentGenerator(p), encoding);
        result.Add(newFilePath);
      }

      return Create(result, this);
    }

    /// <summary>Creates files under each of the paths in the set.</summary>
    /// <param name="fileNameGenerator">A function that returns a file name for each path.</param>
    /// <param name="fileContentGenerator">A function that returns file content for each path.</param>
    /// <returns>The set of created files.</returns>
    public FPath CreateFiles(
      Func<FPath, FPath> fileNameGenerator,
      Func<FPath, byte[]> fileContentGenerator) {
      var result = new HashSet<string>();
      foreach (var path in _paths) {
        var p = Create(path, this);
        var newFilePath = p.Combine(fileNameGenerator(p).FirstPath()).FirstPath();
        EnsureDirectoryExists(newFilePath);
        File.WriteAllBytes(newFilePath, fileContentGenerator(p));
        result.Add(newFilePath);
      }

      return Create(result, this);
    }

    /// <summary>Deletes this path from the file system.</summary>
    /// <returns>The parent path.</returns>
    public FPath Delete() => Delete(false);

    /// <summary>Deletes all files and folders in the set, including non-empty directories if recursive is true.</summary>
    /// <param name="recursive">If true, also deletes the content of directories. Default is false.</param>
    /// <returns>The set of parent directories of all deleted file system entries.</returns>
    public FPath Delete(bool recursive) {
      var result = new HashSet<string>();
      foreach (var path in _paths) {
        if (Directory.Exists(path)) {
          if (recursive)
            foreach (var file in Directory.GetFiles(path, "*", SearchOption.AllDirectories))
              File.Delete(file);
          Directory.Delete(path, recursive);
        }
        else {
          File.Delete(path);
        }

        result.Add(SystemPath.GetDirectoryName(path));
      }

      return Create(result, this);
    }

    /// <summary>Filters the set according to the predicate.</summary>
    /// <param name="predicate">A predicate that returns true for the entries that must be in the returned set.</param>
    /// <returns>The filtered set.</returns>
    public FPath Where(Predicate<FPath> predicate) {
      var result = new HashSet<string>();
      foreach (var path in _paths.Where(path => predicate(Create(path, this)))) result.Add(path);
      return Create(result, this);
    }

    /// <summary>Filters the set</summary>
    /// <param name="extensions"></param>
    /// <returns></returns>
    public FPath WhereExtensionIs(params string[] extensions) =>
      Where(
        p => {
          var ext = p.Extension;
          return extensions.Contains(ext) ||
                 ext.Length > 0 && extensions.Contains(ext.Substring(1));
        });

    /// <summary>Executes an action for each file or folder in the set.</summary>
    /// <param name="action">An action that takes the path of each entry as its parameter.</param>
    /// <returns>The set</returns>
    public FPath ForEach(Action<FPath> action) {
      foreach (var path in _paths) action(Create(path, this));
      return this;
    }

    /// <summary>Gets the subdirectories of folders in the set.</summary>
    /// <returns>The set of matching subdirectories.</returns>
    public FPath Directories() => Directories(p => true, "*", false);

    /// <summary>Gets all the subdirectories of folders in the set that match the provided pattern and using the provided
    ///   options.</summary>
    /// <param name="searchPattern">A search pattern such as "*.jpg". Default is "*".</param>
    /// <param name="recursive">True if subdirectories should also be searched recursively. Default is false.</param>
    /// <returns>The set of matching subdirectories.</returns>
    public FPath Directories(string searchPattern, bool recursive) => Directories(p => true, searchPattern, recursive);

    /// <summary>Creates a set from all the subdirectories that satisfy the specified predicate.</summary>
    /// <param name="predicate">A function that returns true if the directory should be included.</param>
    /// <returns>The set of directories that satisfy the predicate.</returns>
    public FPath Directories(Predicate<FPath> predicate) => Directories(predicate, "*", false);

    /// <summary>Creates a set from all the subdirectories that satisfy the specified predicate.</summary>
    /// <param name="predicate">A function that returns true if the directory should be included.</param>
    /// <param name="recursive">True if subdirectories should be recursively included.</param>
    /// <returns>The set of directories that satisfy the predicate.</returns>
    public FPath Directories(Predicate<FPath> predicate, bool recursive) => Directories(predicate, "*", recursive);

    /// <summary>Creates a set from all the subdirectories that satisfy the specified predicate.</summary>
    /// <param name="predicate">A function that returns true if the directory should be included.</param>
    /// <param name="searchPattern">A search pattern such as "*.jpg". Default is "*".</param>
    /// <param name="recursive">True if subdirectories should be recursively included.</param>
    /// <returns>The set of directories that satisfy the predicate.</returns>
    public FPath Directories(Predicate<FPath> predicate, string searchPattern, bool recursive) {
      var result = new HashSet<string>();
      foreach (var dir in _paths
        .Select(p => Directory.GetDirectories(p, searchPattern,
          recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly))
        .SelectMany(dirs => dirs.Where(dir => predicate(Create(dir, this))))) result.Add(dir);
      return Create(result, this);
    }

    /// <summary>Gets all the files under the directories of the set.</summary>
    /// <returns>The set of files.</returns>
    public FPath Files() => Files(p => true, "*", false);

    /// <summary>Gets all the files under the directories of the set that match the pattern, going recursively into
    ///   subdirectories if recursive is set to true.</summary>
    /// <param name="searchPattern">A search pattern such as "*.jpg". Default is "*".</param>
    /// <param name="recursive">If true, subdirectories are explored as well. Default is false.</param>
    /// <returns>The set of files that match the pattern.</returns>
    public FPath Files(string searchPattern, bool recursive) => Files(p => true, searchPattern, recursive);

    /// <summary>Creates a set from all the files under the path that satisfy the specified predicate.</summary>
    /// <param name="predicate">A function that returns true if the path should be included.</param>
    /// <returns>The set of paths that satisfy the predicate.</returns>
    public FPath Files(Predicate<FPath> predicate) => Files(predicate, "*", false);

    /// <summary>Creates a set from all the files under the path that satisfy the specified predicate.</summary>
    /// <param name="predicate">A function that returns true if the path should be included.</param>
    /// <param name="recursive">True if subdirectories should be recursively included.</param>
    /// <returns>The set of paths that satisfy the predicate.</returns>
    public FPath Files(Predicate<FPath> predicate, bool recursive) => Files(predicate, "*", recursive);

    /// <summary>Creates a set from all the files under the path that satisfy the specified predicate.</summary>
    /// <param name="predicate">A function that returns true if the path should be included.</param>
    /// <param name="searchPattern">A search pattern such as "*.jpg". Default is "*".</param>
    /// <param name="recursive">True if subdirectories should be recursively included.</param>
    /// <returns>The set of paths that satisfy the predicate.</returns>
    public FPath Files(Predicate<FPath> predicate, string searchPattern, bool recursive) {
      var result = new HashSet<string>();
      foreach (var file in _paths
        .Select(p => Directory.GetFiles(p, searchPattern,
          recursive ? SearchOption.AllDirectories : SearchOption.TopDirectoryOnly))
        .SelectMany(files => files.Where(f => predicate(Create(f, this))))) result.Add(file);
      return Create(result, this);
    }

    /// <summary>Gets all the files and subdirectories under the directories of the set.</summary>
    /// <returns>The set of files and folders.</returns>
    public FPath FileSystemEntries() => FileSystemEntries(p => true, "*", false);

    /// <summary>Gets all the files and subdirectories under the directories of the set that match the pattern, going
    ///   recursively into subdirectories if recursive is set to true.</summary>
    /// <param name="searchPattern">A search pattern such as "*.jpg". Default is "*".</param>
    /// <param name="recursive">If true, subdirectories are explored as well. Default is false.</param>
    /// <returns>The set of files and folders that match the pattern.</returns>
    public FPath FileSystemEntries(string searchPattern, bool recursive) => FileSystemEntries(p => true, searchPattern, recursive);

    /// <summary>Creates a set from all the files and subdirectories under the path that satisfy the specified predicate.</summary>
    /// <param name="predicate">A function that returns true if the path should be included.</param>
    /// <returns>The set of fils and subdirectories that satisfy the predicate.</returns>
    public FPath FileSystemEntries(Predicate<FPath> predicate) => FileSystemEntries(predicate, "*", false);

    /// <summary>Creates a set from all the files and subdirectories under the path that satisfy the specified predicate.</summary>
    /// <param name="predicate">A function that returns true if the path should be included.</param>
    /// <param name="recursive">True if subdirectories should be recursively included.</param>
    /// <returns>The set of fils and subdirectories that satisfy the predicate.</returns>
    public FPath FileSystemEntries(Predicate<FPath> predicate, bool recursive) => FileSystemEntries(predicate, "*", recursive);

    /// <summary>Creates a set from all the files and subdirectories under the path that satisfy the specified predicate.</summary>
    /// <param name="predicate">A function that returns true if the path should be included.</param>
    /// <param name="searchPattern">A search pattern such as "*.jpg". Default is "*".</param>
    /// <param name="recursive">True if subdirectories should be recursively included.</param>
    /// <returns>The set of fils and subdirectories that satisfy the predicate.</returns>
    public FPath FileSystemEntries(Predicate<FPath> predicate, string searchPattern, bool recursive) {
      var result = new HashSet<string>();
      var searchOptions = recursive
        ? SearchOption.AllDirectories
        : SearchOption.TopDirectoryOnly;
      foreach (var p in _paths) {
        var directories = Directory.GetDirectories(p, searchPattern, searchOptions);
        foreach (var entry in directories.Where(d => predicate(Create(d, this)))) result.Add(entry);
        var files = Directory.GetFiles(p, searchPattern, searchOptions);
        foreach (var entry in files.Where(f => predicate(Create(f, this)))) result.Add(entry);
      }

      return Create(result, this);
    }

    /// <summary>Gets the first path of the set.</summary>
    /// <returns>A new path from the first path of the set</returns>
    public FPath First() {
      var first = _paths.FirstOrDefault();
      if (first != null) return Create(first, this);
      throw new InvalidOperationException(
        "Can't get the first element of an empty collection.");
    }

    /// <summary>Looks for a specific text pattern in each file in the set.</summary>
    /// <param name="regularExpression">The pattern to look for</param>
    /// <param name="action">The action to execute for each match</param>
    /// <returns>The set</returns>
    public FPath Grep(string regularExpression, Action<FPath, Match, string> action) => Grep(new Regex(regularExpression, RegexOptions.Multiline), action);

    /// <summary>Looks for a specific text pattern in each file in the set.</summary>
    /// <param name="regularExpression">The pattern to look for</param>
    /// <param name="action">The action to execute for each match</param>
    /// <returns>The set</returns>
    public FPath Grep(Regex regularExpression, Action<FPath, Match, string> action) {
      foreach (var path in _paths.Where(p => !Directory.Exists(p))) {
        var contents = File.ReadAllText(path);
        var matches = regularExpression.Matches(contents);
        var p = Create(path, this);
        foreach (Match match in matches) action(p, match, contents);
      }

      return this;
    }

    /// <summary>Makes this path the current path for the application.</summary>
    /// <returns>The set.</returns>
    public FPath MakeCurrent() {
      Current = this;
      return this;
    }

    /// <summary>Makes each path relative to the current path.</summary>
    /// <returns>The set of relative paths.</returns>
    public FPath MakeRelative() => MakeRelativeTo(Current);

    /// <summary>Makes each path relative to the provided one.</summary>
    /// <param name="parent">The path to which the new one is relative to.</param>
    /// <returns>The set of relative paths.</returns>
    public FPath MakeRelativeTo(string parent) => MakeRelativeTo(Create(parent, this));

    /// <summary>Makes each path relative to the provided one.</summary>
    /// <param name="parent">The path to which the new one is relative to.</param>
    /// <returns>The set of relative paths.</returns>
    public FPath MakeRelativeTo(FPath parent) => MakeRelativeTo(p => parent);

    /// <summary>Makes each path relative to the provided one.</summary>
    /// <param name="parentGenerator">A function that returns a path to which the new one is relative to for each of the paths
    ///   in the set.</param>
    /// <returns>The set of relative paths.</returns>
    public FPath MakeRelativeTo(Func<FPath, FPath> parentGenerator) {
      var result = new HashSet<string>();
      foreach (var path in _paths) {
        if (!SystemPath.IsPathRooted(path)) throw new InvalidOperationException("Path must be rooted to be made relative.");
        var fullPath = SystemPath.GetFullPath(path);
        var parentFull = parentGenerator(Create(path, this)).FullPath;
        if (parentFull[parentFull.Length - 1] != SystemPath.DirectorySeparatorChar) parentFull += SystemPath.DirectorySeparatorChar;
        if (!fullPath.StartsWith(parentFull)) throw new InvalidOperationException("Path must start with parent.");
        result.Add(fullPath.Substring(parentFull.Length));
      }

      return Create(result, this);
    }

    /// <summary>Maps all the paths in the set to a new set of paths using the provided mapping function.</summary>
    /// <param name="pathMapping">A function that takes a path and returns a transformed path.</param>
    /// <returns>The mapped set.</returns>
    public FPath Map(Func<FPath, FPath> pathMapping) {
      var result = new HashSet<string>();
      foreach (var mapped in
        from path in _paths
        select pathMapping(Create(path))
        into mappedPaths
        where mappedPaths != null
        from mapped in mappedPaths._paths
        select mapped) result.Add(mapped);
      return Create(result, this);
    }

    /// <summary>Moves the current path in the file system. Existing files are never overwritten.</summary>
    /// <param name="destination">The destination path.</param>
    /// <returns>The destination path.</returns>
    public FPath Move(string destination) => Move(p => Create(destination, this), Overwrite.Never);

    /// <summary>Moves the current path in the file system.</summary>
    /// <param name="destination">The destination path.</param>
    /// <param name="overwrite">Overwriting policy. Default is never.</param>
    /// <returns>The destination path.</returns>
    public FPath Move(string destination, Overwrite overwrite) => Move(p => Create(destination, this), overwrite);

    /// <summary>Moves all the files and folders in the set to new locations as specified by the mapping function.</summary>
    /// <param name="pathMapping">The function that maps from the current path to the new one.</param>
    /// <returns>The moved set.</returns>
    public FPath Move(Func<FPath, FPath> pathMapping) => Move(pathMapping, Overwrite.Never);

    /// <summary>Moves all the files and folders in the set to new locations as specified by the mapping function.</summary>
    /// <param name="pathMapping">The function that maps from the current path to the new one.</param>
    /// <param name="overwrite">Overwriting policy. Default is never.</param>
    /// <returns>The moved set.</returns>
    public FPath Move(
      Func<FPath, FPath> pathMapping,
      Overwrite overwrite) {
      var result = new HashSet<string>();
      foreach (var path in _paths) {
        if (path == null) continue;
        var source = Create(path, this);
        var dest = pathMapping(source);
        if (dest == null) continue;
        foreach (var destPath in dest._paths) {
          var d = destPath;
          if (Directory.Exists(path)) {
            MoveDirectory(path, d, overwrite);
          }
          else {
            d = Directory.Exists(d)
              ? SystemPath.Combine(d, SystemPath.GetFileName(path))
              : d;
            MoveFile(path, d, overwrite);
          }

          result.Add(d);
        }
      }

      return Create(result, this);
    }

    /// <summary>Opens all the files in the set and hands them to the provided action.</summary>
    /// <param name="action">The action to perform on the open files.</param>
    /// <param name="mode">The FileMode to use. Default is OpenOrCreate.</param>
    /// <param name="access">The FileAccess to use. Default is ReadWrite.</param>
    /// <param name="share">The FileShare to use. Default is None.</param>
    /// <returns>The set</returns>
    public FPath Open(Action<FileStream> action, FileMode mode, FileAccess access, FileShare share) {
      foreach (var path in _paths)
        using (var stream = File.Open(path, mode, access, share))
          action(stream);

      return this;
    }

    /// <summary>Opens all the files in the set and hands them to the provided action.</summary>
    /// <param name="action">The action to perform on the open streams.</param>
    /// <returns>The set</returns>
    public FPath Open(Action<FileStream> action) => Open(action, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

    /// <summary>Opens all the files in the set and hands them to the provided action.</summary>
    /// <param name="action">The action to perform on the open streams.</param>
    /// <returns>The set</returns>
    public FPath Open(Action<FileStream, FPath> action) => Open(action, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);

    /// <summary>Opens all the files in the set and hands them to the provided action.</summary>
    /// <param name="action">The action to perform on the open streams.</param>
    /// <param name="mode">The FileMode to use. Default is OpenOrCreate.</param>
    /// <param name="access">The FileAccess to use. Default is ReadWrite.</param>
    /// <param name="share">The FileShare to use. Default is None.</param>
    /// <returns>The set</returns>
    public FPath Open(
      Action<FileStream, FPath> action,
      FileMode mode,
      FileAccess access,
      FileShare share) {
      foreach (var path in _paths)
        using (var stream = File.Open(path, mode, access, share))
          action(stream, Create(path, this));

      return this;
    }

    /// <summary>Returns the previous path collection. Use this to end a sequence of commands on a path obtained from a
    ///   previous path.
    ///   <example>
    ///     <code>
    /// Path.Get("c:\temp")
    ///     .CreateSubDirectory("foo")
    ///         .CreateFile("bar.txt", "This is the bar file.")
    ///         .End()
    ///         .CreateFile("baz.txt", "This is the bar file.")
    ///         .End()
    ///     .End()
    ///     .CreateFile("foo.txt", "This is the foo file.");
    /// </code>
    ///   </example>
    /// </summary>
    /// <returns>The previous path collection.</returns>
    public FPath End() => Previous();

    /// <summary>Runs the provided process function on the content of the file for the current path and writes the result back
    ///   to the file.</summary>
    /// <param name="processFunction">The processing function.</param>
    /// <returns>The set.</returns>
    public FPath Process(Func<string, string> processFunction) => Process((p, s) => processFunction(s));

    /// <summary>Runs the provided process function on the content of the file for the current path and writes the result back
    ///   to the file.</summary>
    /// <param name="processFunction">The processing function.</param>
    /// <returns>The set.</returns>
    public FPath Process(Func<FPath, string, string> processFunction) {
      foreach (var path in _paths) {
        if (Directory.Exists(path)) continue;
        var p = Create(path, this);
        var read = File.ReadAllText(path);
        File.WriteAllText(path, processFunction(p, read));
      }

      return this;
    }

    /// <summary>Runs the provided process function on the content of the file for the current path and writes the result back
    ///   to the file.</summary>
    /// <param name="processFunction">The processing function.</param>
    /// <returns>The set.</returns>
    public FPath Process(Func<byte[], byte[]> processFunction) => Process((p, s) => processFunction(s));

    /// <summary>Runs the provided process function on the content of the file for the current path and writes the result back
    ///   to the file.</summary>
    /// <param name="processFunction">The processing function.</param>
    /// <returns>The set.</returns>
    public FPath Process(Func<FPath, byte[], byte[]> processFunction) {
      foreach (var path in _paths) {
        if (Directory.Exists(path)) continue;
        var p = Create(path, this);
        var read = File.ReadAllBytes(path);
        File.WriteAllBytes(path, processFunction(p, read));
      }

      return this;
    }

    /// <summary>Reads all text in files in the set.</summary>
    /// <returns>The string as read from the files.</returns>
    public string Read() =>
      string.Join("",
        from p in _paths
        where !Directory.Exists(p)
        select File.ReadAllText(p));

    /// <summary>Reads all text in files in the set.</summary>
    /// <param name="encoding">The encoding to use when reading the file.</param>
    /// <returns>The string as read from the files.</returns>
    public string Read(Encoding encoding) =>
      string.Join("",
        from p in _paths
        where !Directory.Exists(p)
        select File.ReadAllText(p, encoding));

    /// <summary>Reads all text in files in the set and hands the results to the provided action.</summary>
    /// <param name="action">An action that takes the content of the file.</param>
    /// <returns>The set</returns>
    public FPath Read(Action<string> action) => Read((s, p) => action(s));

    /// <summary>Reads all text in files in the set and hands the results to the provided action.</summary>
    /// <param name="action">An action that takes the content of the file.</param>
    /// <param name="encoding">The encoding to use when reading the file.</param>
    /// <returns>The set</returns>
    public FPath Read(Action<string> action, Encoding encoding) => Read((s, p) => action(s), encoding);

    /// <summary>Reads all text in files in the set and hands the results to the provided action.</summary>
    /// <param name="action">An action that takes the content of the file and its path.</param>
    /// <returns>The set</returns>
    public FPath Read(Action<string, FPath> action) {
      foreach (var path in _paths) action(File.ReadAllText(path), Create(path, this));
      return this;
    }

    /// <summary>Reads all text in files in the set and hands the results to the provided action.</summary>
    /// <param name="action">An action that takes the content of the file and its path.</param>
    /// <param name="encoding">The encoding to use when reading the file.</param>
    /// <returns>The set</returns>
    public FPath Read(Action<string, FPath> action, Encoding encoding) {
      foreach (var path in _paths) action(File.ReadAllText(path, encoding), Create(path, this));
      return this;
    }

    /// <summary>Reads all the bytes in the files in the set.</summary>
    /// <returns>The bytes from the files.</returns>
    public byte[] ReadBytes() {
      var bytes = (
        from p in _paths
        where !Directory.Exists(p)
        select File.ReadAllBytes(p)
      ).ToList();
      if (!bytes.Any()) return new byte[] { };
      if (bytes.Count() == 1) return bytes.First();
      var result = new byte[bytes.Aggregate(0, (i, b) => i + b.Length)];
      var offset = 0;
      foreach (var b in bytes) {
        b.CopyTo(result, offset);
        offset += b.Length;
      }

      return result;
    }

    /// <summary>Reads all the bytes in a file and hands them to the provided action.</summary>
    /// <param name="action">An action that takes an array of bytes.</param>
    /// <returns>The set</returns>
    public FPath ReadBytes(Action<byte[]> action) => ReadBytes((b, p) => action(b));

    /// <summary>Reads all the bytes in a file and hands them to the provided action.</summary>
    /// <param name="action">An action that takes an array of bytes and a path.</param>
    /// <returns>The set</returns>
    public FPath ReadBytes(Action<byte[], FPath> action) {
      foreach (var path in _paths) action(File.ReadAllBytes(path), Create(path, this));
      return this;
    }

    public string[] ToStringArray() => _paths.ToArray();

    /// <summary>Adds several paths to the current one and makes one set out of the result.</summary>
    /// <param name="paths">The paths to add to the current set.</param>
    /// <returns>The composite set.</returns>
    public FPath Add(params string[] paths) => Create(paths.Union(_paths), this);

    /// <summary>Adds several paths to the current one and makes one set out of the result.</summary>
    /// <param name="paths">The paths to add to the current set.</param>
    /// <returns>The composite set.</returns>
    public FPath Add(params FPath[] paths) => Create(paths.SelectMany(p => p._paths).Union(_paths), this);

    /// <summary>Gets all files under this path.</summary>
    /// <returns>The collection of file paths.</returns>
    public FPath AllFiles() => Files("*", true);

    /// <summary>The attributes for the file for the first path in the collection.</summary>
    /// <returns>The attributes</returns>
    public FileAttributes Attributes() => File.GetAttributes(FirstPath());

    /// <summary>The attributes for the file for the first path in the collection.</summary>
    /// <param name="action">An action to perform on the attributes of each file.</param>
    /// <returns>The attributes</returns>
    public FPath Attributes(Action<FileAttributes> action) => Attributes((p, fa) => action(fa));

    /// <summary>The attributes for the file for the first path in the collection.</summary>
    /// <param name="action">An action to perform on the attributes of each file.</param>
    /// <returns>The attributes</returns>
    public FPath Attributes(Action<FPath, FileAttributes> action) {
      foreach (var path in _paths.Where(path => !Directory.Exists(path))) action(Create(path, this), File.GetAttributes(path));
      return this;
    }

    /// <summary>Sets attributes on all files in the set.</summary>
    /// <param name="attributes">The attributes to set.</param>
    /// <returns>The set</returns>
    public FPath Attributes(FileAttributes attributes) => Attributes(p => attributes);

    /// <summary>Sets attributes on all files in the set.</summary>
    /// <param name="attributeFunction">A function that gives the attributes to set for each path.</param>
    /// <returns>The set</returns>
    public FPath Attributes(Func<FPath, FileAttributes> attributeFunction) {
      foreach (var p in _paths) File.SetAttributes(p, attributeFunction(Create(p, this)));
      return this;
    }

    /// <summary>Gets the creation time of the first path in the set</summary>
    /// <returns>The creation time</returns>
    public DateTime CreationTime() {
      var firstPath = FirstPath();
      return Directory.Exists(firstPath)
        ? Directory.GetCreationTime(firstPath)
        : File.GetCreationTime(firstPath);
    }

    /// <summary>Sets the creation time across the set.</summary>
    /// <param name="creationTime">The time to set.</param>
    /// <returns>The set</returns>
    public FPath CreationTime(DateTime creationTime) => CreationTime(p => creationTime);

    /// <summary>Sets the creation time across the set.</summary>
    /// <param name="creationTimeFunction">A function that returns the new creation time for each path.</param>
    /// <returns>The set</returns>
    public FPath CreationTime(Func<FPath, DateTime> creationTimeFunction) {
      foreach (var path in _paths) {
        var t = creationTimeFunction(Create(path, this));
        if (Directory.Exists(path)) Directory.SetCreationTime(path, t);
        else File.SetCreationTime(path, t);
      }

      return this;
    }

    /// <summary>Gets the UTC creation time of the first path in the set</summary>
    /// <returns>The UTC creation time</returns>
    public DateTime CreationTimeUtc() {
      var firstPath = FirstPath();
      return Directory.Exists(firstPath)
        ? Directory.GetCreationTimeUtc(firstPath)
        : File.GetCreationTimeUtc(firstPath);
    }

    /// <summary>Sets the UTC creation time across the set.</summary>
    /// <param name="creationTimeUtc">The time to set.</param>
    /// <returns>The set</returns>
    public FPath CreationTimeUtc(DateTime creationTimeUtc) => CreationTimeUtc(p => creationTimeUtc);

    /// <summary>Sets the UTC creation time across the set.</summary>
    /// <param name="creationTimeFunctionUtc">A function that returns the new time for each path.</param>
    /// <returns>The set</returns>
    public FPath CreationTimeUtc(Func<FPath, DateTime> creationTimeFunctionUtc) {
      foreach (var path in _paths) {
        var t = creationTimeFunctionUtc(Create(path, this));
        if (Directory.Exists(path)) Directory.SetCreationTimeUtc(path, t);
        else File.SetCreationTimeUtc(path, t);
      }

      return this;
    }

    /// <summary>Gets the last access time of the first path in the set</summary>
    /// <returns>The last access time</returns>
    public DateTime LastAccessTime() {
      var firstPath = FirstPath();
      return Directory.Exists(firstPath)
        ? Directory.GetLastAccessTime(firstPath)
        : File.GetLastAccessTime(firstPath);
    }

    /// <summary>Sets the last access time across the set.</summary>
    /// <param name="lastAccessTime">The time to set.</param>
    /// <returns>The set</returns>
    public FPath LastAccessTime(DateTime lastAccessTime) => LastAccessTime(p => lastAccessTime);

    /// <summary>Sets the last access time across the set.</summary>
    /// <param name="lastAccessTimeFunction">A function that returns the new time for each path.</param>
    /// <returns>The set</returns>
    public FPath LastAccessTime(Func<FPath, DateTime> lastAccessTimeFunction) {
      foreach (var path in _paths) {
        var t = lastAccessTimeFunction(Create(path, this));
        if (Directory.Exists(path)) Directory.SetLastAccessTime(path, t);
        else File.SetLastAccessTime(path, t);
      }

      return this;
    }

    /// <summary>Gets the last access UTC time of the first path in the set</summary>
    /// <returns>The last access UTC time</returns>
    public DateTime LastAccessTimeUtc() {
      var firstPath = FirstPath();
      return Directory.Exists(firstPath)
        ? Directory.GetLastAccessTimeUtc(firstPath)
        : File.GetLastAccessTimeUtc(firstPath);
    }

    /// <summary>Sets the UTC last access time across the set.</summary>
    /// <param name="lastAccessTimeUtc">The time to set.</param>
    /// <returns>The set</returns>
    public FPath LastAccessTimeUtc(DateTime lastAccessTimeUtc) => LastAccessTimeUtc(p => lastAccessTimeUtc);

    /// <summary>Sets the UTC last access time across the set.</summary>
    /// <param name="lastAccessTimeFunctionUtc">A function that returns the new time for each path.</param>
    /// <returns>The set</returns>
    public FPath LastAccessTimeUtc(Func<FPath, DateTime> lastAccessTimeFunctionUtc) {
      foreach (var path in _paths) {
        var t = lastAccessTimeFunctionUtc(Create(path, this));
        if (Directory.Exists(path)) Directory.SetLastAccessTimeUtc(path, t);
        else File.SetLastAccessTimeUtc(path, t);
      }

      return this;
    }

    /// <summary>Gets the last write time of the first path in the set</summary>
    /// <returns>The last write time</returns>
    public DateTime LastWriteTime() {
      var firstPath = FirstPath();
      return Directory.Exists(firstPath)
        ? Directory.GetLastWriteTime(firstPath)
        : File.GetLastWriteTime(firstPath);
    }

    /// <summary>Sets the last write time across the set.</summary>
    /// <param name="lastWriteTime">The time to set.</param>
    /// <returns>The set</returns>
    public FPath LastWriteTime(DateTime lastWriteTime) => LastWriteTime(p => lastWriteTime);

    /// <summary>Sets the last write time across the set.</summary>
    /// <param name="lastWriteTimeFunction">A function that returns the new time for each path.</param>
    /// <returns>The set</returns>
    public FPath LastWriteTime(Func<FPath, DateTime> lastWriteTimeFunction) {
      foreach (var path in _paths) {
        var t = lastWriteTimeFunction(Create(path, this));
        if (Directory.Exists(path)) Directory.SetLastWriteTime(path, t);
        else File.SetLastWriteTime(path, t);
      }

      return this;
    }

    /// <summary>Gets the last write UTC time of the first path in the set</summary>
    /// <returns>The last write UTC time</returns>
    public DateTime LastWriteTimeUtc() {
      var firstPath = FirstPath();
      return Directory.Exists(firstPath)
        ? Directory.GetLastWriteTimeUtc(firstPath)
        : File.GetLastWriteTimeUtc(firstPath);
    }

    /// <summary>Sets the UTC last write time across the set.</summary>
    /// <param name="lastWriteTimeUtc">The time to set.</param>
    /// <returns>The set</returns>
    public FPath LastWriteTimeUtc(DateTime lastWriteTimeUtc) => LastWriteTimeUtc(p => lastWriteTimeUtc);

    /// <summary>Sets the UTC last write time across the set.</summary>
    /// <param name="lastWriteTimeFunctionUtc">A function that returns the new time for each path.</param>
    /// <returns>The set</returns>
    public FPath LastWriteTimeUtc(Func<FPath, DateTime> lastWriteTimeFunctionUtc) {
      foreach (var path in _paths) {
        var t = lastWriteTimeFunctionUtc(Create(path, this));
        if (Directory.Exists(path)) Directory.SetLastWriteTimeUtc(path, t);
        else File.SetLastWriteTimeUtc(path, t);
      }

      return this;
    }

    /// <summary>Goes up the specified number of levels on each path in the set. Never goes above the root of the drive.</summary>
    /// <returns>The new set</returns>
    public FPath Up() => Up(1);

    /// <summary>Goes up the specified number of levels on each path in the set. Never goes above the root of the drive.</summary>
    /// <param name="levels">The number of levels to go up.</param>
    /// <returns>The new set</returns>
    public FPath Up(int levels) {
      var result = new HashSet<string>();
      foreach (var path in _paths) {
        var str = path;
        for (var i = 0; i < levels; i++) {
          var strUp = SystemPath.GetDirectoryName(str);
          if (strUp == null) break;
          str = strUp;
        }

        result.Add(str);
      }

      return Create(result, this);
    }

    /// <summary>Writes to all files in the set using UTF8.</summary>
    /// <param name="text">The text to write.</param>
    /// <returns>The set</returns>
    public FPath Write(string text) => Write(p => text, false);

    /// <summary>Writes to all files in the set using UTF8.</summary>
    /// <param name="text">The text to write.</param>
    /// <param name="append">True if the text should be appended to the existing content. Default is false.</param>
    /// <returns>The set</returns>
    public FPath Write(string text, bool append) => Write(p => text, append);

    /// <summary>Writes to all files in the set.</summary>
    /// <param name="text">The text to write.</param>
    /// <param name="encoding">The encoding to use.</param>
    /// <returns>The set</returns>
    public FPath Write(string text, Encoding encoding) => Write(p => text, encoding, false);

    /// <summary>Writes to all files in the set.</summary>
    /// <param name="text">The text to write.</param>
    /// <param name="encoding">The encoding to use.</param>
    /// <param name="append">True if the text should be appended to the existing content. Default is false.</param>
    /// <returns>The set</returns>
    public FPath Write(string text, Encoding encoding, bool append) => Write(p => text, encoding, append);

    /// <summary>Writes to all files in the set.</summary>
    /// <param name="textFunction">A function that returns the text to write for each path.</param>
    /// <param name="append">True if the text should be appended to the existing content. Default is false.</param>
    /// <returns>The set</returns>
    public FPath Write(Func<FPath, string> textFunction, bool append) => Write(textFunction, Encoding.GetEncoding("utf-8"), append);

    /// <summary>Writes to all files in the set.</summary>
    /// <param name="textFunction">A function that returns the text to write for each path.</param>
    /// <param name="encoding">The encoding to use.</param>
    /// <param name="append">True if the text should be appended to the existing content. Default is false.</param>
    /// <returns>The set</returns>
    public FPath Write(Func<FPath, string> textFunction, Encoding encoding, bool append) {
      foreach (var p in _paths) {
        EnsureDirectoryExists(p);
        if (append) File.AppendAllText(p, textFunction(Create(p, this)), encoding);
        else File.WriteAllText(p, textFunction(Create(p, this)), encoding);
      }

      return this;
    }

    /// <summary>Writes to all files in the set.</summary>
    /// <param name="bytes">The byte array to write.</param>
    /// <returns>The set</returns>
    public FPath Write(byte[] bytes) => Write(p => bytes);

    /// <summary>Writes to all files in the set.</summary>
    /// <param name="byteFunction">A function that returns a byte array to write for each path.</param>
    /// <returns>The set</returns>
    public FPath Write(Func<FPath, byte[]> byteFunction) {
      foreach (var p in _paths) {
        EnsureDirectoryExists(p);
        File.WriteAllBytes(p, byteFunction(Create(p, this)));
      }

      return this;
    }

    protected static FPath Create(IEnumerable<string> paths, FPath previousPaths) => new FPath {_paths = paths, _previousPaths = previousPaths};

    protected static FPath Create(IEnumerable<FPath> paths) => Create(paths.SelectMany(p => p._paths), null);

    protected static FPath Create(IEnumerable<string> paths) => Create(paths, null);

    protected static FPath Create(params string[] paths) => Create((IEnumerable<string>) paths);

    protected static FPath Create() => Create(new string[] { });

    protected static FPath Create(params FPath[] paths) => Create((IEnumerable<FPath>) paths);

    protected static FPath Create(string path) => Create(path, (FPath) null);

    protected static FPath Create(string path, FPath previousPaths) => new FPath {_paths = new[] {path}, _previousPaths = previousPaths};

    protected bool IsSameAs(FPath path) {
      var dict = _paths.ToDictionary(s => s, s => false);
      foreach (var p in path._paths) {
        if (!dict.ContainsKey(p)) return false;
        dict[p] = true;
      }

      return !dict.ContainsValue(false);
    }

    protected string FirstPath() {
      var first = _paths.FirstOrDefault();
      if (first != null) return first;
      throw new InvalidOperationException(
        "Can't get the first element of an empty collection.");
    }

    /*static FPath Create(IEnumerable<string> paths, FPath previousPaths)
        => Create(paths, (FPath) previousPaths);

    static FPath Create(string path, FPath previousPaths)
        => Create(path, (FPath) previousPaths);*/

    static void CopyFile(string srcPath, string destPath, Overwrite overwrite) {
      if (overwrite == Overwrite.Throw && File.Exists(destPath)) throw new InvalidOperationException($"File {destPath} already exists.");
      if (overwrite != Overwrite.Always &&
          (overwrite != Overwrite.Never || File.Exists(destPath)) &&
          (overwrite != Overwrite.IfNewer || File.Exists(destPath) &&
            File.GetLastWriteTime(srcPath) <= File.GetLastWriteTime(destPath))) return;
      var dir = SystemPath.GetDirectoryName(destPath);
      if (dir == null) throw new InvalidOperationException($"Directory {destPath} not found.");
      if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
      File.Copy(srcPath, destPath, true);
    }

    static void CopyDirectory(
      string source, string destination, Overwrite overwrite, bool recursive) {
      if (!Directory.Exists(destination)) Directory.CreateDirectory(destination);
      if (recursive)
        foreach (var subdirectory in Directory.GetDirectories(source)) {
          if (subdirectory == null) continue;
          CopyDirectory(
            subdirectory,
            SystemPath.Combine(
              destination,
              SystemPath.GetFileName(subdirectory)),
            overwrite, true);
        }

      foreach (var file in Directory.GetFiles(source)) {
        if (file == null) continue;
        CopyFile(
          file, SystemPath.Combine(
            destination, SystemPath.GetFileName(file)), overwrite);
      }
    }

    static bool MoveFile(string srcPath, string destPath, Overwrite overwrite) {
      if (overwrite == Overwrite.Throw && File.Exists(destPath)) throw new InvalidOperationException($"File {destPath} already exists.");
      if (overwrite != Overwrite.Always && (overwrite != Overwrite.Never || File.Exists(destPath)) &&
          (overwrite != Overwrite.IfNewer ||
           File.Exists(destPath) && File.GetLastWriteTime(srcPath) <= File.GetLastWriteTime(destPath)))
        return false;
      EnsureDirectoryExists(destPath);
      File.Delete(destPath);
      File.Move(srcPath, destPath);
      return true;
    }

    public void EnsureDirectoryExists() {
      foreach (var p in _paths)
        EnsureDirectoryExists(p);
    }

    static void EnsureDirectoryExists(string destPath) {
      var fi = new FileInfo(destPath);
      var dir = fi.Extension.HasValue() ? fi.Directory.FullName : fi.FullName;
      if (dir == null) throw new InvalidOperationException($"Directory {destPath} not found.");
      if (!Directory.Exists(dir)) Directory.CreateDirectory(dir);
    }

    static bool MoveDirectory(
      string source, string destination, Overwrite overwrite) {
      var everythingMoved = true;
      if (!Directory.Exists(destination)) Directory.CreateDirectory(destination);
      foreach (var subdirectory in Directory.GetDirectories(source)) {
        if (subdirectory == null) continue;
        everythingMoved &=
          MoveDirectory(subdirectory,
            SystemPath.Combine(destination, SystemPath.GetFileName(subdirectory)), overwrite);
      }

      foreach (var file in Directory.GetFiles(source)) {
        if (file == null) continue;
        everythingMoved &=
          MoveFile(file,
            SystemPath.Combine(destination, SystemPath.GetFileName(file)), overwrite);
      }

      if (everythingMoved) Directory.Delete(source);
      return everythingMoved;
    }
  }
}