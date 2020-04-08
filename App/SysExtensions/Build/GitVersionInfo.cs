using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Medallion.Shell;
using Newtonsoft.Json.Linq;
using Semver;
using Serilog;
using Serilog.Core;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Text;

namespace SysExtensions.Build {
  public class GitVersionInfo {
    public string SemVer          { get; set; }
    public string BranchName      { get; set; }
    public string MajorMinorPatch { get; set; }
    public string NuGetVersionV2  { get; set; }

    public int Major { get; set; }
    public int Minor { get; set; }
    public int Path  { get; set; }

    /// <summary>Use github to work out the current version</summary>
    public static async Task<SemVersion> DiscoverSemVer(Type typeToDetectVersion, ILogger log = null) {
      log = log ?? Log.Logger ?? Logger.None;
      if (FPath.Current.DirOfParent(".git").Exists) {
        var outputLines = new List<string>();
        var process = Command.Run("dotnet", "gitversion");
        await process.StandardOutput.PipeToAsync(outputLines);
        await process.Task;
        try {
          var jVersion = JObject.Parse(outputLines.Join("\n"));
          var gitVersion = jVersion.ToObject<GitVersionInfo>();
          log.Debug("{Noun} - '.git/' detected. Discovered version: {Version}", nameof(GitVersionInfo), gitVersion.SemVer);
          return SemVersion.Parse(gitVersion.SemVer);
        }
        catch (Exception ex) {
          throw new InvalidOperationException($"Unable to parse result from gitversion: {outputLines.Join(" ")}", ex);
        }
      }
      var assemblyVersion = RuntimeSemVer(typeToDetectVersion);
      log.Debug("{Noun} - Using assembly version: {Version}", nameof(GitVersionInfo), assemblyVersion);
      return assemblyVersion;
    }

    public static SemVersion RuntimeSemVer(Type type) =>
      SemVersion.Parse(type.Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ??
                       throw new InvalidOperationException($"Can't find {type.Assembly.GetName().Name} InformationalVersion"));
  }

  public static class SemVerEx {
    public static string AssemblyVersion(this SemVersion v) => $"{v.Major}.{v.Minor}.{v.Patch}";
  }
}