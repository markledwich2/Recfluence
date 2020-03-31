using System;
using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;
using Medallion.Shell;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Text;

namespace Mutuo.Tools {
  public class GitVersionInfo {
    public string SemVer          { get; set; }
    public string BranchName      { get; set; }
    public string MajorMinorPatch { get; set; }
    public string NuGetVersionV2  { get; set; }

    public int Major { get; set; }
    public int Minor { get; set; }
    public int Path  { get; set; }

    public const string VersionFileName = "gitVersion.json";

    /// <summary>
    ///   Use github to work out the current version
    /// </summary>
    public static async Task<GitVersionInfo> Discover(ILogger log) {
      var outputLines = new List<string>();
      var process = Command.Run("dotnet", "gitversion");
      await process.StandardOutput.PipeToAsync(outputLines);
      await process.Task;
      try {
        var jVerson = JObject.Parse(outputLines.Join("\n"));
        return jVerson.ToObject<GitVersionInfo>();
      }
      catch (Exception ex) {
        throw new InvalidOperationException($"Unable to parse result from gitversion: {outputLines.Join(" ")}", ex);
      }
    }

    public static string Semver(Type type) => 
      type.Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion;
  }
}