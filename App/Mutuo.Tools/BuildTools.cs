using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml;
using System.Xml.Linq;
using Serilog;
using Serilog.Core;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Serialization;

namespace Mutuo.Tools {
  public class BuildTools {
    public ILogger Log { get; }

    public BuildTools(ILogger log) => Log = log;

    /// <summary>
    ///   Updates projects with git versions
    /// </summary>
    [DisplayName("gitversion")]
    public async Task GitVersionUpdate(DirectoryInfo dir = null, bool dry = false) {
      var rootDir = dir == null ? FPath.Current.ParentWithFile("*.sln", true) : new FPath(dir.FullName);
      var versionInfo = await GitVersionInfo.Discover(Log);

      var projFiles = rootDir.Files("*.csproj", true);
      foreach (var f in dry ? projFiles.First() : projFiles)
        await UpdateProject(f);
      
      async Task UpdateProject(FPath f) {
        var projElement = await LoadProj(f);
        var propGroup = projElement.Element("PropertyGroup") ?? throw new InvalidOperationException("Can't find PropertyGroup");

        UpdateElement("AssemblyVersion", versionInfo.MajorMinorPatch);
        UpdateElement("InformationalVersion", versionInfo.SemVer);
        UpdateElement("PackageVersion", versionInfo.SemVer);

        await Save();
        
        void UpdateElement(string name, string value) {
          var e = propGroup.Element(name);
          if (e == null)
            propGroup.Add(new XElement(name, value));
          else
            e.Value = value;
        }
        
        async Task Save() {
          var suffix = dry ? ".dry" : "";
          var projFile = @$"{f.DirectoryName}\{f.FileNameWithoutExtension}{suffix}{f.Extension}";
          using var xw = XmlWriter.Create(projFile,
            new XmlWriterSettings {OmitXmlDeclaration = true, Indent = true, Async = true});
          await projElement.SaveAsync(xw, CancellationToken.None);
          Log.Information($"Updated {f}");
        }

        static async Task<XElement> LoadProj(FPath f) {
          using var stream = f.OpenText();
          return await XElement.LoadAsync(stream, LoadOptions.None, CancellationToken.None);
        }
      }
    }
  }
}