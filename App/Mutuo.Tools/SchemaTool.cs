using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Medallion.Shell;
using Newtonsoft.Json;
using Newtonsoft.Json.Schema.Generation;
using Newtonsoft.Json.Serialization;
using SysExtensions.Collections;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Threading;

namespace Mutuo.Tools {
  /// <summary>Tool for generating json schemas</summary>
  public static class SchemaTool {
    /// <summary>Save a schema file for a type</summary>
    [DisplayName("schema")]
    public static async Task GenerateSchema(string types, DirectoryInfo dir, bool ignoreRequired) {
      // build proj
      var shell = new Shell(o => o.WorkingDirectory(dir.FullName));

      var run = shell.Run("dotnet", "build");
      await run.StandardOutput.PipeToAsync(Console.Out);
      var res = await run.Task;
      if (!res.Success) throw new InvalidOperationException($"build failed: {res.StandardError}");

      var projPath = dir.FullName.AsPath();
      var projFile = projPath.Files("*.csproj", false).FirstOrDefault() ?? throw new InvalidOperationException("Can't find project");
      var latestAssembly = projPath.Files($"{projFile.FileNameWithoutExtension}.dll", true)
                             .OrderByDescending(f => f.FileInfo().LastWriteTime).FirstOrDefault() ??
                           throw new InvalidOperationException("Can't find built assembly");

      var a = Assembly.LoadFrom(latestAssembly.FullPath);
      await types.Split("|").BlockAction(async type => {
        var t = a.GetType(type) ?? throw new InvalidOperationException($"can't find type {type}");
        var g = new JSchemaGenerator {
          DefaultRequired = Required.DisallowNull,
          ContractResolver =
            new CoreSerializeContractResolver {
              NamingStrategy = new CamelCaseNamingStrategy()
            },
          GenerationProviders = {new StringEnumGenerationProvider()}
        };
        var schema = g.Generate(t);
        if (ignoreRequired)
          foreach (var s in schema.AsEnumerable().WithDescendants(s => s.Properties.Values.Concat(s.Items)))
            s.Required.Clear();
        await File.WriteAllTextAsync($"{dir.FullName}/{t.Name}.schema.json", schema.ToString());
      });
    }
  }
}