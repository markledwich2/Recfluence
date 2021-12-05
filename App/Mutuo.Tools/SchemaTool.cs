using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using CliFx;
using CliFx.Attributes;
using CliFx.Infrastructure;
using Medallion.Shell;
using Newtonsoft.Json;
using Newtonsoft.Json.Schema.Generation;
using Newtonsoft.Json.Serialization;
using SysExtensions.Collections;
using SysExtensions.IO;
using SysExtensions.Serialization;
using SysExtensions.Threading;

namespace Mutuo.Tools;

/// <summary>Tool for generating json schemas</summary>
[Command("schema")]
public class SchemaCmd : ICommand {
  [CommandOption("dir", shortName: 'd', Description = "the directory of the project to build containing the types")]
  public DirectoryInfo Dir { get; set; }

  [CommandOption("types", shortName: 't', Description = "| separated list of types to generate schemas for")]
  public string Types { get; set; }

  [CommandOption("ignore-required", shortName: 'i', Description = "if specified will not enforce required types")]
  public bool IgnoreRequired { get; set; }

  public async ValueTask ExecuteAsync(IConsole console) {
    // build proj
    var shell = new Shell(o => o.WorkingDirectory(Dir.FullName));

    var run = shell.Run("dotnet", "build");
    await run.StandardOutput.PipeToAsync(console.Output);
    var res = await run.Task;
    if (!res.Success) throw new InvalidOperationException($"build failed: {res.StandardError}");

    var projPath = Dir.FullName.AsFPath();
    var projFile = projPath.Files("*.csproj", recursive: false).FirstOrDefault() ?? throw new InvalidOperationException("Can't find project");
    var latestAssembly = projPath.Files($"{projFile.FileNameWithoutExtension}.dll", recursive: true)
        .OrderByDescending(f => f.FileInfo().LastWriteTime).FirstOrDefault() ??
      throw new InvalidOperationException("Can't find built assembly");

    var a = Assembly.LoadFrom(latestAssembly.FullPath);
    await Types.Split("|").BlockDo(async type => {
      var t = a.GetType(type) ?? throw new InvalidOperationException($"can't find type {type}");
      var g = new JSchemaGenerator {
        DefaultRequired = Required.DisallowNull,
        ContractResolver =
          new CoreSerializeContractResolver {
            NamingStrategy = new CamelCaseNamingStrategy()
          },
        GenerationProviders = { new StringEnumGenerationProvider() }
      };
      var schema = g.Generate(t);
      if (IgnoreRequired)
        foreach (var s in schema.InArray().WithDescendants(s => s.Properties.Values.Concat(s.Items)))
          s.Required.Clear();
      await File.WriteAllTextAsync($"{Dir.FullName}/{t.Name}.schema.json", schema.ToString());
    });
  }
}