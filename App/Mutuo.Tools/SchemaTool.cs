using System;
using System.ComponentModel;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Schema.Generation;
using Newtonsoft.Json.Serialization;
using SysExtensions.Threading;

namespace Mutuo.Tools {
  /// <summary>Tool for generating json schemas</summary>
  public static class SchemaTool {
    /// <summary>Save a schema file for a type</summary>
    [DisplayName("schema")]
    public static async Task GenerateSchema(FileInfo assembly, string types, DirectoryInfo dir) {
      var a = Assembly.LoadFrom(assembly.FullName);
      await types.Split("|").BlockAction(async type => {
        var t = a.GetType(type) ?? throw new InvalidOperationException($"can't find type {type}");
        var g = new JSchemaGenerator {
          DefaultRequired = Required.DisallowNull,
          ContractResolver =
            new DefaultContractResolver {
              NamingStrategy = new CamelCaseNamingStrategy()
            },
          GenerationProviders = {new StringEnumGenerationProvider()}
        };
        var schema = g.Generate(t);
        await File.WriteAllTextAsync($"{dir.FullName}/{t.Name}.schema.json", schema.ToString());
      });
    }
  }
}