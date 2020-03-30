using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Schema.Generation;
using Newtonsoft.Json.Serialization;

namespace Mutuo.Tools {
  /// <summary>
  ///   Tool for generating json schemas
  /// </summary>
  public static class SchemaTool {
    /// <summary>
    ///   Save a schema file for a type
    /// </summary>
    public static async Task GenerateSchema(FileInfo assembly, string type, DirectoryInfo dir) {
      var a = Assembly.LoadFrom(assembly.FullName);
      var t = a.GetType(type);
      var g = new JSchemaGenerator {
        DefaultRequired = Required.DisallowNull, ContractResolver =
          new DefaultContractResolver {
            NamingStrategy = new CamelCaseNamingStrategy()
          }
      };
      var schema = g.Generate(t);
      await File.WriteAllTextAsync($"{dir.FullName}/{t.Name}.schema.json", schema.ToString());
    }
  }
}