using System;
using System.CommandLine;
using System.CommandLine.DragonFruit;
using System.Threading.Tasks;

namespace Mutuo.Tools {
  class Program {
    static async Task<int> Main(string[] args) {
      var root = new RootCommand();
      var cmdSchema = new Command("schema");
      cmdSchema.ConfigureFromMethod(typeof(SchemaTool).GetMethod(nameof(SchemaTool.GenerateSchema)));
      root.AddCommand(cmdSchema);
      return await root.InvokeAsync(args);
    }
  }
}