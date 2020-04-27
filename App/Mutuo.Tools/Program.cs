using System.CommandLine;
using System.Threading.Tasks;
using Serilog;
using Serilog.Events;

namespace Mutuo.Tools {
  class Program {
    static async Task<int> Main(string[] args) {
      var root = new RootCommand();
      var log = new LoggerConfiguration()
        .WriteTo.Console(LogEventLevel.Information).CreateLogger();
      root.AddCommand(typeof(SchemaTool), nameof(SchemaTool.GenerateSchema));
      return await root.InvokeAsync(args);
    }
  }
}