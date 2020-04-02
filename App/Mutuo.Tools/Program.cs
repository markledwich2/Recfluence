using System;
using System.CommandLine;
using System.CommandLine.DragonFruit;
using System.ComponentModel;
using System.Reflection;
using System.Threading.Tasks;
using Serilog;
using Serilog.Events;

namespace Mutuo.Tools {
  class Program {
    static async Task<int> Main(string[] args) {
      var root = new RootCommand();
      var log = new LoggerConfiguration()
        .WriteTo.Console(LogEventLevel.Information).CreateLogger();
      var buildTools = new BuildTools(log);
      root.AddCommand(typeof(SchemaTool), nameof(SchemaTool.GenerateSchema));
      root.AddCommand(buildTools, nameof(BuildTools.GitVersionUpdate));
      return await root.InvokeAsync(args);
    }
  }

  /// <summary>Utils for working with Commands</summary>
  public static class CommandHelper {
    /// <summary>Adds a command to the root based on a method signature and attributes</summary>
    public static void AddCommand(this RootCommand root, object instanceOrType, string method) {
      var type = instanceOrType is Type t ? t : instanceOrType.GetType();
      var m = type.GetMethod(method);
      var name = m.GetCustomAttribute<DisplayNameAttribute>().DisplayName ?? method;
      var c = new Command(name);
      c.ConfigureFromMethod(m, instanceOrType is Type ? null : instanceOrType);
      root.AddCommand(c);
    }
  }
}