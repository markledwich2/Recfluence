using System;
using System.CommandLine;
using System.CommandLine.DragonFruit;
using System.ComponentModel;
using System.Reflection;

namespace Mutuo.Tools {
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