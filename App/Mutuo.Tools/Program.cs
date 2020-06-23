using System.Threading.Tasks;
using Autofac;
using CliFx;
using Serilog;
using Serilog.Events;

namespace Mutuo.Tools {
  class Program {
    static async Task<int> Main(string[] args) {
      var log = new LoggerConfiguration()
        .WriteTo.Console(LogEventLevel.Information).CreateLogger();
      var cb = new ContainerBuilder();
      cb.Register(_ => log);
      cb.RegisterAssemblyTypes(typeof(Program).Assembly).AssignableTo<ICommand>();
      using var scope = cb.Build();

      var app = new CliApplicationBuilder()
        .AddCommandsFromThisAssembly()
        .UseTypeActivator(t => scope.Resolve(t))
        .UseTitle("Mutuo Tools")
        .Build();
      return await app.RunAsync(args);
    }
  }
}