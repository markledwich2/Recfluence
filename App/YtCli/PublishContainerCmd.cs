using System;
using System.Linq;
using System.Threading.Tasks;
using CommandLine;
using Medallion.Shell;
using Mutuo.Etl.Pipe;
using Mutuo.Tools;
using Serilog;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Text;

namespace YtCli {

  [Verb("publish-container")]
  public class PublishContainerCmd {
    [Option('p', HelpText = "Publish to registry, otherwise a local build only")]
    public bool PublishToRegistry { get; set; }
    
    public static async Task<ExitCode> PublishContainer(CmdCtx<PublishContainerCmd> ctx) {
      //var buildTools = new BuildTools(ctx.Log);
      //await buildTools.GitVersionUpdate();
      var v = await GitVersionInfo.Discover(ctx.Log);
      var container = ctx.Cfg.Container;
      var sln = FPath.Current.ParentWithFile("YtNetworks.sln", true);
      if (!sln.Exists) throw new InvalidOperationException("Can't find YtNetworks.sln file to organize build");
      var image = $"{container.Registry}/{container.ImageName}:{v.SemVer}";

      ctx.Log.Information("Building & publishing container {Image}", image);

      var shell = new Shell(o => o.WorkingDirectory(sln.Parent().FullPath));
      await RunShell(shell, ctx.Log, "docker", "build", "-t", image, "-f", "App/Dockerfile", ".");

      if (ctx.Option.PublishToRegistry)
        await RunShell(shell, ctx.Log, "docker", "push", image);

      return ExitCode.Success;
    }
    
    static async Task<Command> RunShell(Shell shell, ILogger log, string cmd, params object[] args) {
      log.Information($"Running command: {cmd} {args.Select(a => a.ToString()).Join(" ")}");
      var process = shell.Run(cmd, args);
      await process.StandardOutput.PipeToAsync(Console.Out);
      var res = await process.Task;
      if (!res.Success) {
        Console.Error.WriteLine($"command failed with exit code {res.ExitCode}: {res.StandardError}");
        throw new InvalidOperationException($"command ({cmd}) failed");
      }
      return process;
    }
  }
  
  
  
}