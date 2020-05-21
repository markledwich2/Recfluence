using System;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using CommandLine;
using Medallion.Shell;
using Serilog;
using SysExtensions.Build;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Text;
using YtReader;

namespace YtCli {
  [Verb("publish-container")]
  public class PublishContainerCmd {
    [Option('p', HelpText = "Publish to registry, otherwise a local build only")]
    public bool PublishToRegistry { get; set; }

    [Option('n', HelpText = "Local Nuget Cache Directory")]
    public string LocalNugetCache { get; set; }

    public static async Task PublishContainer(CmdCtx<PublishContainerCmd> ctx) {
      //var buildTools = new BuildTools(ctx.Log);
      //await buildTools.GitVersionUpdate();

      var sw = Stopwatch.StartNew();
      var v = ctx.Scope.Resolve<VersionInfo>();
      var container = ctx.Cfg.Pipe.Default.Container;
      var sln = FPath.Current.ParentWithFile("YtNetworks.sln", true);
      if (!sln.Exists) throw new InvalidOperationException("Can't find YtNetworks.sln file to organize build");
      var image = $"{container.Registry}/{container.ImageName}:{v.Version}";

      ctx.Log.Information("Building & publishing container {Image}", image);

      var appDir = sln.FullPath;
      var shell = new Shell(o => o.WorkingDirectory(appDir));
      await RunShell(shell, ctx.Log, "docker", "build", "-t", image,
        "--build-arg", $"SEMVER={v.Version}",
        "--build-arg", $"ASSEMBLY_SEMVER={v.Version.MajorMinorPatch()}",
        ".");

      if (ctx.Option.PublishToRegistry)
        await RunShell(shell, ctx.Log, "docker", "push", image);

      ctx.Log.Information("Completed building docker image {Image} in {Duration}", image, sw.Elapsed.HumanizeShort());
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