using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Medallion.Shell;
using Semver;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Pipe {
  public class LocalPipeWorker : IPipeWorker, IContainerLauncher {
    readonly SemVersion Version;

    public LocalPipeWorker(SemVersion version) => Version = version;

    public async Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log, CancellationToken cancel) =>
      await ids.BlockFunc(async id => {
        var runCfg = id.PipeCfg(ctx.PipeCfg);
        var image = runCfg.Container.FullContainerImageName(Version.PipeTag());
        var args = new[] {"run"}
          .Concat(ctx.AppCtx.EnvironmentVariables.SelectMany(e => new[] {"--env", $"{e.name}={e.value}"}))
          .Concat("--rm", "-i", image)
          .Concat(runCfg.Container.Exe)
          .Concat(id.PipeArgs())
          .ToArray<object>();
        var cmd = Command.Run("docker", args, o => o.CancellationToken(cancel)).RedirectTo(Console.Out);
        var res = await cmd.Task;
        PipeRunMetadata md = res.Success
          ? new() {Id = id}
          : new() {
            Id = id,
            ErrorMessage = await cmd.StandardError.ReadToEndAsync()
          };
        await md.Save(ctx.Store, log);
        return md;
      });

    public async Task RunContainer(string containerName, string fullImageName, (string name, string value)[] envVars, string[] args = null,
      bool returnOnStart = false, string exe = null,
      string groupName = null, ILogger log = null, CancellationToken cancel = default) {
      groupName ??= containerName;
      var dockerArgs = new[] {"run"}
        .Concat(envVars.SelectMany(e => new[] {"--env", $"{e.name}={e.value}"}))
        .Concat("--rm", "-i", fullImageName)
        .Concat(exe)
        .Concat(args)
        .NotNull()
        .ToArray<object>();
      log?.Debug($"LocalPipeWorker - launching docker: docker {exe ?? ""} {dockerArgs.Join(" ", o => o.ToString())}");
      var cmd = Command.Run("docker", dockerArgs, o => o.CancellationToken(cancel)).RedirectTo(Console.Out);
      var res = await cmd.Task;
      if (!res.Success) throw new InvalidOperationException($"Container {groupName} failed ({res.ExitCode}): {res.StandardError}");
    }
  }
}