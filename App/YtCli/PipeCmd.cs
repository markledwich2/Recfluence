using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Autofac;
using CommandLine;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions.Build;
using SysExtensions.Serialization;
using SysExtensions.Text;
using YtReader;

namespace YtCli {
  public class PipeCmd : PipeArgs {
    [Option('z', "cloudinstance", HelpText = "run this command in a container instance")]
    public bool LaunchContainer { get; set; }

    public static async Task<ExitCode> RunPipe(CmdCtx<PipeCmd> ctx) {
      var option = ctx.Option;
      ctx.Log.Information("Starting pipe run {RunId} for {Env} environment", option.RunId ?? option.Pipe);
      var pipeCtx = ctx.Scope.Resolve<Func<IPipeCtx>>()();
      if (option.RunId.HasValue() && pipeCtx.Id.ToString() != option.RunId)
        throw new InvalidOperationException($"The pipe runId {option.RunId} was supplied but a new one was created that doesn't match {pipeCtx.Id}");
      return await pipeCtx.RunPipe();
    }

    public static IPipeCtx PipeCtx(object option, RootCfg rootCfg, AppCfg cfg, IComponentContext scope, ILogger log) {
      var semver = GitVersionInfo.Semver(typeof(Program));

      var pipe = cfg.Pipe.JsonClone();
      pipe.Container ??= cfg.Container.JsonClone();
      pipe.Container.Tag ??= semver;

      pipe.Store ??= new PipeAppStorageCfg {
        Cs = cfg.Storage.DataStorageCs,
        Path = cfg.Storage.PipePath
      };
      pipe.Azure ??= new PipeAzureCfg {
        ResourceGroup = cfg.ResourceGroup,
        ServicePrincipal = cfg.ServicePrincipal,
        SubscriptionId = cfg.SubscriptionId
      };

      var runId = option switch {
        PipeCmd p => p.RunId.HasValue() ? PipeRunId.FromString(p.RunId) : PipeRunId.Create(p.Pipe),
        _ => null
      };

      var envVars = new Dictionary<string, string> {
        {nameof(RootCfg.Env), rootCfg.Env},
        {nameof(RootCfg.AppCfgSas), rootCfg.AppCfgSas.ToString()}
      };
      var pipeCtx = Pipes.CreatePipeCtx(pipe, runId, log, scope, new[] {typeof(YtDataUpdater).Assembly}, envVars);
      return pipeCtx;
    }
  }
}