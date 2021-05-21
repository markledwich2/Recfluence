using System;
using System.Threading.Tasks;
using Autofac;
using Nito.AsyncEx;
using NUnit.Framework;
using Serilog;
using YtReader;

namespace Tests {
  public record TestCtx(ILifetimeScope Scope, ILogger Log, AppCfg App, RootCfg Root) : IDisposable {
    public void Dispose() => Scope?.Dispose();
    public T Resolve<T>() => Scope.Resolve<T>();
  }

  public static class TestSetup {
    static readonly AsyncLazy<TestCtx> Ctx = new(async () => {
      var (cfg, rootCfg, version) = await Setup.LoadCfg(basePath: Setup.SolutionDir.Combine("YtCli").FullPath);
      var log = Setup.CreateLogger(rootCfg.Env, "Recfluence.Tests", version, cfg);
      var appCtx = Setup.PipeAppCtxEmptyScope(rootCfg, cfg, version.Version);
      return new(Setup.MainScope(rootCfg, cfg, appCtx, version, log), log, cfg, rootCfg);
    });

    public static async Task<TestCtx> TextCtx() {
      var ctx = await Ctx;
      ctx.Log.Information("Starting Test - {TestName}", TestContext.CurrentContext.Test.Name);
      return ctx;
    }
  }
}