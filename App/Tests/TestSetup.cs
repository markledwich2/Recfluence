using System;
using System.Threading.Tasks;
using Autofac;
using NUnit.Framework;
using Serilog;
using YtReader;

namespace Tests {
  public record TestCtx(ILifetimeScope Scope, ILogger Log, AppCfg App, RootCfg Root) : IDisposable {
    public void Dispose() => Scope?.Dispose();
    public T Resolve<T>() => Scope.Resolve<T>();
  }

  public static class TestSetup {
    public static async Task<TestCtx> TextCtx() {
      var (cfg, rootCfg, version) = await Setup.LoadCfg(basePath: Setup.SolutionDir.Combine("YtCli").FullPath);
      var log = Setup.CreateTestLogger();
      log.Information("Starting {TestName}", TestContext.CurrentContext.Test.Name);
      var appCtx = Setup.PipeAppCtxEmptyScope(rootCfg, cfg, version.Version);
      return new(Setup.MainScope(rootCfg, cfg, appCtx, version, log), log, cfg, rootCfg);
    }
  }
}