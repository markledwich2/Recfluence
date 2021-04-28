using System;
using Autofac.Extensions.DependencyInjection;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Serilog;
using YtReader;

var cfgDir = Setup.SolutionDir?.Combine("YtCli").FullPath ?? Environment.CurrentDirectory;
var (cfg, root, version) = await Setup.LoadCfg(cfgDir, Setup.ConsoleLogger());
using var log = Setup.CreateLogger(root.Env, "Recfluence", version, cfg);

// bellow is what we need to do to run pipes from functions. But we don't need to right now
//  we pre-build the the scope so we can provide a PipeAppCtx with a scope that's ready to resolve. 
// var appCtx = Setup.PipeAppCtxEmptyScope(root, cfg, version.Version);
// using var mainScope = Setup.MainScope(root, cfg, appCtx, version, log, args);
try {
  var host = new HostBuilder()
    .UseServiceProviderFactory(new AutofacServiceProviderFactory(c => {
      c.ConfigureScope(root, cfg, Setup.PipeAppCtxEmptyScope(root, cfg, version.Version), version, log, args);
    }))
    .ConfigureAppConfiguration(c => c.AddCommandLine(args))
    .ConfigureFunctionsWorkerDefaults()
    .UseSerilog(log)
    .Build();
  await host.RunAsync();
}
catch (Exception ex) {
  log.Fatal(ex, "Unhandled error in YtFunction: {Message}", ex.Message);
}