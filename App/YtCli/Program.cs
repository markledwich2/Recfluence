using Autofac;
using CliFx;
using SysExtensions.Text;
using YtCli;
using YtReader;

var (cfg, root, version) = await Setup.LoadCfg(rootLogger: Setup.ConsoleLogger());
var ytAssembly = typeof(RecExportCmd).Assembly;
using var log = Setup.CreateLogger(root.Env, "Recfluence", version, cfg);
using var scope = Setup.MainScope(root, cfg, Setup.PipeAppCtxEmptyScope(root, cfg, version.Version), version, log, args);
using var cmdScope = scope.BeginLifetimeScope(c => {
  c.RegisterAssemblyTypes(typeof(ChannelInfoCmd).Assembly, ytAssembly)
    .AssignableTo<ICommand>();
});
var app = new CliApplicationBuilder()
  .AddCommandsFromThisAssembly()
  .AddCommandsFrom(ytAssembly)
  .UseTypeActivator(t => cmdScope.Resolve(t))
  .SetTitle("Recfluence")
  .SetVersion(version.Version.ToString())
  .Build();
log.Information("Starting cmd (recfluence {Args}) {Env} {Version}", args.Join(" "), root.Env, version.Version);
var res = await app.RunAsync(args);
log.Information("Completed cmd (recfluence {Args}) {Env} {Version}", args.Join(" "), root.Env, version.Version);
return res;