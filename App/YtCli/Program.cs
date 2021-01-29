using Autofac;
using CliFx;
using SysExtensions.Text;
using YtCli;
using YtReader;

var (cfg, root, version) = await Setup.LoadCfg(rootLogger: Setup.ConsoleLogger());
using var log = Setup.CreateLogger(root.Env, "Recfluence", version, cfg);
using var scope = Setup.MainScope(root, cfg, Setup.PipeAppCtxEmptyScope(root, cfg, version.Version), version, log);
using var cmdScope = scope.BeginLifetimeScope(c => { c.RegisterAssemblyTypes(typeof(ChannelInfoCmd).Assembly).AssignableTo<ICommand>(); });
var app = new CliApplicationBuilder()
  .AddCommandsFromThisAssembly()
  .UseTypeActivator(t => cmdScope.Resolve(t))
  .UseTitle("Recfluence")
  .UseVersionText(version.Version.ToString())
  .Build();
log.Information("Starting cmd (recfluence {Args}) {Env} {Version}", args.Join(" "), root.Env, version.Version);
var res = await app.RunAsync(args);
log.Information("Completed cmd (recfluence {Args}) {Env} {Version}", args.Join(" "), root.Env, version.Version);
return res;