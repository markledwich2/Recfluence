using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Autofac;
using Autofac.Util;
using CommandLine;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Reflection;
using SysExtensions.Security;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Pipe {
  /// <summary>
  ///   Context & Cfg for running a pipe command
  /// </summary>
  public interface IPipeCtx {
    PipeRunId        Id             { get; }
    ILogger          Log            { get; }
    ISimpleFileStore Store          { get; }
    PipeAppCfg       Cfg            { get; }
    Assembly[]       PipeAssemblies { get; }
  }

  public interface IPipeCtxScoped : IPipeCtx {
    IComponentContext Scope { get; set; }
  }

  class PipeCtx : IPipeCtxScoped {
    public PipeCtx() { }

    public PipeCtx(IPipeCtx ctx, PipeRunId id) {
      Log = ctx.Log;
      Store = ctx.Store;
      Cfg = ctx.Cfg;
      Id = id;
      PipeAssemblies = ctx.PipeAssemblies;
      Scope = (ctx as IPipeCtxScoped)?.Scope;
    }

    public ILogger           Log            { get; set; }
    public ISimpleFileStore  Store          { get; set; }
    public PipeAppCfg        Cfg            { get; set; }
    public PipeRunId         Id             { get; set; }
    public Assembly[]        PipeAssemblies { get; set; } = { };
    public IComponentContext Scope          { get; set; }
  }

  /// <summary>
  ///   A unique string for a pipe run. HUman readable and easily passable though commands
  /// </summary>
  public class PipeRunId {
    public PipeRunId(string name, string runId, int num) {
      Name = name;
      RunId = runId;
      Num = num;
    }

    public PipeRunId() { }

    public string Name  { get; set; }
    public string RunId { get; set; }
    public int    Num   { get; set; }

    public override string ToString() => $"{Name}|{RunId}|{Num}";

    public static PipeRunId Create(string name, int num = 0) => new PipeRunId(name, NewRunId(), num);

    public static string NewRunId() => $"{DateTime.UtcNow.FileSafeTimestamp()}_{Guid.NewGuid().ToShortString()}";

    public static PipeRunId FromString(string path) {
      var split = path.Split("/");
      if (split.Length < 3) throw new InvalidOperationException($"{path} doesn't have 3 components");
      return new PipeRunId {
        Name = split[0],
        RunId = split[1],
        Num = split[2].ParseInt()
      };
    }
  }

  public class PipeAppCfg {
    public PipeAppStorageCfg Store         { get; set; }
    public PipeAzureCfg      Azure         { get; set; }
    public ContainerCfg      Container     { get; set; }
    public bool              RunLocal      { get; set; }
    public int               LocalParallel { get; set; } = 2;
  }

  public class PipeAzureCfg {
    public string              SubscriptionId   { get; set; }
    public ServicePrincipalCfg ServicePrincipal { get; set; }
    public string              ResourceGroup    { get; set; }
  }

  public class ServicePrincipalCfg {
    public string ClientId  { get; set; }
    public string Secret    { get; set; }
    public string TennantId { get; set; }
  }

  public class ContainerCfg {
    public string     Registry                    { get; set; }
    public string     Name                        { get; set; }
    public string     ImageName                   { get; set; }
    public string     Tag                         { get; set; }
    public int        Cores                       { get; set; }
    public double     Mem                         { get; set; }
    public NameSecret RegistryCreds               { get; set; }
    public string     Region                      { get; set; } = Microsoft.Azure.Management.ResourceManager.Fluent.Core.Region.USWest2.Name;
    public string     EntryAssemblyPath           { get; set; }
    public string[]   ForwardEnvironmentVariables { get; set; }
  }

  public class PipeAppStorageCfg {
    public string     Cs   { get; set; }
    public StringPath Path { get; set; }
  }

  public static class Pipes {
    public static IPipeCtxScoped CreatePipeCtx(PipeAppCfg cfg, PipeRunId runId, ILogger log, IComponentContext getScope,
      IEnumerable<Assembly> pipeAssemblies = null) =>
      new PipeCtx {
        Cfg = cfg,
        Store = new AzureBlobFileStore(cfg.Store.Cs, cfg.Store.Path),
        Id = runId,
        PipeAssemblies = pipeAssemblies?.ToArray() ?? new Assembly[] { },
        Log = log,
        Scope = getScope
      };

    /// <summary>
    ///   Runs a pipeline to process a list of work in batches on multiple containers.
    ///   The transform is used to provide strong typing, but may not actually be run locally.
    /// </summary>
    public static async Task<IReadOnlyCollection<TOut>> RunPipe<TIn, TOut>(
      this IEnumerable<TIn> items, Func<IEnumerable<TIn>, Task<IEnumerable<TOut>>> transform, IPipeCtx ctx, int minBatch, int parallel, ILogger log) {
      var isPipe = transform.Method.GetCustomAttribute<PipeAttribute>() != null;
      if (!isPipe) throw new InvalidOperationException($"given transform '{transform.Method.Name}' must be a pipe");
      var pipeNme = transform.Method.Name;

      // batch and create state for containers to read
      var batches = await items.Batch(minBatch, parallel)
        .Select((g, i) => (Id: PipeRunId.Create(pipeNme, i), In: g.ToArray()))
        .BlockTransform(async b => {
          using var inStream = b.In.ToJsonlGzStream();
          await ctx.Store.Save(b.Id.InStatePath(), inStream);
          return (b.Id, inStatePath: b.Id.InStatePath());
        });


      if (ctx.Cfg.RunLocal) {
        var res = await batches
          .BlockTransform(async b => await RunPipe(new PipeCtx(ctx, b.Id)), ctx.Cfg.LocalParallel);
      }
      else {
        await AzureContainerRunner.RunBatch(ctx, batches.Select(b => b.Id), log);
      }

      var outState = await batches
        .BlockTransform(async b => { return await GetOutState<TOut>(ctx, b.Id); });

      return outState.SelectMany(m => m).ToReadOnly();
    }

    static IEnumerable<Assembly> PipeAssemblies(this IPipeCtx ctx) => Assembly.GetExecutingAssembly().AsEnumerable().Concat(ctx.PipeAssemblies);

    /// <summary>
    ///   Executes a pipe in this process
    /// </summary>
    public static async Task<ExitCode> RunPipe(this IPipeCtxScoped ctx) {
      var pipeMethods = ctx.PipeAssemblies().SelectMany(a => a.GetLoadableTypes())
        .SelectMany(t => t.GetRuntimeMethods().Where(m => m.GetCustomAttribute<PipeAttribute>() != null).Select(m => (Type: t, Method: m)))
        .ToKeyedCollection(m => m.Method.Name);

      var pipeName = ctx.Id.Name;
      var pipeType = pipeMethods[pipeName];
      if (pipeType == default) throw new InvalidOperationException($"Could not find pipe {pipeName}");
      if (!pipeType.Method.ReturnType.IsAssignableTo<Task>()) throw new InvalidOperationException($"Pipe {pipeName} must be async");

      var pipeInstance = ctx.Scope.Resolve(pipeType.Type);
      var method = pipeType.Method;
      var stateParams = method.GetParameters().Where(p => p.GetCustomAttribute<PipeArgAttribute>() == null).ToArray();
      if (stateParams.Length > 1) throw new InvalidOperationException("Only one pipe state parameter supported");

      var pipeParams = await method.GetParameters().BlockTransform(async p => {
        var argAttribute = p.GetCustomAttribute<PipeArgAttribute>();
        if (argAttribute == null) {
          using var inSr = await ctx.LoadInState();
          var genericType = p.ParameterType.GenericTypeArguments.FirstOrDefault() ?? 
                            throw new InvalidOperationException($"Expecting arg method {pipeType.Type}.{method.Name} parameter {p.Name} to be IEnumerable<Type>");
          var deserializeMethod = typeof(JsonlExtensions).GetMethod("LoadJsonlGz", new[] {typeof(Stream)})?.MakeGenericMethod(genericType)
                                  ?? throw new InvalidOperationException("LoadJsonlGz method not found ");
          return deserializeMethod.Invoke(null, new object[] {inSr});
        }

        var variableName = $"{pipeName}:{p.Name}";
        var stringValue = Environment.GetEnvironmentVariable(variableName);
        if (stringValue == null) {
          ctx.Log.Debug($"Unable to find pipe arg in environment variable '{variableName}'");
          return p.ParameterType.DefaultForType();
        }
        var value = Convert.ChangeType(stringValue, p.ParameterType);
        return value;
      });

      try {
        dynamic task = method.Invoke(pipeInstance, pipeParams.ToArray()) ??
                   throw new InvalidOperationException($"Method '{method.Name}' returned null, should be Task");
        if (method.ReturnType == typeof(Task)) {
          await task;
        }
        else {
          var pipeResult = await task;
          var listResult = (IEnumerable<object>) pipeResult;
          using var outStream = listResult.ToJsonlGzStream();
          await ctx.Store.Save(ctx.OutStatePath(), outStream);
        }
      }
      catch (Exception ex) {
        ctx.Log.Error(ex, "Pipe {Pipe} failed with error: {Error}", pipeName, ex.Message);
        return ExitCode.Error;
      }
      return ExitCode.Success;
    }

    static string OutStatePath(this PipeRunId id) => $"{id}/OutState.jsonl.gz";
    static string OutStatePath(this IPipeCtx ctx) => ctx.Id.OutStatePath();

    static async Task<IReadOnlyCollection<TOut>> GetOutState<TOut>(IPipeCtx ctx, PipeRunId id) {
      using var stream = await ctx.Store.Load(id.OutStatePath());
      return stream.LoadJsonlGz<TOut>();
    }

    public static string InStatePath(this PipeRunId id) => $"{id}/InState.jsonl.gz";
    public static string InStatePath(this IPipeCtx ctx) => ctx.Id.InStatePath();
    public static Task<Stream> LoadInState(this IPipeCtx ctx) => ctx.Store.Load(ctx.InStatePath());
  }

  public enum ExitCode {
    Success = 0,
    Error   = 10
  }

  /// <summary>
  ///   The application entrypoint for inner pipe dependencies and parallel tasks. Add this to your CLI as a verb
  ///   Not intended to be called by user. Seperately provide your own high level entrypoints with explicit parameters and
  ///   help.
  /// </summary>
  [Verb("pipe")]
  public class PipeArgs {
    [Option('p', HelpText = "Name of the pipe to run", Required = true)]
    public string Pipe { get; set; }
    [Option('r', HelpText = "The run id in the format Pipe/RunId/Num. No need to supply this if you are running this standalone.")]
    public string RunId { get; set; }
  }

  /// <summary>
  ///   Decorate any types that contain pipe functions.
  ///   The parameters will be populated from either the InState deserialized form blob storage, or from command line
  ///   parameters, or from ILifetimeScope
  /// </summary>
  [AttributeUsage(AttributeTargets.Method)]
  public class PipeAttribute : Attribute { }

  /// <summary>
  ///   Decorate a parameter that should come form the command line
  /// </summary>
  [AttributeUsage(AttributeTargets.Parameter)]
  public class PipeArgAttribute : Attribute { }
}