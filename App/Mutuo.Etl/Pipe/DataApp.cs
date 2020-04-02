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
  /// <summary>Context & Cfg for running a pipe command</summary>
  public interface IPipeCtx {
    PipeRunId         Id             { get; }
    ILogger           Log            { get; }
    ISimpleFileStore  Store          { get; }
    PipeAppCfg        Cfg            { get; }
    Assembly[]        PipeAssemblies { get; }
    IComponentContext Scope          { get; set; }

    /// <summary>Environment variables to forward</summary>
    IDictionary<string, string> EnvVars { get; }
  }

  class PipeCtx : IPipeCtx {
    public PipeCtx() { }

    public PipeCtx(IPipeCtx ctx, PipeRunId id) {
      Log = ctx.Log;
      Store = ctx.Store;
      Cfg = ctx.Cfg;
      Id = id;
      PipeAssemblies = ctx.PipeAssemblies;
      Scope = ctx.Scope;
    }

    public ILogger                     Log            { get; set; }
    public ISimpleFileStore            Store          { get; set; }
    public PipeAppCfg                  Cfg            { get; set; }
    public PipeRunId                   Id             { get; set; }
    public Assembly[]                  PipeAssemblies { get; set; } = { };
    public IComponentContext           Scope          { get; set; }
    public IDictionary<string, string> EnvVars        { get; set; } = new Dictionary<string, string>();
  }

  /// <summary>A unique string for a pipe run. Human readable and easily passable though commands.</summary>
  public class PipeRunId {
    public PipeRunId(string name, string groupId, int num) {
      Name = name;
      GroupId = groupId;
      Num = num;
    }

    public PipeRunId() { }

    public string Name { get; set; }

    /// <summary>A unique string for a batch of pipe run's that are part of the same operation</summary>
    public string GroupId { get; set; }
    public int Num { get;        set; }

    public static PipeRunId Create(string name, int num = 0) => new PipeRunId(name, NewGroupId(), num);

    public static string NewGroupId() => $"{DateTime.UtcNow.ToString("yyyy-MM-dd-hh-mm-ss")}-{Guid.NewGuid().ToShortString(4)}";

    public override string ToString() => $"{Name}|{GroupId}|{Num}";

    public static PipeRunId FromString(string path) {
      var split = path.Split("|");
      if (split.Length < 3) throw new InvalidOperationException($"{path} doesn't have 3 components");
      return new PipeRunId {
        Name = split[0],
        GroupId = split[1],
        Num = split[2].ParseInt()
      };
    }
  }

  public enum PipeRunLocation {
    Container,
    LocalContainer,
    LocalThread
  }

  public class PipeAppCfg {
    public PipeAppStorageCfg Store         { get; set; }
    public PipeAzureCfg      Azure         { get; set; }
    public ContainerCfg      Container     { get; set; }
    public PipeRunLocation   Location      { get; set; }
    public int               LocalParallel { get; set; } = 2;
    public Uri               DockerUri     { get; set; } = new Uri("npipe://./pipe/docker_engine");
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
    public string     Registry      { get; set; }
    public string     Name          { get; set; }
    public string     ImageName     { get; set; }
    public string     Tag           { get; set; }
    public int        Cores         { get; set; }
    public double     Mem           { get; set; }
    public NameSecret RegistryCreds { get; set; }
    public string     Region        { get; set; } = Microsoft.Azure.Management.ResourceManager.Fluent.Core.Region.USWest2.Name;
  }

  public class PipeAppStorageCfg {
    public string     Cs   { get; set; }
    public StringPath Path { get; set; }
  }

  public static class Pipes {
    public static IPipeCtx CreatePipeCtx(PipeAppCfg cfg, PipeRunId runId, ILogger log, IComponentContext getScope,
      IEnumerable<Assembly> pipeAssemblies = null, IEnumerable<KeyValuePair<string, string>> environmentVars = null) =>
      new PipeCtx {
        Cfg = cfg,
        Store = new AzureBlobFileStore(cfg.Store.Cs, cfg.Store.Path),
        Id = runId,
        PipeAssemblies = pipeAssemblies?.ToArray() ?? new Assembly[] { },
        Log = log,
        Scope = getScope,
        EnvVars = new Dictionary<string, string>(environmentVars ?? new List<KeyValuePair<string, string>>())
      };

    /// <summary>Runs a pipeline to process a list of work in batches on multiple containers. The transform is used to provide
    ///   strong typing, but may not actually be run locally.</summary>
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

      IContainerRunner containerRunner = ctx.Cfg.Location switch {
        PipeRunLocation.Container => new AzureContainerRunner(),
        PipeRunLocation.LocalContainer => new LocalContainerRunner(),
        PipeRunLocation.LocalThread => new ThreadContainerRunner(),
        _ => throw new NotImplementedException($"{ctx.Cfg.Location}")
      };

      await containerRunner.RunBatch(ctx, batches.Select(b => b.Id).ToArray(), log);

      var outState = await batches
        .BlockTransform(async b => await GetOutState<TOut>(ctx, b.Id));

      return outState.SelectMany(m => m).ToReadOnly();
    }

    static IEnumerable<Assembly> PipeAssemblies(this IPipeCtx ctx) => Assembly.GetExecutingAssembly().AsEnumerable().Concat(ctx.PipeAssemblies);

    /// <summary>Executes a pipe in this process</summary>
    public static async Task<ExitCode> RunPipe(this IPipeCtx ctx) {
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
                            throw new InvalidOperationException(
                              $"Expecting arg method {pipeType.Type}.{method.Name} parameter {p.Name} to be IEnumerable<Type>");
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
        var value = ChangeType(stringValue, p.ParameterType);
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
          await ctx.Store.Save(ctx.Id.OutStatePath(), outStream);
        }
      }
      catch (Exception ex) {
        ctx.Log.Error(ex, "Pipe {Pipe} failed with error: {Error}", pipeName, ex.Message);
        return ExitCode.Error;
      }
      return ExitCode.Success;
    }

    static object ChangeType(string stringValue, Type t) {
      if (t.IsGenericType && t.GetGenericTypeDefinition() == typeof(Nullable<>)) {
        if (stringValue.HasValue()) {
          var typeArgument = t.GetGenericArguments()[0];
          var value = Convert.ChangeType(stringValue, typeArgument);
          // get the Nullable<T>(T) constructor
          var ctor = t.GetConstructor(new[] {typeArgument}) ?? throw new InvalidOperationException($"Expected constructor for type '{typeArgument}'");
          return ctor.Invoke(new[] {value});
        }
        return t.DefaultForType();
      }
      return Convert.ChangeType(stringValue, t);
    }

    public static string StatePath(this PipeRunId id) => $"{id.Name}/{id.GroupId}/{id.Num}";
    static string OutStatePath(this PipeRunId id) => $"{id.StatePath()}.OutState.jsonl.gz";
    public static string InStatePath(this PipeRunId id) => $"{id.StatePath()}.InState.jsonl.gz";

    static async Task<IReadOnlyCollection<TOut>> GetOutState<TOut>(IPipeCtx ctx, PipeRunId id) {
      using var stream = await ctx.Store.Load(id.OutStatePath());
      return stream.LoadJsonlGz<TOut>();
    }

    public static string InStatePath(this IPipeCtx ctx) => ctx.Id.InStatePath();
    public static Task<Stream> LoadInState(this IPipeCtx ctx) => ctx.Store.Load(ctx.InStatePath());
  }

  public enum ExitCode {
    Success = 0,
    Error   = 10
  }

  /// <summary>The application entrypoint for inner pipe dependencies and parallel tasks. Add this to your CLI as a verb Not
  ///   intended to be called by user. Seperately provide your own high level entrypoints with explicit parameters and help.</summary>
  [Verb("pipe")]
  public class PipeArgs {
    [Option('p', HelpText = "Name of the pipe to run")]
    public string Pipe { get; set; }

    [Option('r', HelpText = "The run id in the format Pipe/Group/Num. No need to supply this if you are running this standalone.")]
    public string RunId { get; set; }
  }

  /// <summary>Decorate any types that contain pipe functions. The parameters will be populated from either the InState
  ///   deserialized form blob storage, or from command line parameters, or from ILifetimeScope</summary>
  [AttributeUsage(AttributeTargets.Method)]
  public class PipeAttribute : Attribute { }

  /// <summary>Decorate a parameter that will come from environent variables in for format PipeName:ArgName</summary>
  [AttributeUsage(AttributeTargets.Parameter)]
  public class PipeArgAttribute : Attribute { }
}