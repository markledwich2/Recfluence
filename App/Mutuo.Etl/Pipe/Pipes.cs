using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Autofac;
using Autofac.Util;
using CommandLine;
using Microsoft.Extensions.Configuration;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Reflection;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Pipe {
  public static class Pipes {
    public static IPipeCtx CreatePipeCtx(PipeAppCfg cfg, ILogger log, IComponentContext getScope,
      IEnumerable<Assembly> pipeAssemblies = null, IEnumerable<KeyValuePair<string, string>> environmentVars = null) =>
      new PipeCtx {
        Cfg = cfg,
        Store = new AzureBlobFileStore(cfg.Store.Cs, cfg.Store.Path, log),
        PipeAssemblies = pipeAssemblies?.ToArray() ?? new Assembly[] { },
        Log = log,
        Scope = getScope,
        EnvVars = new Dictionary<string, string>(environmentVars ?? new List<KeyValuePair<string, string>>())
      };

    /// <summary>Launches a root pipe (i.e. on without work state)</summary>
    /// <returns></returns>
    public static async Task<PipeRunMetadata> RunPipe(this IPipeCtx ctx, string pipeName, bool returnOnStarted, ILogger log) {
      var pipeWorker = PipeWorker(ctx, log);
      return pipeWorker is IPipeWorkerStartable s ? await s.RunWork(ctx, pipeName, returnOnStarted) : await pipeWorker.RunWork(ctx, pipeName);
    }

    /// <summary>Runs a pipeline to process a list of work in batches on multiple containers. The transform is used to provide
    ///   strong typing, but may not actually be run locally.</summary>
    public static async Task<IReadOnlyCollection<(PipeRunMetadata Metadata, TOut OutState)>> RunPipe<TIn, TOut>(
      this IEnumerable<TIn> items, Func<IReadOnlyCollection<TIn>, Task<TOut>> transform, IPipeCtx ctx, PipeRunCfg runCfg, ILogger log) where TOut : class {
      var isPipe = transform.Method.GetCustomAttribute<PipeAttribute>() != null;
      if (!isPipe) throw new InvalidOperationException($"given transform '{transform.Method.Name}' must be a pipe");
      var pipeNme = transform.Method.Name;

      // batch and create state for containers to read
      var group = PipeRunId.NewGroupId();

      var batches = await items.Batch(runCfg.MinWorkItems, runCfg.MaxParallel)
        .Select((g, i) => (Id: new PipeRunId(pipeNme, group, i), In: g.ToArray()))
        .BlockTransform(async b => {
          await ctx.SaveInState(b.In, b.Id);
          return b.Id;
        });

      var pipeWorker = PipeWorker(ctx, log);
      var res = pipeWorker is IPipeWorkerStartable s ? await s.RunWork(ctx, batches, runCfg.ReturnOnStart) : await pipeWorker.RunWork(ctx, batches);

      var outState = await res
        .BlockTransform(async b => (Metadata: b, OutState: b.Success ? await GetOutState<TOut>(ctx, b.Id) : null));

      var batchId = $"{pipeNme}|{group}";
      log.Information("{BatchId} - Batches {Succeded}/{Total} succeeded/total",
        batchId, res.Count(r => r.Success), res.Count);

      var failed = outState.Where(o => o.Metadata.Error).ToArray();
      if (failed.Any())
        log.Error("{BatchId} - {Batches} batches failed. Ids:{Ids}",
          batchId, failed.Length, failed.Select(f => f.Metadata.Id));

      return outState;
    }

    static IPipeWorker PipeWorker(IPipeCtx ctx, ILogger log) {
      IPipeWorker pipeWorker = ctx.Cfg.Location switch {
        PipeRunLocation.Container => new AzurePipeWorker(ctx.Cfg, log),
        PipeRunLocation.LocalContainer => new LocalPipeWorker(),
        _ => new ThreadPipeWorker()
      };
      return pipeWorker;
    }

    static IEnumerable<Assembly> PipeAssemblies(this IPipeCtx ctx) => Assembly.GetExecutingAssembly().AsEnumerable().Concat(ctx.PipeAssemblies);

    /// <summary>Executes a pipe in this process</summary>
    public static async Task<ExitCode> DoPipeWork(this IPipeCtx ctx, PipeRunId id) {
      var pipeMethods = ctx.PipeAssemblies().SelectMany(a => a.GetLoadableTypes())
        .SelectMany(t => t.GetRuntimeMethods().Where(m => m.GetCustomAttribute<PipeAttribute>() != null).Select(m => (Type: t, Method: m)))
        .ToKeyedCollection(m => m.Method.Name);

      var pipeName = id.Name;
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
          var genericType = p.ParameterType.GenericTypeArguments.FirstOrDefault() ??
                            throw new InvalidOperationException(
                              $"Expecting arg method {pipeType.Type}.{method.Name} parameter {p.Name} to be IEnumerable<Type>");
          var loadInStateMethod = typeof(Pipes).GetMethod(nameof(LoadInState), new[] {typeof(IPipeCtx), typeof(PipeRunId)})?.MakeGenericMethod(genericType)
                                  ?? throw new InvalidOperationException($"{nameof(LoadInState)} method not found ");
          try {
            dynamic task = loadInStateMethod.Invoke(null, new object[] {ctx, id});
            return (object) await task;
          }
          catch (Exception ex) {
            throw new InvalidOperationException("Unable to load in state for pipe", ex);
          }
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
          object pipeResult = await task;
          await ctx.SetOutState(pipeResult, id);
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

    static string OutStatePath(this PipeRunId id) => $"{id.StatePath()}.OutState";
    static string InStatePath(this PipeRunId id) => $"{id.StatePath()}.InState";

    static async Task<T> GetOutState<T>(this IPipeCtx ctx, PipeRunId id) where T : class =>
      await ctx.Store.Get<T>(id.OutStatePath());

    static async Task SetOutState<T>(this IPipeCtx ctx, T state, PipeRunId id) where T : class =>
      await ctx.Store.Set(id.OutStatePath(), state);

    static async Task SaveInState<T>(this IPipeCtx ctx, IEnumerable<T> state, PipeRunId id) {
      using var s = state.ToJsonlGzStream();
      var path = $"{id.InStatePath()}.jsonl.gz";
      await ctx.Store.Save(path, s);
    }

    public static async Task<IReadOnlyCollection<T>> LoadInState<T>(this IPipeCtx ctx, PipeRunId id) {
      using var s = await ctx.Store.Load($"{id.InStatePath()}.jsonl.gz");
      return s.LoadJsonlGz<T>();
    }

    public static PipeRunCfg PipeCfg(this PipeRunId id, PipeAppCfg cfg) {
      PipeRunCfg pipeCfg = cfg.Pipes.FirstOrDefault(p => p.PipeName == id.Name);
      if (pipeCfg == null) return cfg.Default;

      using var defaultStream = cfg.Default.ToJsonStream();
      using var pipeStream = cfg.Default.ToJsonStream();

      var builder = new ConfigurationBuilder()
        .AddJsonStream(defaultStream)
        .AddJsonStream(pipeStream)
        .Build();

      pipeCfg = builder.Get<PipeRunCfg>();
      return pipeCfg;
    }

    public static PipeRunCfg PipeCfg(this PipeRunId id, IPipeCtx ctx) => id.PipeCfg(ctx.Cfg);
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