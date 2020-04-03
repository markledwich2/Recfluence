using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Autofac;
using Autofac.Util;
using CommandLine;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Reflection;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Pipe {
  public static class Pipes {
    public static IPipeCtx CreatePipeCtx(PipeAppCfg cfg, PipeRunId runId, ILogger log, IComponentContext getScope,
      IEnumerable<Assembly> pipeAssemblies = null, IEnumerable<KeyValuePair<string, string>> environmentVars = null) =>
      new PipeCtx {
        Cfg = cfg,
        Store = new AzureBlobFileStore(cfg.Store.Cs, cfg.Store.Path, log),
        Id = runId,
        PipeAssemblies = pipeAssemblies?.ToArray() ?? new Assembly[] { },
        Log = log,
        Scope = getScope,
        EnvVars = new Dictionary<string, string>(environmentVars ?? new List<KeyValuePair<string, string>>())
      };

    /// <summary>Runs a pipeline to process a list of work in batches on multiple containers. The transform is used to provide
    ///   strong typing, but may not actually be run locally.</summary>
    public static async Task<IReadOnlyCollection<(PipeRunMetadata Metadata, TOut OutState)>> RunPipe<TIn, TOut>(
      this IEnumerable<TIn> items, Func<IEnumerable<TIn>, Task<TOut>> transform, IPipeCtx ctx, int minBatch, int parallel, ILogger log) where TOut : class {
      var isPipe = transform.Method.GetCustomAttribute<PipeAttribute>() != null;
      if (!isPipe) throw new InvalidOperationException($"given transform '{transform.Method.Name}' must be a pipe");
      var pipeNme = transform.Method.Name;

      // batch and create state for containers to read
      var group = PipeRunId.NewGroupId();

      var batches = await items.Batch(minBatch, parallel)
        .Select((g, i) => (Id: new PipeRunId(pipeNme, group, i), In: g.ToArray()))
        .BlockTransform(async b => {
          await ctx.SaveInState(b.In, b.Id);
          return b.Id;
        });

      IContainerRunner containerRunner = ctx.Cfg.Location switch {
        PipeRunLocation.Container => new AzureContainerRunner(),
        PipeRunLocation.LocalContainer => new LocalContainerRunner(),
        PipeRunLocation.LocalThread => new ThreadContainerRunner(),
        _ => throw new NotImplementedException($"{ctx.Cfg.Location}")
      };

      var res = await containerRunner.RunBatch(ctx, batches.ToArray(), log);
      var outState = await res
        .BlockTransform(async b => (Metadata: b, OutState: b.Success ? await GetOutState<TOut>(ctx, b.Id) : null));

      var batchId = $"{pipeNme}|{group}";
      log.Information("{BatchId} - Batches {Succeded}/{Total} succeeded/total",
        batchId, res.Count(r => r.Success), res.Count);

      var failed = outState.Where(o => !o.Metadata.Success).ToArray();
      if (failed.Any())
        log.Error("{BatchId} - {Batches} batches failed. Ids:{Ids}",
          batchId, failed.Length, failed.Select(f => f.Metadata.Id));

      return outState;
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
          var genericType = p.ParameterType.GenericTypeArguments.FirstOrDefault() ??
                            throw new InvalidOperationException(
                              $"Expecting arg method {pipeType.Type}.{method.Name} parameter {p.Name} to be IEnumerable<Type>");
          var loadInStateMethod = typeof(Pipes).GetMethod("LoadInState", new[] {typeof(IPipeCtx)})?.MakeGenericMethod(genericType)
                                  ?? throw new InvalidOperationException("LoadJsonlGz method not found ");
          try {
            dynamic task = loadInStateMethod.Invoke(null, new object[] {ctx});
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
          await ctx.SetOutState(pipeResult);
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

    public static async Task<T> GetOutState<T>(this IPipeCtx ctx, PipeRunId id = null) where T : class =>
      await ctx.Store.Get<T>((id ?? ctx.Id).OutStatePath());

    public static async Task SetOutState<T>(this IPipeCtx ctx, T state, PipeRunId id = null) where T : class =>
      await ctx.Store.Set((id ?? ctx.Id).OutStatePath(), state);

    public static async Task SaveInState<T>(this IPipeCtx ctx, IEnumerable<T> state, PipeRunId id = null) {
      using var s = state.ToJsonlGzStream();
      var path = $"{(id ?? ctx.Id).InStatePath()}.jsonl.gz";
      await ctx.Store.Save(path, s);
    }

    public static async Task<IReadOnlyCollection<T>> LoadInState<T>(this IPipeCtx ctx) {
      using var s = await ctx.Store.Load($"{ctx.Id.InStatePath()}.jsonl.gz");
      return s.LoadJsonlGz<T>();
    }
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