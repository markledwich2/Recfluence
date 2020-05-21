using System;
using System.Linq;
using System.Reflection;
using Autofac;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Mutuo.Etl.Blob;
using Serilog;
using Serilog.Core;
using SysExtensions;
using SysExtensions.Text;

namespace Mutuo.Etl.Pipe {
  /// <summary>Context & Cfg for running a pipe commands</summary>
  public interface IPipeCtx {
    ILogger          Log     { get; }
    ISimpleFileStore Store   { get; }
    PipeAppCtx       AppCtx  { get; }
    ILifetimeScope   Scope   { get; }
    PipeAppCfg       PipeCfg { get; }
  }

  public class PipeAppCtx {
    public PipeAppCtx(ILifetimeScope scope, params Type[] assemblyTypes) {
      Scope = scope;
      Assemblies = assemblyTypes.Select(t => t.Assembly).Distinct().ToArray();
    }

    public PipeAppCtx(PipeAppCtx appCtx) {
      Scope = appCtx.Scope;
      Assemblies = appCtx.Assemblies;
      EnvironmentVariables = appCtx.EnvironmentVariables;
      CustomRegion = appCtx.CustomRegion;
    }

    /// <summary>Context to resolve pipe instances</summary>
    public ILifetimeScope Scope { get;                               set; }
    public Assembly[]                    Assemblies           { get; set; }
    public (string name, string value)[] EnvironmentVariables { get; set; } = { };
    public Func<Region>                  CustomRegion         { get; set; }
  }

  public class PipeCtx : IPipeCtx, IDisposable {
    public PipeCtx() { }

    public PipeCtx(PipeAppCfg cfg, PipeAppCtx appCtx, ISimpleFileStore store, ILogger log = null) {
      AppCtx = appCtx;
      PipeCfg = cfg;
      Store = store;
      Log = log ?? Logger.None;
      Scope = appCtx.Scope.BeginLifetimeScope(b => b.Register(_ => this).As<IPipeCtx>());
    }

    public PipeAppCfg PipeCfg { get; }

    /*public PipeCtx(IPipeCtx ctx) {
      Log = ctx.Log;
      Store = ctx.Store;
      BasePipeCfg = ctx.Cfg;
    }*/

    public void Dispose() => Scope.Dispose();

    public ILogger          Log    { get; }
    public ISimpleFileStore Store  { get; }
    public PipeAppCtx       AppCtx { get; }
    public ILifetimeScope   Scope  { get; }
  }

  /// <summary>A unique string for a pipe run. Human readable and easily passable though commands.</summary>
  public class PipeRunId {
    public PipeRunId(string name, string groupId, int num = default) {
      Name = name;
      GroupId = groupId;
      Num = num;
    }

    public PipeRunId() { }

    public string Name { get; set; }

    /// <summary>A unique string for a batch of pipe run's that are part of the same operation</summary>
    public string GroupId { get; set; }
    public int Num { get;        set; }

    public bool HasGroup => GroupId.HasValue();

    public static PipeRunId FromName(string name) => new PipeRunId(name, NewGroupId());

    public static PipeRunId FromString(string path) {
      var split = path.Split("|");
      if (split.Length == 1) return FromName(path);
      if (split.Length < 3) throw new InvalidOperationException($"{path} doesn't have 3 components");
      return new PipeRunId {
        Name = split[0],
        GroupId = split[1],
        Num = split[2].ParseInt()
      };
    }

    public static string NewGroupId() => $"{DateTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-ss")}-{Guid.NewGuid().ToShortString(4)}";

    public override string ToString() => $"{Name}|{GroupId}|{Num}";
  }
}