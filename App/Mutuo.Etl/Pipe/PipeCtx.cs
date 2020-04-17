using System;
using System.Collections.Generic;
using System.Reflection;
using Autofac;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions;
using SysExtensions.Text;

namespace Mutuo.Etl.Pipe {
  /// <summary>Context & Cfg for running a pipe commands</summary>
  public interface IPipeCtx {
    ILogger           Log    { get; }
    ISimpleFileStore  Store  { get; }
    PipeAppCfg        Cfg    { get; }
    PipeAppCtx        AppCtx { get; }
    IComponentContext Scope  { get; }
  }

  public class PipeAppCtx {
    public PipeAppCtx() { }

    public PipeAppCtx(PipeAppCtx appCtx) {
      Scope = appCtx.Scope;
      Assemblies = appCtx.Assemblies;
      EnvironmentVariables = appCtx.EnvironmentVariables;
      CustomRegion = appCtx.CustomRegion;
    }

    /// <summary>Context to resolve pipe instances</summary>
    public IComponentContext Scope { get;                                               set; }
    public IReadOnlyCollection<Assembly>                    Assemblies           { get; set; } = new List<Assembly>();
    public IReadOnlyCollection<(string name, string value)> EnvironmentVariables { get; set; } = new List<(string, string)>();
    public Func<Region>                                     CustomRegion         { get; set; }
  }

  public class PipeCtx : IPipeCtx {
    public PipeCtx() { }

    public PipeCtx(PipeAppCfg cfg, PipeAppCtx appCtx, ISimpleFileStore store, ILogger log) {
      AppCtx = appCtx;
      Cfg = cfg;
      Store = store;
      Log = log;
    }

    public PipeCtx(IPipeCtx ctx) {
      Log = ctx.Log;
      Store = ctx.Store;
      Cfg = ctx.Cfg;
    }

    public ILogger           Log    { get; }
    public ISimpleFileStore  Store  { get; }
    public PipeAppCtx        AppCtx { get; }
    public PipeAppCfg        Cfg    { get; }
    public IComponentContext Scope  => AppCtx.Scope;
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

    public static PipeRunId FromName(string name) => new PipeRunId(name, NewGroupId(), 0);

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
}