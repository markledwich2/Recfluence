using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Medallion.Shell;
using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Management.ContainerInstance.Fluent.Models;
using Mutuo.Etl.Blob;
using Semver;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace Mutuo.Etl.Pipe {
  public interface IPipeWorker {
    /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
    ///   returns the status.</summary>
    Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log);
  }

  public interface IPipeWorkerStartable : IPipeWorker {
    /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
    ///   returns the status.</summary>
    Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, bool returnOnRunning, ILogger log);
  }

  public enum ContainerState {
    Unknown,
    Pending,
    Running,
    Succeeded,
    Failed,
    Stopped,
    Updating,
    Terminated
  }

  public static class PipeWorkerEx {
    public static string ContainerGroupName(this PipeRunId runid) => $"{runid.Name}-{runid.GroupId}-{runid.Num}".ToLowerInvariant();

    /// <summary>the container image name, with its registry and tag</summary>
    public static string FullContainerImageName(this ContainerCfg cfg, string tag) => $"{cfg.Registry}/{cfg.ImageName}:{tag}";

    public static string[] PipeArgs(this PipeRunId runId) => new[] {"pipe", "-r", runId.ToString()};

    public static async Task Save(this PipeRunMetadata md, ISimpleFileStore store, ILogger log) =>
      await store.Set($"{md.Id.StatePath()}.RunMetadata", md, false, log);

    public static ContainerState State(this IContainerGroup group) => group.State.ToEnum<ContainerState>(false);
    public static bool IsCompletedState(this ContainerState state) => state.In(ContainerState.Succeeded, ContainerState.Failed);

    public static async Task<IContainerGroup> WaitForState(this IContainerGroup group, params ContainerState[] states) {
      while (true) {
        if (group.State().In(states))
          return group;
        await Task.Delay(1.Seconds());
        group = await group.RefreshAsync();
      }
    }

    /// <summary>Starts some pipe work. Assumes required state has been created</summary>
    public static async Task<PipeRunMetadata> Launch(this IPipeWorker worker, IPipeCtx ctx, PipeRunId runId, ILogger log) =>
      (await worker.Launch(ctx, new[] {runId}, log)).First();

    /// <summary>Starts some pipe work. Assumes required state has been created</summary>
    public static async Task<PipeRunMetadata> Launch(this IPipeWorkerStartable worker, IPipeCtx ctx, PipeRunId runId, bool returnOnStarting, ILogger log) =>
      (await worker.Launch(ctx, new[] {runId}, returnOnStarting, log)).First();
  }

  public class LocalPipeWorker : IPipeWorker {
    readonly SemVersion Version;

    public LocalPipeWorker(SemVersion version) => Version = version;

    public async Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) =>
      await ids.BlockFunc(async id => {
        var runCfg = id.PipeCfg(ctx.PipeCfg);
        var image = runCfg.Container.FullContainerImageName(Version.PipeTag());
        var args = new[] {"run"}
          .Concat(ctx.AppCtx.EnvironmentVariables.SelectMany(e => new[] {"--env", $"{e.name}={e.value}"}))
          .Concat("--rm", "-i", image)
          .Concat(runCfg.Container.Exe)
          .Concat(id.PipeArgs())
          .ToArray<object>();
        var cmd = Command.Run("docker", args).RedirectTo(Console.Out);
        var res = await cmd.Task;
        var md = res.Success
          ? new PipeRunMetadata {
            Id = id
          }
          : new PipeRunMetadata {
            Id = id,
            ErrorMessage = await cmd.StandardError.ReadToEndAsync()
          };
        await md.Save(ctx.Store, log);
        return md;
      });
  }

  public class ThreadPipeWorker : IPipeWorker {
    public async Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log) {
      var res = await ids.BlockFunc(async id => {
        await ctx.DoPipeWork(id);
        var md = new PipeRunMetadata {
          Id = id
        };
        await md.Save(ctx.Store, log);
        return md;
      });
      return res;
    }
  }

  public class PipeRunMetadata {
    public PipeRunId      Id           { get; set; }
    public string         RawState     { get; set; }
    public ContainerState State        { get; set; }
    public TimeSpan       Duration     { get; set; }
    public Container[]    Containers   { get; set; }
    public string         ErrorMessage { get; set; }
    public PipeRunCfg     RunCfg       { get; set; }

    public bool Error => ErrorMessage.HasValue();
  }
}