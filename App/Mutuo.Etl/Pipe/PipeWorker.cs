using Microsoft.Azure.Management.ContainerInstance.Fluent;
using Microsoft.Azure.Management.ContainerInstance.Fluent.Models;
using Mutuo.Etl.Blob;
using Semver;

namespace Mutuo.Etl.Pipe;

public interface IPipeWorker {
  /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
  ///   returns the status.</summary>
  Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log, CancellationToken cancel);
}

public interface IPipeWorkerStartable : IPipeWorker {
  /// <summary>Run a batch of containers. Must have already created state for them. Waits till the batch is complete and
  ///   returns the status.</summary>
  Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, bool returnOnRunning, bool exclusive, ILogger log,
    CancellationToken cancel);
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
  public static string ContainerGroupName(this PipeRunId runId, bool exclusive, SemVersion version) =>
    new[] {
      runId.Name,
      version.Prerelease == "" ? null : version.Prerelease,
      exclusive ? null : runId.GroupId,
      runId.Num > 0 ? runId.Num.ToString() : null
    }.NotNull().Join("-", p => p!.ToLowerInvariant());

  /// <summary>the container image name, with its registry and tag</summary>
  public static string FullContainerImageName(this ContainerCfg cfg, string tag) => $"{cfg.Registry}/{cfg.ImageName}:{tag}";

  public static string[] PipeArgs(this PipeRunId runId) => new[] { "pipe", "-r", runId.ToString() };

  public static async Task Save(this PipeRunMetadata md, ISimpleFileStore store, ILogger log) =>
    await store.SetState($"{md.Id.StatePath()}.RunMetadata", md, zip: false, log);

  public static ContainerState State(this IContainerGroup group) => group.State.ParseEnum<ContainerState>(false);

  public static bool IsCompletedState(this ContainerState state) => state.In(
    ContainerState.Succeeded, ContainerState.Failed, ContainerState.Stopped, ContainerState.Terminated);

  public static async Task<IContainerGroup> WaitForState(this IContainerGroup group, params ContainerState[] states) {
    while (true) {
      if (group.State().In(states))
        return group;
      await Task.Delay(1.Seconds());
      group = await group.RefreshAsync();
    }
  }

  /// <summary>Starts some pipe work. Assumes required state has been created</summary>
  public static async Task<PipeRunMetadata> Launch(this IPipeWorker worker, IPipeCtx ctx, PipeRunId runId, ILogger log, CancellationToken cancel = default) =>
    (await worker.Launch(ctx, new[] { runId }, log, cancel)).First();

  /// <summary>Starts some pipe work. Assumes required state has been created</summary>
  public static async Task<PipeRunMetadata> Launch(this IPipeWorkerStartable worker, IPipeCtx ctx, PipeRunId runId, bool returnOnStarting, bool exclusive,
    ILogger log,
    CancellationToken cancel = default) =>
    (await worker.Launch(ctx, new[] { runId }, returnOnStarting, exclusive, log, cancel)).First();
}

public class ThreadPipeWorker : IPipeWorker {
  public async Task<IReadOnlyCollection<PipeRunMetadata>> Launch(IPipeCtx ctx, IReadOnlyCollection<PipeRunId> ids, ILogger log, CancellationToken cancel) {
    var res = await ids.BlockDo(async id => {
      await ctx.DoPipeWork(id, cancel);
      var md = new PipeRunMetadata {
        Id = id,
        State = ContainerState.Succeeded
      };
      await md.Save(ctx.Store, log);
      return md;
    }).ToArrayAsync();
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