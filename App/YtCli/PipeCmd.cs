using System.Threading.Tasks;
using CliFx;
using CliFx.Attributes;
using CliFx.Exceptions;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions.Text;

namespace YtCli {
  [Command("pipe")]
  public class PipeCmd : PipeCmdArgs {
    readonly IPipeCtx PipeCtx;
    readonly ILogger  Log;

    public PipeCmd(IPipeCtx pipeCtx, ILogger log) {
      PipeCtx = pipeCtx;
      Log = log;
    }

    public override async ValueTask ExecuteAsync(IConsole console) {
      var pipeMethods = PipeCtx.PipeMethods();
      var runId = RunId.HasValue() ? PipeRunId.FromString(RunId) : new PipeRunId();
      if (RunId.NullOrEmpty()) throw new CommandException($"Provide one of the following pipes to run: {pipeMethods.Join(", ", m => m.Method.Name)}");
      if (!pipeMethods.ContainsKey(runId.Name))
        throw new CommandException($"Pipe {runId.Name} not found. Available: {pipeMethods.Join(", ", m => m.Method.Name)}");

      Log.Information("Pipe Run Command Started {RunId}", RunId);
      if (runId.HasGroup)
        await PipeCtx.DoPipeWork(runId, console.GetCancellationToken());

      var res = await PipeCtx.Run(runId.Name, log: Log, location: Location ?? PipeRunLocation.Local);
      if (res.Error)
        throw new CommandException(res.ErrorMessage);
    }
  }
}