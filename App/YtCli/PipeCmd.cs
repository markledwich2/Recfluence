using System.Threading.Tasks;
using CliFx;
using CliFx.Attributes;
using CliFx.Exceptions;
using CliFx.Infrastructure;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions.Text;

namespace YtCli {
  /// <summary>Generic command for pipe ETL to launch instances to perform any pipe operations</summary>
  [Command("pipe")]
  public class PipeCmd : PipeCmdArgs, ICommand {
    readonly IPipeCtx PipeCtx;
    readonly ILogger  Log;

    public PipeCmd(IPipeCtx pipeCtx, ILogger log) {
      PipeCtx = pipeCtx;
      Log = log;
    }
    
    public override async ValueTask ExecuteAsync(IConsole console) {
      var pipeMethods = PipeCtx.PipeMethods();
      var runId = RunId.HasValue() ? PipeRunId.FromString(RunId) : new();
      if (RunId.NullOrEmpty()) throw new CommandException($"Provide one of the following pipes to run: {pipeMethods.Join(", ", m => m.Method.Name)}");
      if (!pipeMethods.ContainsKey(runId.Name))
        throw new CommandException($"Pipe {runId.Name} not found. Available: {pipeMethods.Join(", ", m => m.Method.Name)}");

      var cancel = console.RegisterCancellationHandler();
      var log = Log.ForContext("RunId", runId);
      log.Information("Pipe Run Command Started {RunId}", RunId);
      if (runId.HasGroup) {
        await PipeCtx.DoPipeWork(runId, cancel);
      }
      else {
        var res = await PipeCtx.Run(runId.Name, new() {Location = Location ?? PipeRunLocation.Local}, log: log, cancel: cancel);
        if (res.Error)
          throw new CommandException(res.ErrorMessage);
      }
    }
  }
}