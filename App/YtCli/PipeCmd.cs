using System.Threading.Tasks;
using Autofac;
using Mutuo.Etl.Pipe;
using SysExtensions.Text;

namespace YtCli {
  public class PipeCmd : PipeCmdArgs {
    public static async Task<ExitCode> RunPipe(ICmdCtx<PipeCmd> ctx) {
      var option = ctx.Option;

      var pipeCtx = ctx.Scope.Resolve<IPipeCtx>();
      var pipeMethods = pipeCtx.PipeMethods();
      var runId = option.RunId.HasValue() ? PipeRunId.FromString(option.RunId) : new PipeRunId();
      if (option.RunId.NullOrEmpty()) {
        ctx.Log.Error($"Provide one of the following pipes to run: {pipeMethods.Join(", ", m => m.Method.Name)}");
        return ExitCode.Error;
      }
      if (!pipeMethods.ContainsKey(runId.Name)) {
        ctx.Log.Error($"Pipe {runId.Name} not found. Available: {pipeMethods.Join(", ", m => m.Method.Name)}");
        return ExitCode.Error;
      }

      ctx.Log.Information("Pipe Run Command Started {RunId}", option.RunId);
      if (runId.HasGroup)
        return await pipeCtx.DoPipeWork(runId);

      var res = await pipeCtx.Run(runId.Name, log: ctx.Log, location: ctx.Option.Location ?? PipeRunLocation.Local);
      return res.Error ? ExitCode.Error : ExitCode.Success;
    }
  }
}