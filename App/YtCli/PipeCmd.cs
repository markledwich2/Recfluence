using System.Threading.Tasks;
using Autofac;
using Mutuo.Etl.Pipe;
using SysExtensions.Text;

namespace YtCli {
  public class PipeCmd : PipeArgs {
    public static async Task<ExitCode> RunPipe(ICmdCtx<PipeCmd> ctx) {
      var option = ctx.Option;
      var runId = option.RunId.HasValue() ? PipeRunId.FromString(option.RunId) : PipeRunId.FromName(option.Pipe);
      ctx.Log.Information("Pipe Run {RunId} - launched", runId);
      var pipeCtx = ctx.Scope.Resolve<IPipeCtx>();
      return await pipeCtx.DoPipeWork(runId);
    }
  }
}