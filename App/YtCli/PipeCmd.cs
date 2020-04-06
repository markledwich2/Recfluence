using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using AngleSharp.Text;
using Autofac;
using CommandLine;
using Microsoft.Azure.Management.ResourceManager.Fluent.Core;
using Mutuo.Etl.Pipe;
using Serilog;
using SysExtensions.Build;
using SysExtensions.Serialization;
using SysExtensions.Text;
using Troschuetz.Random;
using YtReader;

namespace YtCli {
  public class PipeCmd : PipeArgs {
    public static async Task<ExitCode> RunPipe(ICmdCtx<PipeCmd> ctx) {
      var option = ctx.Option;
      var runId = option.RunId.HasValue() ? PipeRunId.FromString(option.RunId) : PipeRunId.FromName(option.Pipe);
      ctx.Log.Information("Pipe Run {RunId} - launched", runId);
      var pipeCtx = ctx.Scope.ResolvePipeCtx();
      return await pipeCtx.DoPipeWork(runId);
    }
  }
}