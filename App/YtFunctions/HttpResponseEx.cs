using System;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker;
using Newtonsoft.Json;
using Serilog;
using SysExtensions;
using SysExtensions.Serialization;
using static System.Net.HttpStatusCode;

namespace YtFunctions {
  public static class HttpResponseEx {
    public static HttpResponseData JsonResponse(this object o, JsonSerializerSettings settings) => new(OK, o.ToJson(settings));

    public static HttpResponseData WithJsonContentHeaders(this HttpResponseData res) {
      res.Headers.Add("Content", "Content-Type:application/json;charset=utf8");
      return res;
    }

    public static async Task F(Func<Task> run) =>
      await run().WithOnError(ex => Log.Error(ex, "Func failed: {Message}", ex.Message));

    public static async Task<HttpResponseData> R(Func<Task<HttpResponseData>> run) =>
      await run().WithOnError(ex => Log.Error(ex, "Func failed: {Message}", ex.Message));
  }
}