using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Functions.Worker.Http;
using Newtonsoft.Json;
using Serilog;
using SysExtensions;
using SysExtensions.Serialization;
using static System.Net.HttpStatusCode;

namespace YtFunctions; 

public static class HttpResponseEx {
  public static HttpResponseData JsonResponse(this HttpRequestData req, object data, HttpStatusCode status = OK,
    JsonSerializerSettings settings = null) =>
    req.JsonResponse(data.ToJson(settings), status);

  public static HttpResponseData JsonResponse(this HttpRequestData req, string json, HttpStatusCode status = OK) {
    var res = req.CreateResponse(status).WithJsonContentHeaders();
    res.WriteString(json);
    return res;
  }

  public static HttpResponseData TextResponse(this HttpRequestData req, string data, HttpStatusCode status = OK) {
    var res = req.CreateResponse(status);
    res.Headers.Add("Content-Type", "text/plain; charset=utf-8");
    res.WriteString(data);
    return res;
  }

  public static HttpResponseData WithJsonContentHeaders(this HttpResponseData res) {
    res.Headers.Add("Content-Type", "application/json;charset=utf8");
    return res;
  }

  public static async Task F(Func<Task> run) =>
    await run().WithOnError(ex => Log.Error(ex, "Func failed: {Message}", ex.Message));

  public static async Task<HttpResponseData> R(Func<Task<HttpResponseData>> run) =>
    await run().WithOnError(ex => Log.Error(ex, "Func failed: {Message}", ex.Message));
}