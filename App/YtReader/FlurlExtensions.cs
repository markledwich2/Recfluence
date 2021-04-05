using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using Flurl.Http.Content;
using Flurl.Util;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly;
using Serilog;
using SysExtensions;
using SysExtensions.Net;
using SysExtensions.Text;
using SysExtensions.Threading;
using static SysExtensions.Net.HttpExtensions;
using static SysExtensions.Threading.Def;
using SysExtensions.Collections;

namespace YtReader {
  
  public record FlurlProxyFallbackClient(FlurlClient Direct, FlurlClient Proxy) {
    /// <summary>Executes getResponse and retries with proxy fallback. Throws if unsuccessful</summary>
    public async Task<IFlurlResponse> Send(IFlurlRequest request, HttpMethod verb = null, HttpContent content = null, ILogger log = null) {
      verb ??= HttpMethod.Get;
      Task<IFlurlResponse> GetRes() => request.WithClient(UseProxy ? Proxy : Direct).AllowAnyHttpStatus().SendAsync(verb, content);

      log?.Debug(await request.FormatCurl(verb, content));
      
      var retry = Policy.HandleResult<IFlurlResponse>(d => IsTransient(d.StatusCode))
        .RetryWithBackoff("BcWeb flurl transient error", retryCount: 5, d => d.StatusCode.ToString(), log);
      
      var (res, ex) = await Fun(() => retry.ExecuteAsync(GetRes)).Try();
      if (res != null && IsSuccess(res.StatusCode)) return res;
      UseProxyOrThrow(ex, request.Url, res?.StatusCode);
      res = await retry.ExecuteAsync(GetRes);
      EnsureSuccess(res.StatusCode, request.Url.ToString());
      return res;
    }

    public void UseProxyOrThrow(Exception ex, string url, int? statusCode) {
      if (statusCode != null && !IsTransient(statusCode.Value))
        EnsureSuccess(statusCode.Value, url); // throw for non-transient errors

      if (UseProxy) { // throw if there is an error and we are allready using proxy
        if (ex != null) throw ex;
        if (statusCode != null) EnsureSuccess(statusCode.Value, url);
      }

      UseProxy = true;
    }

    public bool UseProxy { get; set; }
  }
  
  public static class FlurlExtensions {
    public static Url AsUrl(this string url) => new(url);
    public static IFlurlRequest AsRequest(this Url url) => new FlurlRequest(url);

    /// <summary>Reads the content as Json (and unzip if required)</summary>
    public static Task<JObject> JsonObject(this IFlurlResponse response) =>
      response.GetStreamAsync().Then(s => JObject.LoadAsync(new JsonTextReader(new StreamReader(s)) {CloseInput = true}));
    
    /// <summary>Reads the content as Json (and unzip if required)</summary>
    public static Task<JArray> JsonArray(this IFlurlResponse response) =>
      response.GetStreamAsync().Then(s => JArray.LoadAsync(new JsonTextReader(new StreamReader(s)) {CloseInput = true}));
    
    public static Url SetParams(this Url url, object values, bool isEncoded = false) {
      if (values == null)
        return url;
      foreach (var (key, value) in values.ToKeyValuePairs()) {
        if(value is string s)
          url.SetQueryParam(key, s, isEncoded);
        else
          url.SetQueryParam(key, value);
      }
      return url;
    }
    
    public static CapturedUrlEncodedContent FormUrlContent(this IFlurlRequest req, object data) =>
      new (req.Settings.UrlEncodedSerializer.Serialize(data));

    public static async Task<string> FormatCurl(this IFlurlRequest req, HttpMethod verb = null, HttpContent content = null) {
      verb ??= HttpMethod.Get;
      var args = Array.Empty<string>()
        .Concat(req.Url.ToString(), "-X", verb.Method.ToUpper())
        .Concat(req.Headers.SelectMany(h => new []{"-H", $"'{h.Name}:{h.Value}'"}));
      if (content != null) {
        var s = await content.ReadAsStringAsync();
        args = args.Concat("-d", $"'{s}'");
      }
      var curl = $"curl {args.NotNull().Join(" ")}";
      return curl;
    }
  }
}