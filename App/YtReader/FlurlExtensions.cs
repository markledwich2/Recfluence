using System;
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
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Text;
using SysExtensions.Threading;
using static SysExtensions.Net.HttpExtensions;
using static SysExtensions.Threading.Def;
using static YtReader.FlurlExtensions;

namespace YtReader {
  public record FlurlProxyFallbackClient(FlurlClient Direct, FlurlClient Proxy, ProxyCfg Cfg) {
    /// <summary>Executes getResponse and retries with proxy fallback. Throws if unsuccessful</summary>
    public async Task<IFlurlResponse> Send(string desc, IFlurlRequest request, HttpMethod verb = null, HttpContent content = null,
      Func<IFlurlResponse, bool> isTransient = null, ILogger log = null) {
      verb ??= HttpMethod.Get;

      var contentString = content == null ? null : await content.ReadAsStringAsync();

      Task<IFlurlResponse> GetRes() => request.WithClient(UseProxy ? Proxy : Direct).AllowAnyHttpStatus().SendAsync(verb, content);
      void ThrowIfError(IFlurlResponse r, Exception e) => r.EnsureSuccess(log, desc, request, e, verb, contentString);

      var retry = Policy.HandleResult<IFlurlResponse>(d => isTransient?.Invoke(d) ?? IsTransientError(d.StatusCode))
        .RetryWithBackoff("BcWeb flurl transient error", Cfg.Retry,
          (r, i) => log?.Debug("retryable error with {Desc}: '{Error}'. Attempt {Attempt}/{Total}\n{Curl}",
            desc, r.Result?.StatusCode.ToString() ?? r.Exception?.Message ?? "Unknown error", i, Cfg.Retry, request.FormatCurl())
          , log);

      var (res, ex) = await Fun(() => retry.ExecuteAsync(GetRes)).Try();
      if (res != null && IsSuccess(res.StatusCode)) return res;
      ThrowIfError(res, ex);

      if (res?.StatusCode != null && !IsTransientError(res.StatusCode))
        ThrowIfError(res, ex); // throw for non-transient errors
      if (UseProxy)
        ThrowIfError(res, ex); // throw if there is an error and we are already using proxy
      UseProxy = true;
      var res2 = await retry.ExecuteAsync(GetRes);
      ThrowIfError(res2, ex);
      return res2;
    }

    public bool UseProxy { get; set; }

    public void UseProxyOrThrow(ILogger log, string desc, Url url, Exception exception = null, int? statusCode = null) {
      if (statusCode != null && !IsTransientError(statusCode.Value))
        EnsureSuccess(log, desc, url, statusCode, exception);
      if (UseProxy)
        EnsureSuccess(log, desc, url, statusCode, exception); // throw if there is an error and we are already using proxy
      UseProxy = true;
    }
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
      foreach (var (key, value) in values.ToKeyValuePairs())
        if (value is string s)
          url.SetQueryParam(key, s, isEncoded);
        else
          url.SetQueryParam(key, value);
      return url;
    }

    public static void EnsureSuccess(ILogger log, string desc, Url url, int? statusCode, Exception ex = null) {
      if (statusCode != null && IsSuccess(statusCode.Value)) return;
      var error = statusCode?.ToString() ?? ex?.Message ?? "unknown";
      log?.Warning(ex, "Flurl {Desc} - failed {Status}: {Url}", desc, error, url);
      throw new($"Flurl '{desc}' failed ({error})", ex);
    }

    public static  void EnsureSuccess(this IFlurlResponse res, ILogger log, string desc, IFlurlRequest request, Exception ex = null, HttpMethod verb = null,
      string content = null) {
      if (res != null && IsSuccess(res.StatusCode)) return;
      var error = res?.StatusCode.ToString() ?? ex?.Message ?? "unknown";
      var curl = request.FormatCurl(verb, content);
      log?.Warning(ex, "Flurl {Desc} - failed {Status}: {Curl}", desc, error, curl);
      throw new($"Flurl '{desc}' failed ({error})", ex);
    }

    public static CapturedUrlEncodedContent FormUrlContent(this IFlurlRequest req, object data) =>
      new(req.Settings.UrlEncodedSerializer.Serialize(data));

    public static string FormatCurl(this IFlurlRequest req, HttpMethod verb = null, string content = null) {
      verb ??= HttpMethod.Get;
      var args = Array.Empty<string>()
        .Concat(req.Url.ToString(), "-X", verb.Method.ToUpper())
        .Concat(req.Headers.SelectMany(h => new[] {"-H", $"'{h.Name}:{h.Value}'"}));
      if (content != null) {
        args = args.Concat("-d", $"'{content}'");
      }
      var curl = $"curl {args.NotNull().Join(" ")}";
      return curl;
    }
  }
}