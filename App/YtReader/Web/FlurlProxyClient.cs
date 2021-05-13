using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using Polly;
using Serilog;
using SysExtensions;
using SysExtensions.Net;
using SysExtensions.Threading;

namespace YtReader.Web {
  
  public record FlurlProxyClient {
    readonly        FlurlClient Direct;
    readonly        FlurlClient Proxy;
    public readonly ProxyCfg    Cfg;

    public FlurlProxyClient(ProxyCfg cfg) {
      Cfg = cfg;
      Direct = new();
      Proxy = new(cfg.Proxy(ProxyType.Datacenter)?.CreateHttpClient());
    }

    /// <summary>Executes getResponse and retries with proxy fallback. Throws if unsuccessful</summary>
    public async Task<IFlurlResponse> Send(string desc, IFlurlRequest request, HttpMethod verb = null, HttpContent content = null,
      Func<IFlurlResponse, bool> isTransient = null, ILogger log = null) {
      verb ??= HttpMethod.Get;
      isTransient ??= DefaultIsTransient;
      var contentString =
        content == null ? null : await content.ReadAsStringAsync(); // we read the content at the start for logging, to avoid it being disposed on us

      Task<IFlurlResponse> GetRes() => request.WithClient(UseProxy ? Proxy : Direct).AllowAnyHttpStatus().SendAsync(verb, content);
      void ThrowIfError(IFlurlResponse r, Exception e) => r.EnsureSuccess(log, desc, request, e, verb, contentString);
      var retry = Policy.HandleResult(isTransient).RetryWithBackoff("BcWeb flurl transient error", Cfg.Retry,
        (r, i, _) => log?.Debug("retryable error with {Desc}: '{Error}'. Attempt {Attempt}/{Total}\n{Curl}",
          desc, r.Result?.StatusCode.ToString() ?? r.Exception?.Message ?? "Unknown error", i, Cfg.Retry, request.FormatCurl())
        , log);

      var (res, ex) = await Def.Fun(() => retry.ExecuteAsync(GetRes)).Try();
      if (res != null && HttpExtensions.IsSuccess(res.StatusCode)) return res; // return on success

      if (UseProxy) ThrowIfError(res, ex); // throw if there is an error and we are already using proxy
      
      if(res?.StatusCode == 429 && !UseProxy) {
        UseProxy = true;
        log?.Debug("Flurl - switch to proxy service");
        var res2 = await retry.ExecuteAsync(GetRes);
        ThrowIfError(res2, ex);
        return res2;
      }
      ThrowIfError(res, ex);
      return res;
    }

    bool DefaultIsTransient(IFlurlResponse res) {
      if (!UseProxy && res.StatusCode == 429) return false; // return false to immediately fall back to proxy
      return HttpExtensions.IsTransientError(res.StatusCode);
    }

    public bool UseProxy { get; set; }

    public void UseProxyOrThrow(ILogger log, string desc, Url url, Exception exception = null, int? statusCode = null) {
      if (statusCode != null && !HttpExtensions.IsTransientError(statusCode.Value))
        FlurlExtensions.EnsureSuccess(log, desc, url, statusCode, exception);
      if (UseProxy)
        FlurlExtensions.EnsureSuccess(log, desc, url, statusCode, exception); // throw if there is an error and we are already using proxy
      UseProxy = true;
    }
  }
}