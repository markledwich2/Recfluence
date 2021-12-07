using Flurl;
using Flurl.Http;
using Polly;

namespace YtReader.Web;

public record FlurlProxyClient {
  public readonly ProxyCfg    Cfg;
  readonly        FlurlClient Direct;
  readonly        FlurlClient Proxy;

  public FlurlProxyClient(ProxyCfg cfg) {
    Cfg = cfg;
    Direct = new();
    Proxy = new(cfg.Proxy(ProxyType.Datacenter)?.CreateHttpClient());
  }

  public bool UseProxy { get; set; }

  /// <summary>Executes getResponse and retries with proxy fallback. Throws if unsuccessful.
  ///   <param name="content">This is a deleate because when a request fails the content is disposed and we need ot be able to
  ///     regenerate for retry</param>
  /// </summary>
  public async Task<IFlurlResponse> Send(string desc, IFlurlRequest request, HttpMethod verb = null, Func<HttpContent> content = null,
    Func<IFlurlResponse, bool> isTransient = null, ILogger log = null, bool logRequests = false) {
    verb ??= HttpMethod.Get;
    isTransient ??= DefaultIsTransient;

    Task<string> Curl() => request.FormatCurl(verb, content);

    if (logRequests) log?.Debug("Flurl {Desc}: {Curl}", desc, await Curl());
    Task<IFlurlResponse> GetRes() => request.WithClient(UseProxy ? Proxy : Direct).AllowAnyHttpStatus().SendAsync(verb, content?.Invoke());
    async Task ThrowIfError(IFlurlResponse r, Exception e) => await r.EnsureSuccess(log, desc, request, e, verb, content);
    var retry = Policy
      .Handle<FlurlHttpTimeoutException>()
      .OrResult(isTransient)
      .RetryWithBackoff("Flurl transient error", Cfg.Retry,
        async (r, i, _) => log?.Debug("retryable error with {Desc}: '{Error}'. Attempt {Attempt}/{Total}\n{Curl}",
          desc, r.Result?.StatusCode.ToString() ?? r.Exception?.Message ?? "Unknown error", i, Cfg.Retry, await Curl())
        , log);

    var (res, ex) = await Def.Fun(() => retry.ExecuteAsync(GetRes)).Try();
    if (res != null && HttpExtensions.IsSuccess(res.StatusCode)) return res; // return on success

    if (UseProxy) await ThrowIfError(res, ex); // throw if there is an error and we are already using proxy

    if (res?.StatusCode == 429 && !UseProxy) {
      UseProxy = true;
      log?.Debug("Flurl - switch to proxy service");
      var res2 = await retry.ExecuteAsync(GetRes);
      await ThrowIfError(res2, ex);
      return res2;
    }
    await ThrowIfError(res, ex);
    return res;
  }

  public bool DefaultIsTransient(IFlurlResponse res) {
    if (!UseProxy && res.StatusCode == 429) return false; // return false to immediately fall back to proxy
    return HttpExtensions.IsTransientError(res.StatusCode);
  }

  public void UseProxyOrThrow(ILogger log, string desc, Url url, Exception exception = null, int? statusCode = null) {
    if (statusCode != null && !HttpExtensions.IsTransientError(statusCode.Value))
      FlurlExtensions.EnsureSuccess(log, desc, url, statusCode, exception);
    if (UseProxy)
      FlurlExtensions.EnsureSuccess(log, desc, url, statusCode, exception); // throw if there is an error and we are already using proxy
    UseProxy = true;
  }
}