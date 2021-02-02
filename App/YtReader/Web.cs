using System;
using System.Net;
using System.Net.Http;
using Humanizer;

namespace YtReader {
  public static class WebEx {
    public static WebProxy CreateWebProxy(this ProxyConnectionCfg proxy) =>
      new(proxy.Url, BypassOnLocal: true, new string[] { },
        proxy.Creds != null ? new NetworkCredential(proxy.Creds.Name, proxy.Creds.Secret) : null);

    public static HttpClient CreateHttpClient(this ProxyConnectionCfg proxy, TimeSpan? timeout = null) =>
      new(new HttpClientHandler {
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        UseCookies = false,
        Proxy = proxy.Url == null ? null : proxy.CreateWebProxy(),
        UseProxy = proxy.Url != null,
      }) {
        Timeout = timeout ?? 30.Seconds()
      };
  }
}