using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using Humanizer;
using SysExtensions.Text;

namespace YtReader {
  public static class WebEx {
    public static string LastInPath(this string path) => path?.Split('/').LastOrDefault(t => !t.NullOrEmpty());

    /// <summary>Removes leading and trailing slashes from the path</summary>
    public static string TrimPath(this string path) => path?.Split('/').Where(t => !t.Trim().NullOrEmpty()).Join("/");

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