using System;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Flurl;
using Humanizer;
using Mutuo.Etl.Blob;
using Serilog;
using SysExtensions.Text;

namespace YtReader.Web {
  public static class WebEx {
    public static string LastInPath(this string path) => path?.Split('/').LastOrDefault(t => !t.NullOrEmpty());

    /// <summary>Removes leading and trailing slashes from the path</summary>
    public static string TrimPath(this string path) => path?.Split('/').Where(t => !t.Trim().NullOrEmpty()).Join("/");

    public static ProxyConnectionCfg Proxy(this ProxyCfg cfg, ProxyType type) => cfg.Proxies.FirstOrDefault(c => c.Type == type);

    public static WebProxy CreateWebProxy(this ProxyConnectionCfg proxy) =>
      new(proxy.Url, BypassOnLocal: true, new string[] { },
        proxy.Creds != null ? new NetworkCredential(proxy.Creds.Name, proxy.Creds.Secret) : null);

    public static HttpClient CreateHttpClient(this ProxyConnectionCfg proxy, TimeSpan? timeout = null) =>
      new(new HttpClientHandler {
        AutomaticDecompression = DecompressionMethods.GZip | DecompressionMethods.Deflate,
        UseCookies = false,
        Proxy = proxy.Url == null ? null : proxy.CreateWebProxy(),
        UseProxy = proxy.Url != null
      }) {
        Timeout = timeout ?? 30.Seconds()
      };

    public static async Task LogParseError(this ISimpleFileStore logStore, string msg, Exception ex, Url url, string content, ILogger log) {
      var path = SPath.Relative(DateTime.UtcNow.ToString("yyyy-MM-dd"), url.Path);
      var logUrl = logStore.Url(path);
      await logStore.Save(path, content.AsStream(), log);
      log.Warning(ex, "Parsing Diagnostics - Saved content from {Url} to {LogUrl}: {Msg}", url, logUrl, msg);
    }
  }
}