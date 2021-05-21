using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using AngleSharp;
using AngleSharp.Dom;
using SysExtensions.Net;

namespace YtReader.Web {
  public static class AngleExtensions {
    public static T El<T>(this IParentNode b, string selector) where T : class, IElement => b.QuerySelector(selector) as T;
    public static IElement El(this IParentNode b, string selector) => b.QuerySelector(selector);

    public static IEnumerable<T> Els<T>(this IParentNode b, string selector) where T : class, IElement => b.QuerySelectorAll(selector).Cast<T>();
    public static IEnumerable<IElement> Els(this IParentNode b, string selector) => b.QuerySelectorAll(selector);

    public static string QsAttr(this IParentNode b, string selector, string attribute) => b.QuerySelector(selector)?.GetAttribute(attribute);
    public static void EnsureSuccess(this IDocument doc) => doc.StatusCode.EnsureSuccess(doc.Url);

    public static IConfiguration WithProxyRequester(this IConfiguration angleCfg, FlurlProxyClient proxyClient
      , ProxyType proxyType = default) {
      var proxy = proxyClient.UseProxy ? proxyClient.Cfg.Proxy(proxyType)?.CreateWebProxy() : null;
      var handler = new HttpClientHandler {
        Proxy = proxy,
        PreAuthenticate = true,
        UseDefaultCredentials = false,
        UseCookies = true,
        UseProxy = true,
        AutomaticDecompression = DecompressionMethods.All
      };

      /*var requester = new DefaultHttpRequester("PostmanRuntime/7.26.10", request => {
        var proxy = proxyClient.UseProxy ? proxyClient.Cfg.Proxy(proxyType)?.CreateWebProxy() : null;
        if (proxy != null) request.Proxy = proxy;
        log?.Debug("Angle Request {Proxy}: {Curl}", proxy?.Address?.ToString() ?? "(direct)", request.FormatCurl());
      });*/
      /*if (headers != null)
        requester.Headers.AddRange(headers);
      if (timeout != default)
        requester.Timeout = timeout;*/
      return angleCfg.WithRequesters(handler);
    }

    /// <summary>Configures the angle requester from the given flurl proxy client configuration (doesn't actually use flurl
    ///   client at run time)</summary>
    public static IBrowsingContext Browser(this IConfiguration angleCfg) => BrowsingContext.New(angleCfg);
  }
}