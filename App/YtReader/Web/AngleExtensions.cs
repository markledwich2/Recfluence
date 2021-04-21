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

    /// <summary>Configures the angle requester from the given flurl proxy client configuration (doesn't actually use flurl
    ///   client at run time)</summary>
    public static IBrowsingContext WithProxyRequester(this IConfiguration angleCfg, FlurlProxyClient client) {
      return BrowsingContext.New(
        client.UseProxy
          ? angleCfg.WithRequesters(new () {
            Proxy = client.Cfg.Proxies.First().CreateWebProxy(),
            PreAuthenticate = true,
            UseDefaultCredentials = false,
            AutomaticDecompression = DecompressionMethods.All,
          })
          : angleCfg);
    }
  }
}