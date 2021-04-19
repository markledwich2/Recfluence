using System.Collections.Generic;
using System.Linq;
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

    public static IBrowsingContext Browser(this IConfiguration angleCfg, FlurlProxyClient client) => BrowsingContext.New(
      client.UseProxy
        ? angleCfg.WithRequesters(new() {
          Proxy = client.Cfg.Proxies.First().CreateWebProxy(),
          PreAuthenticate = true,
          UseDefaultCredentials = false
        })
        : angleCfg);
  }
}