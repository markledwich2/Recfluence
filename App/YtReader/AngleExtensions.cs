using AngleSharp.Dom;
using SysExtensions.Net;

namespace YtReader {
  public static class AngleExtensions {
    public static T Qs<T>(this IParentNode b, string selector) where T : class, IElement => b.QuerySelector(selector) as T;
    public static void EnsureSuccess(this IDocument doc) => doc.StatusCode.EnsureSuccess(doc.Url);
  }
}