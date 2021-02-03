using AngleSharp.Dom;

namespace YtReader {
  public static class AngleExtensions {
    public static T Qs<T>(this IParentNode b, string selector) where T : class, IElement => b.QuerySelector(selector) as T;
  }
}