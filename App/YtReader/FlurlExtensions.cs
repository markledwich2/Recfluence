using Flurl;

namespace YtReader {
  public static class FlurlExtensions {
    public static Url AsUrl(this string url) => new(url);
  }
}