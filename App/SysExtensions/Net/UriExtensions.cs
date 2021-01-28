using System;

namespace SysExtensions.Net {
  public static class UriExtensions {
    public static UriBuilder Build(this Uri uri) => new (uri);
    public static Uri AsUri(this string url) => new (url);
  }
}