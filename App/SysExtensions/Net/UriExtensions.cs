using System;

namespace SysExtensions.Net {
  public static class UriExtensions {
    public static UriBuilder Build(this Uri uri) => new UriBuilder(uri);
    
    public static Uri AsUri(this string url) => new Uri(url);
  }
}