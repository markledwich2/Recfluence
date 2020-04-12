using System;

namespace SysExtensions.Net {
  public static class UriExtensions {
    public static UriBuilder Build(this Uri uri) => new UriBuilder(uri);
  }
}