using System;
using System.Net;

namespace SysExtensions.Net {
  public static class HttpExtensions {
    public static UriBuilder Build(this Uri uri) => new (uri);
    public static Uri AsUri(this string url) => new (url);

    public static void EnsureSuccess(int status, string url) {
      if (!IsSuccess(status)) throw new InvalidOperationException($"{url} failed with  '{status}'");
    }
    public static void EnsureSuccess(this HttpStatusCode status) {
      if (!status.IsSuccess()) throw new InvalidOperationException($"Failure status '{(int) status}' ");
    }
    public static bool IsSuccess(int code) => code >= 200 && code <= 299;
    public static bool IsSuccess(this HttpStatusCode code) => IsSuccess((int)code);
    public static bool IsTransient(int code) => code switch {
        < 500 => code == 408,
        _ => true
      };
    public static bool IsTransient(this HttpStatusCode code) => IsTransient((int) code);
  }
}