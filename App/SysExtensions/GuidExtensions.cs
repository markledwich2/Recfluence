using System;

namespace SysExtensions {
  public static class GuidExtensions {
    public static string ToShortString(this Guid guid) {
      var base64Guid = Convert.ToBase64String(guid.ToByteArray())
        .Replace('+', '-').Replace('/', '_');
      return base64Guid.Substring(0, base64Guid.Length - 2);
    }
    public static string NewShort() => Guid.NewGuid().ToShortString();
  }
}