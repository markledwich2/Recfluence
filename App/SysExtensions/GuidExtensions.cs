using System;
using SysExtensions.Text;

namespace SysExtensions {
  public static class GuidExtensions {
    public static string ToShortString(this Guid guid, int? legnth = null) {
      var base64Guid = Convert.ToBase64String(guid.ToByteArray())
        .Replace('+', '-').Replace('/', '_');
      var s = base64Guid.Substring(0, base64Guid.Length - 2);
      return legnth.HasValue ? s.Right(legnth.Value) : s;
    }

    public static string NewShort() => Guid.NewGuid().ToShortString();
  }
}