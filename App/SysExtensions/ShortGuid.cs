using System;
using SysExtensions.Text;

namespace SysExtensions; 

public static class ShortGuid {
  public static string ToShortString(this Guid guid, int? length = null) {
    var base64Guid = Convert.ToBase64String(guid.ToByteArray())
      .Replace(oldChar: '+', newChar: '-').Replace(oldChar: '/', newChar: '_');
    var s = base64Guid.Substring(startIndex: 0, base64Guid.Length - 2);
    return length.HasValue ? s.Right(length.Value) : s;
  }

  public static string Create(int? length = null) => Guid.NewGuid().ToShortString(length);
}