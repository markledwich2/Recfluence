using System;
using System.Globalization;

namespace SysExtensions {
  public static class DateTimeExtensions {
    public static string FileSafeTimestamp(this DateTime value) => value.ToString("yyyy-MM-dd_HH-mm-ss-fffffff", CultureInfo.InvariantCulture);
    public static DateTime ParseFileSafeTimestamp(this string ts) => DateTime.ParseExact(ts, "yyyy-MM-dd_HH-mm-ss-fffffff", CultureInfo.InvariantCulture);

    public static string DateString(this DateTime value) => value.ToString("yyyy-MM-dd");

    public static DateTime Epoc { get; } = new DateTime(1970, 1, 1);
  }
}