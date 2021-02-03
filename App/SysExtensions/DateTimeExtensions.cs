using System;
using System.Globalization;

namespace SysExtensions {
  public static class DateTimeExtensions {
    public static string FileSafeTimestamp(this DateTime value) => value.ToString("yyyy-MM-dd_HH-mm-ss-fffffff", CultureInfo.InvariantCulture);
    public static DateTime ParseFileSafeTimestamp(this string ts) => DateTime.ParseExact(ts, "yyyy-MM-dd_HH-mm-ss-fffffff", CultureInfo.InvariantCulture);

    public static DateTime? TryParseDateExact(this string s, string format, DateTimeStyles styles = DateTimeStyles.None) =>
      DateTime.TryParseExact(s, format, CultureInfo.InvariantCulture, styles, out var d) ? d : (DateTime?) null;
    
    public static string DateString(this DateTime value) => value.ToString("yyyy-MM-dd");
    public static DateTime Epoc { get; } = new DateTime(1970, 1, 1);
    
    public static TimeSpan? TryParseTimeSpan(this string s) => TimeSpan.TryParse(s, CultureInfo.InvariantCulture, out var d) ? d : null;
    public static TimeSpan? TryParseTimeSpanExact(this string s, params string[] formats) => TimeSpan.TryParseExact(s, formats, CultureInfo.InvariantCulture, out var d) ? d : null;
  }
}