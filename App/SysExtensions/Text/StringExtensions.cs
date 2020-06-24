using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using SysExtensions.Collections;

namespace SysExtensions.Text {
  public static class StringExtensions {
    static readonly SHA256 _hash = SHA256.Create();

    public static Stream AsStream(this string content) => new MemoryStream(Encoding.UTF8.GetBytes(content));

    public static TextReader AsTextStream(this string content) => new StreamReader(new MemoryStream(Encoding.UTF8.GetBytes(content)));

    public static TextReader TextStream(this Stream stream, Encoding encoding = null) => new StreamReader(stream, encoding ?? Encoding.UTF8);

    public static string AsString(this Stream stream) {
      using (var sr = new StreamReader(stream)) return sr.ReadToEnd();
    }

    /// <summary>Is not null or emtpy</summary>
    public static bool HasValue(this string value) => !value.NullOrEmpty();

    public static string EmptyIfNull(this string s) => s ?? string.Empty;

    /// <summary>Like string.Join. However also will escape the seperator and escape charachter so this is reversable using
    ///   Split</summary>
    public static string Join<T>(this IEnumerable<T> items, string separator, Func<T, string> format = null, char? escapeCharacter = null) {
      format ??= s => s.ToString();
      var escapeFormat = format;
      if (escapeCharacter != null)
        escapeFormat = s =>
          format(s).Replace(escapeCharacter.Value.ToString(), escapeCharacter.ToString() + escapeCharacter)
            .Replace(separator, escapeCharacter + separator);
      return string.Join(separator, items.NotNull().Select(escapeFormat));
    }

    public static bool NullOrEmpty(this string value) => string.IsNullOrEmpty(value);

    public static string[] UnJoin(this string input, char separator, char escapeCharacter = '\\') {
      var res = new List<string>();
      if (input == null) return res.ToArray();
      var itemBuffer = "";
      var wasEscaped = false; //i > 0 && input[i - 1] == escapeCharacter;
      foreach (var c in input) {
        if (wasEscaped) {
          if (c != escapeCharacter && c != separator)
            throw new FormatException($"input contains unknown escape sequence ({escapeCharacter}{c})");
          itemBuffer += c; // add the escped spcial char
          wasEscaped = false;
          continue;
        }

        if (c == escapeCharacter) {
          wasEscaped = true;
          continue;
        }

        if (c != separator) {
          itemBuffer += c;
          continue;
        }

        res.Add(itemBuffer);
        itemBuffer = "";
      }

      res.Add(itemBuffer);
      return res.ToArray();
    }

    public static byte[] ToBytesUtf8(this string s) => Encoding.UTF8.GetBytes(s);

    public static byte[] ToBytesFromBase64(this string s) => Convert.FromBase64String(s);

    public static string ToBase64String(this byte[] b) => Convert.ToBase64String(b);

    public static string ToStringFromUtf8(this byte[] b) => Encoding.UTF8.GetString(b);

    public static string Hash(this string content) => _hash.ComputeHash(content.ToBytesUtf8()).ToBase64String();

    //public static string ToStringOrEmpty(this object o) => o == null ? "" : o.ToString();

    /// <summary>Provides an append method with terse defintiions of the format and condition for appending the item.</summary>
    public static string Add<T>(this string s, T item, Func<T, string> format = null, Func<T, bool> condition = null) {
      format = format ?? (o => o.ToString());
      condition = condition ?? (o => o != null);
      return condition(item) ? s + format(item) : s;
    }

    public static string AddJoin<T>(this string s, string sep, params T[] items) {
      var list = items.NotNull().ToList();
      return s + (list.IsEmpty() ? "" : (s.EndsWith(sep) ? "" : sep) + list.Join(sep));
    }

    /// <summary>Usefull in interpolated stirngs because you can't use ternary op</summary>
    public static string Text(this bool b, string trueString, string falseString = "") => b ? trueString : falseString;

    public static string SubstringAfter(this string s, string sub,
      StringComparison comparison = StringComparison.Ordinal) {
      var index = s.IndexOf(sub, comparison);
      return index < 0 ? string.Empty : s.Substring(index + sub.Length, s.Length - index - sub.Length);
    }

    public static string StripNonDigit(this string s) => Regex.Replace(s, "\\D", "");

    public static int ParseInt(this string s) => int.Parse(s);

    public static long ParseLong(this string s) {
      const NumberStyles styles = NumberStyles.AllowThousands;
      var format = NumberFormatInfo.InvariantInfo;
      return long.Parse(s, styles, format);
    }

    public static DateTime ParseDate(this string s, IFormatProvider format = null, DateTimeStyles style = default) => DateTime.Parse(s, format, style);

    public static decimal ParseDecimal(this string s) => decimal.Parse(s, NumberFormatInfo.InvariantInfo);

    public static DateTimeOffset ParseDateTimeOffset(this string s, string format) =>
      DateTimeOffset.ParseExact(s, format, DateTimeFormatInfo.InvariantInfo, DateTimeStyles.AssumeUniversal);

    public static bool IsNullOrWhiteSpace(this string s) => string.IsNullOrWhiteSpace(s);

    public static string Right(this string source, int length) =>
      length >= source.Length ? source : source.Substring(source.Length - length);

    public static void Deconstruct<T>(this T[] list, out T first, out IList<T> rest) {
      first = list.Length > 0 ? list[0] : default;
      rest = list.Skip(1).ToList();
    }

    public static void Deconstruct<T>(this T[] list, out T first, out T second, out IList<T> rest) {
      first = list.Length > 0 ? list[0] : default;
      second = list.Length > 1 ? list[1] : default;
      rest = list.Skip(2).ToList();
    }
  }
}