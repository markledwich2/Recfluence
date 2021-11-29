﻿using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Text.RegularExpressions;
using Humanizer;
using SysExtensions.Collections;
using static System.Text.RegularExpressions.RegexOptions;

namespace SysExtensions.Text; 

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
  public static string NullIfEmpty(this string value) => string.IsNullOrEmpty(value) ? null : value;

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
    format ??= o => o.ToString();
    condition ??= o => o != null;
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

  public static int? TryParseInt(this string s, NumberStyles styles = NumberStyles.Any) =>
    int.TryParse(s, styles, NumberFormatInfo.InvariantInfo, out var l) ? l : null;

  public static long ParseLong(this string s, NumberStyles styles = NumberStyles.Any) => long.Parse(s, styles, NumberFormatInfo.InvariantInfo);
  public static ulong ParseULong(this string s, NumberStyles styles = NumberStyles.Any) => ulong.Parse(s, styles, NumberFormatInfo.InvariantInfo);

  public static ulong? TryParseULong(this string s, NumberStyles styles = NumberStyles.Any) =>
    ulong.TryParse(s, styles, NumberFormatInfo.InvariantInfo, out var l) ? l : null;

  public static DateTime ParseExact(this string date, string pattern, IFormatProvider format = default, DateTimeStyles style = default) =>
    DateTime.ParseExact(date, pattern, format, style);

  public static decimal ParseDecimal(this string s) => decimal.Parse(s, NumberFormatInfo.InvariantInfo);

  public static double? TryParseDouble(this string s, NumberStyles style = NumberStyles.Any) =>
    double.TryParse(s, style, NumberFormatInfo.InvariantInfo, out var d) ? d : null;

  public static decimal? TryParseDecimal(this string s, NumberStyles style = NumberStyles.Any) =>
    decimal.TryParse(s, style, NumberFormatInfo.InvariantInfo, out var d) ? d : null;

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

  public static string ToCamelCase(this string s) {
    if (s.Contains("_") || s.All(char.IsUpper)) s = s.ToLowerInvariant();
    return s.Camelize();
  }

  public static Match Match(this string input, Regex re) => re.Match(input);

  static readonly Regex HumanNumber = new(@"(?<num>\d+\.?\d*)\s?(?<unit>[KMB]?)", Compiled | IgnoreCase);

  public static double? TryParseNumberWithUnits(this string text) {
    var m = HumanNumber.Match(text);
    if (!m.Success) return null;
    var num = m.Groups["num"].Value.TryParseDouble();
    if (!num.HasValue) return null;
    var unitNum = m.Groups["unit"].Value.ToLowerInvariant() switch {
      "b" => num * 1_000_000_000,
      "m" => num * 1_000_000,
      "k" => num * 1_000,
      _ => num
    };
    return unitNum;
  }
}