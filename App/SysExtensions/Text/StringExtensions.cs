using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using SysExtensions.Collections;

namespace SysExtensions.Text {
  public static class StringExtensions {
    static readonly SHA256 _hash = SHA256.Create();

    public static Stream AsStream(this string content) => new MemoryStream(Encoding.UTF8.GetBytes(content));

    public static TextReader AsTextStream(this string content) => new StreamReader(new MemoryStream(Encoding.UTF8.GetBytes(content)));

    public static async Task<string> AsString(this Stream stream) {
      using (var sr = new StreamReader(stream)) return await sr.ReadToEndAsync();
    }

    /// <summary>
    ///   Is not null or emtpy
    /// </summary>
    public static bool HasValue(this string value) => !value.NullOrEmpty();

    /// <summary>
    ///   Like string.Join. However also will escape the seperator and escape charachter so this is reversable using Split
    /// </summary>
    public static string Join<T>(this IEnumerable<T> items, string separator, Func<T, string> format = null, char? escapeCharacter = null) {
      format = format ?? (s => s.ToString());
      var escapeFormat = format;
      if (escapeCharacter != null)
        escapeFormat = s =>
          format(s).Replace(escapeCharacter.Value.ToString(), escapeCharacter.ToString() + escapeCharacter)
            .Replace(separator, escapeCharacter.ToString() + separator);
      return string.Join(separator, items.NotNull().Select(escapeFormat));
    }

    public static bool NullOrEmpty(this string value) => string.IsNullOrEmpty(value);

    public static IEnumerable<string> UnJoin(this string input, char separator, char escapeCharacter = '\\') {
      if (input == null) yield break;
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

        yield return itemBuffer;
        itemBuffer = "";
      }

      yield return itemBuffer;
    }

    public static byte[] ToBytesUtf8(this string s) => Encoding.UTF8.GetBytes(s);

    public static byte[] ToBytesFromBase64(this string s) => Convert.FromBase64String(s);

    public static string ToBase64String(this byte[] b) => Convert.ToBase64String(b);

    public static string ToStringFromUtf8(this byte[] b) => Encoding.UTF8.GetString(b);

    public static string Hash(this string content) => _hash.ComputeHash(content.ToBytesUtf8()).ToBase64String();

    public static string SurroundWith(this string s, string prefix, string suffix) => prefix + s + suffix;

    //public static string ToStringOrEmpty(this object o) => o == null ? "" : o.ToString();

    /// <summary>
    ///   Provides an append method with terse defintiions of the format and condition for appending the item.
    /// </summary>
    public static string Add<T>(this string s, T item, Func<T, string> format = null, Func<T, bool> condition = null) {
      format = format ?? (o => o.ToString());
      condition = condition ?? (o => o != null);
      return condition(item) ? s + format(item) : s;
    }

    public static string AddJoin<T>(this string s, string sep, params T[] items) {
      var list = items.NotNull().ToList();
      return s + (list.IsEmpty() ? "" : (s.EndsWith(sep) ? "" : sep) + list.Join(sep));
    }

    /// <summary>
    ///   Usefull in interpolated stirngs because you can't use ternary op
    /// </summary>
    public static string Text(this bool b, string trueString, string falseString = "") => b ? trueString : falseString;
  }
}