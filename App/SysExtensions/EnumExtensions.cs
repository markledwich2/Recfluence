using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;

//using Mutuo.SystemExtensions.Collections;

namespace SysExtensions {
  public static class EnumExtensions {
    static readonly ConcurrentDictionary<Type, ConcurrentDictionary<string, Enum>> StringToEnumCache =
      new ConcurrentDictionary<Type, ConcurrentDictionary<string, Enum>>();

    public static string EnumExplicitName<T>(this T value) where T : IConvertible {
      var fieldInfo = value.GetType().GetTypeInfo().GetField(value.ToString(CultureInfo.InvariantCulture));
      var enumMember = (EnumMemberAttribute) fieldInfo.GetCustomAttributes(typeof(EnumMemberAttribute), false).FirstOrDefault();
      return enumMember?.Value;
    }

    static readonly ConcurrentDictionary<Type, ConcurrentDictionary<Enum, string>> EnumExplicitNameCache =
      new ConcurrentDictionary<Type, ConcurrentDictionary<Enum, string>>();

    public static string EnumExplicitName(Enum value) {
      var t = value.GetType();
      var s = EnumExplicitNameCache.GetOrAdd(t, k => new ConcurrentDictionary<Enum, string>())
        .GetOrAdd(value, k => EnumMemberAttribute(value, t)?.Value);
      return s;
    }

    static EnumMemberAttribute EnumMemberAttribute(Enum value, Type t) =>
      (EnumMemberAttribute) t.GetField(value.ToString()).GetCustomAttribute(typeof(EnumMemberAttribute), false);

    /// <summary>
    ///   Converts the enum value to a string, taking into account the EnumMember attribute.
    ///   Uses convertible because that is the closes type that can be used to Enum and don't want too many extensions on
    ///   object
    /// </summary>
    public static string EnumString(this Enum value) =>
      value.EnumExplicitName() ?? value.ToString();

    public static string EnumString<T>(this T value) where T : IConvertible => EnumString(value as Enum);

    public static bool TryToEnum<T>(this string s, out T value) where T : IConvertible {
      var enumValue = ToEnum(s, typeof(T));
      if (enumValue == null)
        value = default(T);
      else
        value = (T) (IConvertible) enumValue;

      return enumValue != null;
    }

    /// <summary>
    ///   Converts a string to an enum value. using the EnumMember attribute if it exists
    /// </summary>
    public static T ToEnum<T>(this string s) where T : IConvertible {
      var t = typeof(T);
      var enumValue = (IConvertible) ToEnum(s, t);
      return (T) enumValue;
    }

    public static Enum ToEnum(string s, Type t, Func<Enum, string> defaultEnumString = null, bool ensureFound = true) {
      defaultEnumString = defaultEnumString ?? (e => e.ToString());

      var enumCache = StringToEnumCache.GetOrAdd(t, k => new ConcurrentDictionary<string, Enum>(StringComparer.OrdinalIgnoreCase));
      var found = enumCache.TryGetValue(s, out var enumValue);

      // initialize if missing (not just if first cache miss) because there may be different defaultEnumString() functions depending on the context (e.g. serialization settings)
      if (!found) {
        foreach (var e in Enum.GetValues(t).Cast<Enum>())
          enumCache.GetOrAdd(e.EnumExplicitName() ?? defaultEnumString(e), key => e);
        found = enumCache.TryGetValue(s, out enumValue);
      }

      if (ensureFound && !found) throw new InvalidCastException($"Unable to cast ({s}) to {t.Name}");
      return enumValue;
    }

    public static IEnumerable<T> Values<T>() where T : IConvertible => Enum.GetValues(typeof(T)).Cast<T>();

    public static bool In<T>(this T value, params T[] values) where T : IComparable => values.Contains(value);
    public static bool NotIn<T>(this T value, params T[] values) where T : IComparable => !value.In(values);
  }
}