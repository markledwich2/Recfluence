using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.Serialization;
using SysExtensions.Reflection;

//using Mutuo.SystemExtensions.Collections;

namespace SysExtensions {
  public static class EnumExtensions {
    static readonly ConcurrentDictionary<Type, ConcurrentDictionary<string, Enum>> StringToEnumCache = new();

    static readonly ConcurrentDictionary<Type, ConcurrentDictionary<Enum, string>> EnumExplicitNameCache =
      new();

    public static string EnumExplicitName<T>(this T value) where T : IConvertible {
      var fieldInfo = value.GetType().GetTypeInfo().GetField(value.ToString(CultureInfo.InvariantCulture));
      var enumMember = (EnumMemberAttribute) fieldInfo.GetCustomAttributes(typeof(EnumMemberAttribute), inherit: false).FirstOrDefault();
      return enumMember?.Value;
    }

    public static string EnumExplicitName(Enum value) {
      var t = value.GetType();
      var s = EnumExplicitNameCache.GetOrAdd(t, _ => new())
        .GetOrAdd(value, k => EnumMemberAttribute(value, t)?.Value);
      return s;
    }

    static EnumMemberAttribute EnumMemberAttribute(Enum value, Type t) =>
      (EnumMemberAttribute) t.GetField(value.ToString()).GetCustomAttribute(typeof(EnumMemberAttribute), inherit: false);

    /// <summary>Converts the enum value to a string, taking into account the EnumMember attribute. Uses convertible because
    ///   that is the closes type that can be used to Enum and don't want too many extensions on object</summary>
    public static string EnumString(this Enum value) =>
      value.EnumExplicitName() ?? value.ToString();

    public static string EnumString<T>(this T value) where T : Enum => EnumString(value as Enum);

    public static bool TryParseEnum<T>(this string s, out T value) where T : Enum {
      var (found, enumValue) = InnerParseEnum(s, typeof(T));
      value = found ? (T) enumValue : default;
      return found;
    }

    public static T ParseEnum<T>(this string s, bool ensureFound = true, Type t = null, Func<Enum, string> defaultEnumString = null) where T : Enum {
      t ??= typeof(T);
      return (T) ParseEnum(s, t, ensureFound, defaultEnumString);
    }

    public static object ParseEnum(this string s, Type t, bool ensureFound = true, Func<Enum, string> defaultEnumString = null) {
      var (found, enumValue) = InnerParseEnum(s, t, defaultEnumString);
      if (ensureFound && !found) throw new InvalidCastException($"Unable to cast ({s}) to {t.Name}");
      return enumValue ?? (Enum) t.DefaultForType();
    }

    static (bool found, Enum value) InnerParseEnum(string s, Type t, Func<Enum, string> defaultEnumString = null) {
      defaultEnumString ??= e => e.ToString();
      var enumCache = StringToEnumCache.GetOrAdd(t, _ => new(StringComparer.OrdinalIgnoreCase));
      var found = enumCache.TryGetValue(s, out var enumValue);

      // initialize if missing (not just if first cache miss) because there may be different defaultEnumString() functions depending on the context (e.g. serialization settings)
      if (found) return (true, enumValue);

      foreach (var e in Enum.GetValues(t).Cast<Enum>())
        enumCache.GetOrAdd(e.EnumExplicitName() ?? defaultEnumString(e), key => e);
      found = enumCache.TryGetValue(s, out enumValue);
      return (found, enumValue);
    }

    public static IEnumerable<T> Values<T>() where T : IConvertible => Enum.GetValues(typeof(T)).Cast<T>();

    public static bool In<T>(this T value, params T[] values) where T : IComparable => values.Contains(value);
    public static bool NotIn<T>(this T value, params T[] values) where T : IComparable => !value.In(values);
  }
}