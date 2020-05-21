﻿using System;
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
    static readonly ConcurrentDictionary<Type, ConcurrentDictionary<string, Enum>> StringToEnumCache =
      new ConcurrentDictionary<Type, ConcurrentDictionary<string, Enum>>();

    static readonly ConcurrentDictionary<Type, ConcurrentDictionary<Enum, string>> EnumExplicitNameCache =
      new ConcurrentDictionary<Type, ConcurrentDictionary<Enum, string>>();

    public static string EnumExplicitName<T>(this T value) where T : IConvertible {
      var fieldInfo = value.GetType().GetTypeInfo().GetField(value.ToString(CultureInfo.InvariantCulture));
      var enumMember = (EnumMemberAttribute) fieldInfo.GetCustomAttributes(typeof(EnumMemberAttribute), false).FirstOrDefault();
      return enumMember?.Value;
    }

    public static string EnumExplicitName(Enum value) {
      var t = value.GetType();
      var s = EnumExplicitNameCache.GetOrAdd(t, k => new ConcurrentDictionary<Enum, string>())
        .GetOrAdd(value, k => EnumMemberAttribute(value, t)?.Value);
      return s;
    }

    static EnumMemberAttribute EnumMemberAttribute(Enum value, Type t) =>
      (EnumMemberAttribute) t.GetField(value.ToString()).GetCustomAttribute(typeof(EnumMemberAttribute), false);

    /// <summary>Converts the enum value to a string, taking into account the EnumMember attribute. Uses convertible because
    ///   that is the closes type that can be used to Enum and don't want too many extensions on object</summary>
    public static string EnumString(this Enum value) =>
      value.EnumExplicitName() ?? value.ToString();

    public static string EnumString<T>(this T value) where T : Enum => EnumString(value as Enum);

    public static bool TryToEnum<T>(this string s, out T value) where T : Enum {
      var enumValue = ToEnum<T>(s);
      if (enumValue == null)
        value = default;
      else
        value = enumValue;

      return enumValue != null;
    }

    public static T ToEnum<T>(this string s, bool ensureFound = true, Type t = null, Func<Enum, string> defaultEnumString = null) where T : Enum {
      t ??= typeof(T);
      return (T) ToEnum(s, t, ensureFound, defaultEnumString);
    }

    public static object ToEnum(this string s, Type t, bool ensureFound = true, Func<Enum, string> defaultEnumString = null) {
      defaultEnumString ??= e => e.ToString();

      Enum enumValue;
      var enumCache = StringToEnumCache.GetOrAdd(t, k => new ConcurrentDictionary<string, Enum>(StringComparer.OrdinalIgnoreCase));
      var found = enumCache.TryGetValue(s, out enumValue);

      // initialize if missing (not just if first cache miss) because there may be different defaultEnumString() functions depending on the context (e.g. serialization settings)
      if (!found) {
        foreach (var e in Enum.GetValues(t).Cast<Enum>())
          enumCache.GetOrAdd(e.EnumExplicitName() ?? defaultEnumString(e), key => e);
        found = enumCache.TryGetValue(s, out enumValue);
      }

      if (ensureFound && !found) throw new InvalidCastException($"Unable to cast ({s}) to {t.Name}");
      if(enumValue == null) enumValue = (Enum)t.DefaultForType(); // enumCache.TryGetValue will set to null instead of the default enum value when not found
      return enumValue;
    }

    public static IEnumerable<T> Values<T>() where T : IConvertible => Enum.GetValues(typeof(T)).Cast<T>();

    public static bool In<T>(this T value, params T[] values) where T : IComparable => values.Contains(value);
    public static bool NotIn<T>(this T value, params T[] values) where T : IComparable => !value.In(values);
  }
}