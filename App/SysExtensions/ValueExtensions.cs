namespace SysExtensions;

public static class ValueExtensions {
  /// <summary>Run a func on a thing. Does not run when thing is null. An easy to way make something fluent and null safe.</summary>
  public static TR Dot<T, TR>(this T? thing, Func<T, TR> fun) where T : struct => thing.HasValue ? fun(thing.Value) : default;

  /// <summary>Run a func on a thing. Does not run when thing is null. An easy to way make something fluent and null safe.</summary>
  public static TR Dot<T, TR>(this T thing, Func<T, TR> fun) where T : class => thing is null ? default : fun(thing);

  /// <summary>Run a func on a collection. Does not run when thing is null or emtpy. An easy to way make something fluent and
  ///   null safe.</summary>
  public static TR Dot<T, TR>(this ICollection<T> thing, Func<ICollection<T>, TR> fun) => thing?.Any() == true ? fun(thing) : default;

  public static TR Dot<T, TR>(this IReadOnlyCollection<T> thing, Func<IReadOnlyCollection<T>, TR> fun) => thing?.Any() == true ? fun(thing) : default;
  public static TR Dot<T, TR>(this T[] thing, Func<T[], TR> fun) => thing?.Any() == true ? fun(thing) : default;

  public static T Clamp<T>(this T v, T min, T max) where T : IComparable<T> => v.CompareTo(max) > 0 ? max : v.CompareTo(min) < 0 ? min : v;

  public static T Min<T>(this T a, T b) where T : IComparable<T> => a.CompareTo(b) <= 0 ? a : b;
  public static T Max<T>(this T a, T b) where T : IComparable<T> => a.CompareTo(b) >= 0 ? a : b;
}