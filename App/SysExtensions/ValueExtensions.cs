using System;

namespace SysExtensions {
  public static class ValueExtensions {
    public static TR Do<T, TR>(this T? thing, Func<T, TR> fun) where T : struct => thing.HasValue ? fun(thing.Value) : default;

    /// <summary>Run a func on a thing. Does not run when thing is null. An easy to way make something fluent and safe.</summary>
    public static TR Do<T, TR>(this T thing, Func<T, TR> fun) where T : class => thing == null ? default : fun(thing);
  }
}