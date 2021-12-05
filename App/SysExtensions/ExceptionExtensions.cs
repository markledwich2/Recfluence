namespace SysExtensions;

public static class ExceptionExtensions {
  public static async Task<T> WithWrappedException<T>(this Task<T> task, Func<Exception, string> getMesage, ILogger log = null) {
    try {
      return await task;
    }
    catch (Exception ex) {
      var msg = getMesage(ex);
      log?.Error(ex, msg);
      throw new InvalidOperationException(msg, ex);
    }
  }

  /// <summary>Wraps and throws the exception. The error is logged if log is provided.</summary>
  /// <param name="task"></param>
  /// <param name="message"></param>
  /// <param name="log">if provided, errors will be logged</param>
  /// <typeparam name="T"></typeparam>
  /// <returns></returns>
  /// <exception cref="InvalidOperationException"></exception>
  public static async Task<T> WithWrappedException<T>(this Task<T> task, string message, ILogger log = null) {
    try {
      return await task;
    }
    catch (Exception ex) {
      log?.Error(ex, message);
      throw new InvalidOperationException(message, ex);
    }
  }

  public static async Task WithWrappedException(this Task task, string taskDescription, ILogger log = null) {
    try {
      await task;
    }
    catch (Exception ex) {
      var msg = $"Unhandled error performing ({taskDescription}): {ex.Message}";
      log?.Error(ex, msg);
      throw new InvalidOperationException(msg, ex);
    }
  }

  /// <summary>Allows you to log exception without changing behaviour. It re-throws the exception</summary>
  public static async Task WithOnError(this Task task, Action<Exception> onError) {
    try {
      await task;
    }
    catch (Exception ex) {
      onError(ex);
      throw;
    }
  }

  /// <summary>Allows you to log exception without changing behaviour. It re-throws the exception</summary>
  public static async Task<T> WithOnError<T>(this Task<T> task, Action<Exception> onError) {
    try {
      return await task;
    }
    catch (Exception ex) {
      onError(ex);
      throw;
    }
  }

  /// <summary>Allows you to log exception without changing behaviour. It re-throws the exception</summary>
  public static async ValueTask OnError(this ValueTask task, Action<Exception> onError) {
    try {
      await task;
    }
    catch (Exception ex) {
      onError(ex);
      throw;
    }
  }

  /// <summary>Allows you to log exception without changing behaviour. It re-throws the exception</summary>
  public static async ValueTask<T> OnError<T>(this ValueTask<T> task, Action<Exception> onError) {
    try {
      return await task;
    }
    catch (Exception ex) {
      onError(ex);
      throw;
    }
  }

  public static async ValueTask<T> Swallow<T>(this ValueTask<T> task, Action<Exception> onError) {
    try {
      return await task;
    }
    catch (Exception ex) {
      onError(ex);
      return default;
    }
  }

  public static async Task<T> Swallow<T>(this Task<T> task, Action<Exception> onError = null) {
    try {
      return await task;
    }
    catch (Exception ex) {
      onError?.Invoke(ex);
      return default;
    }
  }

  public static async Task Swallow(this Task task, Action<Exception> onError = null) {
    try {
      await task;
    }
    catch (Exception ex) {
      onError?.Invoke(ex);
    }
  }

  public static T Swallow<T>(this Func<T> task, Action<Exception> onError = null) {
    try {
      return task();
    }
    catch (Exception ex) {
      onError?.Invoke(ex);
      return default;
    }
  }

  public static Exception Try(this Action action) {
    try {
      action();
      return null;
    }
    catch (Exception ex) {
      return ex;
    }
  }

  public static async Task<Exception> Try(this Task task) {
    try {
      await task;
      return null;
    }
    catch (Exception ex) {
      return ex;
    }
  }

  public static async Task<Either<T, Exception>> Try<T>(this Task<T> task) {
    try {
      return new(await task);
    }
    catch (Exception ex) {
      return ex;
    }
  }

  public static async Task<(T, Exception)> Try<T>(this Func<Task<T>> task, T defaultValue = default) {
    try {
      return (await task(), default);
    }
    catch (Exception ex) {
      return (defaultValue, ex);
    }
  }

  public static (T Value, Exception Ex) Try<T>(this Func<T> task, T defaultValue = default) {
    try {
      return (task(), default);
    }
    catch (Exception ex) {
      return (defaultValue, ex);
    }
  }

  public static async Task Try<T>(this Task<T> task, Action<T> main, Action<Exception> alt) {
    Either<T, Exception> e;
    try {
      e = new(await task);
    }
    catch (Exception ex) {
      e = new(ex);
    }
    e.Do(main, alt);
  }

  public static async Task Try<T>(this Task<T> task, Func<T, Task> main, Func<Exception, Task> alt) {
    Either<T, Exception> e;
    try {
      e = new(await task);
    }
    catch (Exception ex) {
      e = new(ex);
    }
    await e.Do(main, alt);
  }

  public static async Task Try<T>(this Task<T> task, Action<T> main, Func<Exception, Task> alt) {
    Either<T, Exception> e;
    try {
      e = new(await task);
    }
    catch (Exception ex) {
      e = new(ex);
    }
    await e.Do(main, alt);
  }

  public static void ThrowIfUnrecoverable(this Exception ex) {
    if (ex is OutOfMemoryException)
      throw ex; // nothing is going to work now
  }

  public static IEnumerable<Exception> InnerExceptions(this Exception ex) {
    while (ex.InnerException != null) {
      ex = ex.InnerException;
      yield return ex;
    }
  }

  /// <summary>Unwraps aggregate exceptions if they only have a single inner exception</summary>
  public static Exception Unwrap(this Exception ex) => ex switch {
    AggregateException a => a.InnerExceptions.Count > 1 ? a : a.InnerException,
    _ => ex
  };
}