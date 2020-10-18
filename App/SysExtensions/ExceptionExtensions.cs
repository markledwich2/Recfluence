using System;
using System.Threading.Tasks;
using Serilog;

namespace SysExtensions {
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
    
    
    public static async Task<(T, Exception)> Try<T>(this Task<T> task, T defaultValue = default) {
      try {
        return (await task, default);
      }
      catch (Exception ex) {
        return (defaultValue, ex);
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

    public static (T, Exception) Try<T>(this Func<T> task, T defaultValue = default) {
      try {
        return (task(), default);
      }
      catch (Exception ex) {
        return (defaultValue, ex);
      }
    }

    public static void ThrowIfUnrecoverable(this Exception ex) {
      if (ex is OutOfMemoryException)
        throw ex; // nothing is going to work now
    }
  }
}