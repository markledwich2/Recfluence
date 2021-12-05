using System.Diagnostics;
using SysExtensions.Collections;

namespace SysExtensions.Threading;

public static class Def {
  public static Action Fun(Action act) => act;

  /// <summary>Create a func with type inference</summary>
  public static Func<T> Fun<T>(Func<T> func) => func;

  public static Func<T, R> Fun<T, R>(Func<T, R> func) => func;
}

public static class TaskExtensions {
  /// <summary>Waits for all of the tasks, however will cancel the tasks and throw an exception if any of the tasks have a
  ///   fault</summary>
  /// <param name="tasks"></param>
  /// <param name="cancelSource"></param>
  /// <returns></returns>
  public static async Task WhenAllCancelOnException(this IEnumerable<Task> tasks, CancellationTokenSource cancelSource) {
    var tList = tasks.ToList();
    while (tList.HasItems()) {
      var t = await Task.WhenAny(tList);
      if (t.IsFaulted) {
        cancelSource.Cancel();
        await t; // will throw if there was an error
      }

      tList.Remove(t);
    }
  }

  public static Task Delay(this TimeSpan timespan, CancellationToken cancel = default) => Task.Delay(timespan, cancel);

  /// <summary>Executes the tasks in order. Completing tasks trigger the next one to start</summary>
  public static IEnumerable<Task<T>> Interleaved<T>(this IEnumerable<Task<T>> tasks) {
    var inputTasks = tasks.ToList();
    var sources = Enumerable.Range(start: 0, inputTasks.Count).Select(_ => new TaskCompletionSource<T>()).ToList();
    var nextTaskIndex = -1;
    foreach (var inputTask in inputTasks)
      inputTask.ContinueWith(completed => {
          var source = sources[Interlocked.Increment(ref nextTaskIndex)];
          if (completed.IsFaulted)
            source.TrySetException(completed.Exception.InnerExceptions);
          else if (completed.IsCanceled)
            source.TrySetCanceled();
          else
            source.TrySetResult(completed.Result);
        }, CancellationToken.None,
        TaskContinuationOptions.ExecuteSynchronously,
        TaskScheduler.Default);
    return from source in sources
      select source.Task;
  }

  public static Task<T[]> InterleavedWhenAll<T>(this IEnumerable<Task<T>> tasks) {
    var inputs = tasks.ToList();
    var ce = new CountdownEvent(inputs.Count);
    var tcs = new TaskCompletionSource<T[]>();

    Action<Task> onCompleted = completed => {
      if (completed.IsFaulted)
        tcs.TrySetException(completed.Exception.InnerExceptions);
      if (ce.Signal() && !tcs.Task.IsCompleted)
        tcs.TrySetResult(inputs.Select(t => t.Result).ToArray());
    };

    foreach (var t in inputs) t.ContinueWith(onCompleted);
    return tcs.Task;
  }

  public static async Task<TimeSpan> WithDuration(this Task task) {
    var sw = Stopwatch.StartNew();
    await task;
    sw.Stop();
    return sw.Elapsed;
  }

  public static async Task<(T Result, TimeSpan Duration)> WithDuration<T>(this Task<T> task) {
    var sw = Stopwatch.StartNew();
    var result = await task;
    sw.Stop();
    return (Result: result, Duration: sw.Elapsed);
  }

  public static Task<(T Result, TimeSpan Duration)> WithDuration<T>(this ValueTask<T> task) => task.AsTask().WithDuration();

  public static void Wait(this Task task) => task.GetAwaiter().GetResult();

  public static async Task<IReadOnlyCollection<T>> SelectManyList<T>(this IAsyncEnumerable<IReadOnlyCollection<T>> items) {
    var res = new List<T>();
    await foreach (var list in items) res.AddRange(list);
    return res;
  }

  public static async Task<bool> WithTimeout(this Task task, TimeSpan timeout, CancellationToken cancel = default) =>
    await WithTimeoutInner(task, timeout, cancel);

  /// <summary>Runs the task that will be abandoned after a given timeout. NOTE: does ot throw any Cancellation/Timeout
  ///   exception, this info is part of the result</summary>
  public static async Task<(bool finished, TResult res)> WithTimeout<TResult>(this Task<TResult> task, TimeSpan timeout,
    CancellationToken cancel = default) {
    var success = await WithTimeoutInner(task, timeout, cancel);
    return success ? (true, await task) : (false, default);
  }

  static async Task<bool> WithTimeoutInner(Task task, TimeSpan timeout, CancellationToken cancel) {
    if (timeout <= TimeSpan.Zero) return false; // if the given timeout is 0, return immediately as not complete
    var delayCancel = CancellationTokenSource.CreateLinkedTokenSource(cancel);
    var completedTask = await Task.WhenAny(task, timeout.Delay(delayCancel.Token));
    delayCancel.Cancel();
    var success = completedTask == task;
    return success;
  }

  /// <summary>Waits for `task` to complete, and after than executes `then`. If the result of task is IDisposable, it will
  ///   call dispose after then is executed. This is nice for things like DB connections</summary>
  public static async Task<TR> Then<T, TR>(this Task<T> task, Func<T, Task<TR>> then, bool dispose = true) {
    var r = await task;
    var res = await then(r);
    if (dispose) await r.TryDispose();
    return res;
  }

  /// <summary>Waits for `task` to complete, and after than executes `then`. If the result of task is IDisposable, it will
  ///   call dispose after then is executed. This is nice for things like DB connections</summary>
  public static async Task Then<T>(this Task<T> task, Action<T> then, bool dispose = true) {
    var r = await task;
    then(r);
    if (dispose) await r.TryDispose();
  }

  /// <summary>Waits for `task` to complete, and after than executes `then`. If the result of task is IDisposable, it will
  ///   call dispose after then is executed. This is nice for things like DB connections</summary>
  public static async Task Then<T>(this Task<T> task, Func<T, Task> then, bool dispose = true) {
    var r = await task;
    await then(r);
    if (dispose) await r.TryDispose();
  }

  /// <summary>Waits for `task` to complete, and after than executes `then`. If the result of task is IDisposable, it will
  ///   call dispose after then is executed. This is nice for things like DB connections</summary>
  public static async Task<TR> Then<T, TR>(this Task<T> task, Func<T, TR> then, bool dispose = true) {
    var r = await task;
    var res = then(r);
    if (res is Task t) await t;
    if (dispose) await r.TryDispose();
    return res;
  }

  /// <summary>Waits for `task` to complete, and after than executes `then`. If the result of task is IDisposable, it will
  ///   call dispose after then is executed. This is nice for things like DB connections</summary>
  public static async ValueTask<TR> Then<T, TR>(this ValueTask<T> task, Func<T, ValueTask<TR>> then, bool dispose = true) {
    var r = await task;
    var res = await then(r);
    if (dispose) await r.TryDispose();
    return res;
  }

  /// <summary>Waits for `task` to complete, and after than executes `then`. If the result of task is IDisposable, it will
  ///   call dispose after then is executed. This is nice for things like DB connections</summary>
  public static async ValueTask<TR> Then<T, TR>(this ValueTask<T> task, Func<T, TR> then, bool dispose = true) {
    var r = await task;
    var res = then(r);
    if (dispose) await r.TryDispose();
    return res;
  }

  public static async Task TryDispose<T>(this T r) {
    if (r is IAsyncDisposable a) await a.DisposeAsync();
    else if (r is IDisposable d) d.Dispose();
  }
}