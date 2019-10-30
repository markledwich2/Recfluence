using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SysExtensions.Collections;

namespace SysExtensions.Threading {
  public static class TaskExtensions {
    /// <summary>
    ///   Waits for all of the tasks, however will cancel the tasks and throw an exception if any of the tasks have a fault
    /// </summary>
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

    public static Task Delay(this TimeSpan timespan) => Task.Delay(timespan);

    /// <summary>
    ///   Executes the tasks in order. Completing tasks trigger the next one to start
    /// </summary>
    public static IEnumerable<Task<T>> Interleaved<T>(this IEnumerable<Task<T>> tasks) {
      var inputTasks = tasks.ToList();
      var sources = Enumerable.Range(0, inputTasks.Count).Select(_ => new TaskCompletionSource<T>()).ToList();
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

    public static async Task<TimedResult<T>> WithDuration<T>(this Task<T> task) {
      var sw = Stopwatch.StartNew();
      var result = await task;
      sw.Stop();
      return new TimedResult<T> {Duration = sw.Elapsed, Result = result};
    }

    public static void Wait(this Task task) => task.GetAwaiter().GetResult();

    public static async Task<IReadOnlyCollection<T>> SelectManyList<T>(this IAsyncEnumerable<IReadOnlyCollection<T>> items) {
      var res = new List<T>();
      await foreach (var list in items) res.AddRange(list);
      return res;
    }
  }

  public class TimedResult<T> {
    public TimeSpan Duration { get; set; }
    public T Result { get; set; }
  }
}