using System;
using System.Threading;
using System.Threading.Tasks;
using SysExtensions.Reflection;

namespace SysExtensions.Threading {
  public class AsyncLazy<T> {
    readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
    public AsyncLazy(Func<Task<T>> creator) => Creator = creator;
    Func<Task<T>> Creator { get; }
    T             Value;

    public async Task<T> GetOrCreate() {
      if (!Value.NullOrDefault())
        return Value;
      using (await _lock.LockAsync()) {
        if (!Value.NullOrDefault())
          return Value; // check a second time within the lock to avoid race condition and needless locking
        Value = await Creator();
      }
      return Value;
    }
  }

  public class AsyncLazy<T, TParam> {
    readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);
    public AsyncLazy(Func<TParam, Task<T>> creator) => Creator = creator;
    Func<TParam, Task<T>> Creator { get; }
    T                     Value;

    public async Task<T> GetOrCreate(TParam param) {
      if (!Value.NullOrDefault())
        return Value;
      using (await _lock.LockAsync()) {
        if (!Value.NullOrDefault())
          return Value; // check a second time within the lock to avoid race condition and needless locking
        Value = await Creator(param);
      }

      return Value;
    }
  }

  public static class SemaphoreExtensions {
    public static async Task<LockReleaser> LockAsync(this SemaphoreSlim semaphore) {
      await semaphore.WaitAsync();
      return new LockReleaser(semaphore);
    }
  }

  public struct LockReleaser : IDisposable {
    readonly SemaphoreSlim _semaphore;

    internal LockReleaser(SemaphoreSlim toRelease) => _semaphore = toRelease;

    public void Dispose() => _semaphore?.Release();
  }

  // https://blogs.msdn.microsoft.com/pfxteam/2012/02/11/building-async-coordination-primitives-part-1-asyncmanualresetevent/
  public class AsyncManualResetEvent {
    volatile TaskCompletionSource<bool> _tcs = new TaskCompletionSource<bool>();

    public Task WaitAsync() => _tcs.Task;

    public void Set() => _tcs.TrySetResult(true);

    public void Reset() {
      while (true) {
        var tcs = _tcs;
        if (!tcs.Task.IsCompleted ||
            Interlocked.CompareExchange(ref _tcs, new TaskCompletionSource<bool>(), tcs) == tcs)
          return;
      }
    }
  }
}