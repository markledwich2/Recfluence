using Nito.AsyncEx;
using SysExtensions.Reflection;

namespace SysExtensions.Threading;

public class Defer<T> : IAsyncDisposable {
  readonly SemaphoreSlim _lock = new SemaphoreSlim(initialCount: 1, maxCount: 1);
  public Defer(Func<Task<T>> creator) => Creator = creator;
  Func<Task<T>> Creator { get; }
  public T      Value;

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

  public async ValueTask DisposeAsync() {
    _lock?.Dispose();
    if (Value == null) return;
    if (Value is IAsyncDisposable a) await a.DisposeAsync();
    else if (Value is IDisposable d) d.Dispose();
  }
}

public class Defer<T, TParam> {
  readonly SemaphoreSlim _lock = new SemaphoreSlim(initialCount: 1, maxCount: 1);
  public Defer(Func<TParam, Task<T>> creator) => Creator = creator;
  Func<TParam, Task<T>> Creator { get; }
  public T              Value;

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