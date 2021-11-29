using System;
using System.Threading;
using System.Threading.Tasks;
using Nito.AsyncEx;

namespace YtReader; 

class ResourceCycle<T, TCfg> : IAsyncDisposable
  where TCfg : class
  where T : class {
  readonly Func<TCfg, Task<T>> Create;
  readonly TCfg[]              _configs;
  (T Resource, TCfg Cfg)?      _current;
  readonly SemaphoreSlim       _lock = new SemaphoreSlim(initialCount: 1, maxCount: 1);

  public ResourceCycle(TCfg[] cfg, Func<TCfg, Task<T>> create, int index = 0) {
    Create = create;
    _configs = cfg;
    Idx = index;
  }

  public int Idx { get; private set; }

  public async Task<(T Resource, TCfg Cfg)> Get() {
    var c = _current;
    if (c != null) return c.Value;
    return await NextResource(null).ConfigureAwait(false);
  }

  /// <summary>Will cycle to the next resource if the current one matches the cfg given (reference equality)</summary>
  /// <param name="cfg"></param>
  /// <returns></returns>
  public async Task<(T Resource, TCfg Cfg)> NextResource(T Resource) {
    using var l = await _lock.LockAsync();

    if (_current.HasValue && _current.Value.Resource == Resource) {
      Idx = (Idx + 1) % _configs.Length;
      if (_current.Value.Resource is IAsyncDisposable d)
        await d.DisposeAsync().ConfigureAwait(false);
      _current = null;
    }

    if (_current != null) return _current.Value;

    var cfg = _configs[Idx];
    _current = (await Create(cfg).ConfigureAwait(false), cfg);
    return _current.Value;
  }

  public async ValueTask DisposeAsync() {
    var r = _current?.Resource;
    if (r == null) return;
    if (r is IAsyncDisposable a)
      await a.DisposeAsync().ConfigureAwait(false);
    else if (_current?.Resource is IDisposable d)
      d.Dispose();
  }
}