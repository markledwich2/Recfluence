using System.Runtime.CompilerServices;
using SysExtensions.Reflection;

namespace SysExtensions.Collections;

public static class AsyncEnumerableExtensions {
  public static async IAsyncEnumerable<T> SelectMany<T>(this IAsyncEnumerable<IEnumerable<T>> items) {
    await foreach (var g in items)
    foreach (var i in g)
      yield return i;
  }

  /// <summary>Like TakeWhile, but will include the first element that doesn't meet the predicate</summary>
  public static async IAsyncEnumerable<TSource> TakeWhileInclusive<TSource>(this IAsyncEnumerable<TSource> source, Func<TSource, bool> predicate
    , [EnumeratorCancellation] CancellationToken cancel = default) {
    await foreach (var e in source.WithCancellation(cancel).ConfigureAwait(false)) {
      yield return e;
      if (!predicate(e))
        break;
    }
  }

  public static IAsyncEnumerable<T> NotNull<T>(this IAsyncEnumerable<T> items) => items.Where(i => !i.NullOrDefault());

  /// <summary>Batch into size chunks lazily</summary>
  public static async IAsyncEnumerable<T[]> Batch<T>(this IAsyncEnumerable<T> items, int size) {
    var batch = new List<T>();
    await foreach (var item in items) {
      batch.Add(item);
      if (batch.Count < size) continue;
      yield return batch.ToArray();
      batch.Clear();
    }
    if (batch.Count > 0)
      yield return batch.ToArray();
  }

  public static IAsyncEnumerable<(T item, int index)> WithIndex<T>(this IAsyncEnumerable<T> items) => items.Select((item, index) => (item, index));
}