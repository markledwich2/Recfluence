using System.Collections.Concurrent;
using System.Collections.Specialized;
using System.Linq.Expressions;

namespace SysExtensions.Collections;

public static class CollectionExtensions {
  public static void AddRange<T>(this ICollection<T> list, IEnumerable<T> items) {
    foreach (var item in items)
      list.Add(item);
  }

  public static void AddRange<T>(this IProducerConsumerCollection<T> list, IEnumerable<T> items) {
    foreach (var item in items)
      list.TryAdd(item);
  }

  public static void AddRange<T>(this ICollection<T> list, params T[] items) => list.AddRange((IEnumerable<T>)items);

  public static void AddRange<TKey, TValue>(this IDictionary<TKey, TValue> dic, IEnumerable<ValueTuple<TKey, TValue>> values) {
    foreach (var tuple in values)
      dic.Add(tuple.Item1, tuple.Item2);
  }

  public static IEnumerable<T> CommonSequence<T>(this IEnumerable<T> items, IEnumerable<T> sequence) {
    using (var sequenceEnumerator = sequence.GetEnumerator())
      foreach (var item in items) {
        if (!sequenceEnumerator.MoveNext())
          break;

        if (item.Equals(sequenceEnumerator.Current))
          yield return item;
        else
          break;
      }
  }

  public static bool HasItems<T>(this IReadOnlyCollection<T> list) => list != null && list.Count > 0;

  public static void Init<T>(this ICollection<T> list, params T[] items) {
    list.Clear();
    list.AddRange(items);
  }

  public static void Init<T>(this ICollection<T> list, IEnumerable<T> items) {
    list.Clear();
    list.AddRange(items);
  }

  public static bool IsEmpty<T>(this ICollection<T> list) => list == null || list.Count == 0;

  public static IKeyedCollection<K, V> KeyBy<K, V>(this IEnumerable<V> list, Expression<Func<V, K>> getKey,
    IEqualityComparer<K> comparer = null, bool threadSafe = false) =>
    new KeyedCollection<K, V>(getKey, list, comparer, threadSafe);

  public static IKeyedCollection<K, V> KeyBy<K, V, U>(this IEnumerable<U> list, Func<U, V> getValue,
    Expression<Func<V, K>> getKey) => new KeyedCollection<K, V>(getKey, list.Select(getValue));

  public static IReadOnlyCollection<T> AsReadOnly<T>(this IEnumerable<T> items) => items switch {
    IReadOnlyCollection<T> ro => ro,
    _ => items.ToArray()
  };

  public static IEnumerable<(string Name, string Value)> ToTuples(this NameValueCollection items) =>
    from key in items.Cast<string>()
    from value in items.GetValues(key)
    select (key, value);
}