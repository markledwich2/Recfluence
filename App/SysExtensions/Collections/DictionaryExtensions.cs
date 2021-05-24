using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SysExtensions.Collections {
  public static class DictionaryExtensions {
    public static MultiValueDictionary<TKey, TValue> ToMultiValueDictionary<TKey, TValue, T>(this IEnumerable<T> items,
      Func<T, TKey> keySelector,
      Func<T, TValue> valueSelector = null) {
      var dic = new MultiValueDictionary<TKey, TValue>();
      foreach (var item in items)
        dic.Add(keySelector(item), valueSelector(item));
      return dic;
    }

    public static MultiValueDictionary<TKey, T> ToMultiValueDictionary<TKey, T>(this IEnumerable<T> items, Func<T, TKey> keySelector) =>
      items.ToMultiValueDictionary(keySelector, v => v);

    public static MultiValueDictionary<TKey, T> ToMultiValueDictionary<TKey, T>(this IEnumerable<(TKey, T)> items) =>
      items.ToMultiValueDictionary(i => i.Item1, i => i.Item2);

    public static TValue TryGet<TKey, TValue>(this IDictionary<TKey, TValue> dictionary, TKey key, TValue defaultValue = default) =>
      dictionary.TryGetValue(key, out var value) ? value : defaultValue;

    public static IReadOnlyCollection<TValue> TryGet<TKey, TValue>(this MultiValueDictionary<TKey, TValue> dictionary, TKey key) {
      IReadOnlyCollection<TValue> value;
      return dictionary.TryGetValue(key, out value) ? value : new TValue[] { };
    }

    public static async Task<TValue>
      GetOrAdd<Tkey, TValue>(this IDictionary<Tkey, TValue> dictionary, Tkey key, Func<Task<TValue>> create) {
      if (dictionary.TryGetValue(key, out var value))
        return value;

      value = await create();
      dictionary.Add(key, value);
      return value;
    }

    public static TValue GetOrAdd<Tkey, TValue>(this IDictionary<Tkey, TValue> dictionary, Tkey key, Func<TValue> create) {
      if (dictionary.TryGetValue(key, out var value))
        return value;

      value = create();
      dictionary.Add(key, value);
      return value;
    }

    public static IDictionary<TKey, TValue> ToDictionary<TKey, TValue>(this IEnumerable<ValueTuple<TKey, TValue>> items) =>
      items.ToDictionary(i => i.Item1, i => i.Item2);

    /// <summary>De-duplicates keys by taking first.</summary>
    public static IDictionary<TKey, TValue> ToDictionarySafe<T, TKey, TValue>(this IEnumerable<T> items, Func<T, TKey> keySelector,
      Func<T, TValue> valueSelector) =>
      items.GroupBy(keySelector).ToDictionary(g => g.Key, g => valueSelector(g.First()));

    /// <summary>De-duplicates keys by taking first.</summary>
    public static IDictionary<TKey, T> ToDictionarySafe<T, TKey>(this IEnumerable<T> items, Func<T, TKey> keySelector) =>
      items.GroupBy(keySelector).ToDictionary(g => g.Key, g => g.First());
  }
}