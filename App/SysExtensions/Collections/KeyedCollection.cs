using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;
using SysExtensions.Serialization;

namespace SysExtensions.Collections {
  public interface IKeyedCollection<K, V> : ICollection<V> {
    void SetKey(V item, K value);
    V GetOrAdd(K k, Func<V> create);
    Task<V> GetOrAdd(K k, Func<Task<V>> create);
    V this[K key] { get; }
    bool ContainsKey(K key);
    K GetKey(V item);
  }

  /// <summary>Wraps a dictionary for items that can provide a way to key items. More clean and collection compatible usage
  ///   compared to dictionary</summary>
  public class KeyedCollection<K, V> : IKeyedCollection<K, V> {
    readonly Expression<Func<V, K>> _getKeyExpression;
    readonly IDictionary<K, V>      _dic;

    public IDictionary<K, V> ToDictionary() => _dic.ToDictionary(t => t.Key, t => t.Value);

    public KeyedCollection(Expression<Func<V, K>> getKeyExpression, IEqualityComparer<K> comparer = null, bool theadSafe = false) {
      _getKeyExpression = getKeyExpression;
      if (theadSafe)
        _dic = comparer == null ? new() : new ConcurrentDictionary<K, V>(comparer);
      else
        _dic = comparer == null ? new() : new Dictionary<K, V>(comparer);
      //_dic = comparer == null ? new Dictionary<K, V>() : new Dictionary<K, V>(comparer);
    }

    public KeyedCollection(Expression<Func<V, K>> getKey, IEnumerable<V> list, IEqualityComparer<K> comparer = null, bool theadSafe = false)
      : this(getKey, comparer, theadSafe) {
      foreach (var item in list)
        Add(item);
    }

    public V this[K key] => _dic.TryGet(key);

    public void Add(V item) {
      var key = GetKey(item); // it won't be accessible via the key. But we can still hold it in the collection
      _dic[key] = item;
    }

    public V GetOrAdd(K k, Func<V> create) => _dic.GetOrAdd(k, create);
    public Task<V> GetOrAdd(K k, Func<Task<V>> create) => _dic.GetOrAdd(k, create);

    /// <summary>Like add, but also returns the item so it can be chained</summary>
    public V AddItem(V item) {
      Add(item);
      return item;
    }

    public bool ContainsKey(K key) => _dic.ContainsKey(key);

    Func<V, K> _getKeyFuc;

    public K GetKey(V item) {
      if (_getKeyFuc == null)
        _getKeyFuc = _getKeyExpression.Compile();
      return _getKeyFuc(item);
    }

    Action<V, K> _setKeyAction;

    public void SetKey(V item, K value) {
      if (_setKeyAction == null)
        _setKeyAction = KeyedCollectionExtensions.GetSetter(_getKeyExpression);
      _setKeyAction(item, value);
    }

    public IEnumerable<K> Keys => _dic.Keys;

    #region ICollection

    public void Clear() => _dic.Clear();

    /// <summary>Use key comparison rather than equality for performance and predictability</summary>
    public bool Contains(V item) => ContainsKey(GetKey(item));

    public void CopyTo(V[] array, int startIndex) => _dic.Values.CopyTo(array, startIndex);

    public void CopyTo(Array array, int index) => Array.Copy(_dic.Values.ToArray(), array, index);

    public bool Remove(V item) => _dic.Remove(GetKey(item));

    public int Count => _dic.Count;

    public bool IsReadOnly => _dic.IsReadOnly;

    public bool   IsSynchronized => false;
    public object SyncRoot       => null;

    public IEnumerator<V> GetEnumerator() => _dic.Values.GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    #endregion
  }

  public class MultiValueKeyedCollection<K, V> : ICollection<V> {
    readonly MultiValueDictionary<K, V> _dic;
    readonly Func<V, K>                 _getKey;

    public MultiValueKeyedCollection(Func<V, K> getKey, IEqualityComparer<K> comparer = null) {
      _dic = comparer == null ? new() : new MultiValueDictionary<K, V>(comparer);
      _getKey = getKey;
    }

    public MultiValueKeyedCollection(Func<V, K> getKey, IEnumerable<V> list, IEqualityComparer<K> comparer = null)
      : this(getKey, comparer) {
      foreach (var item in list)
        Add(item);
    }

    public IReadOnlyCollection<V> this[K key] => _dic.TryGet(key);

    public IEnumerable<K> Keys => _dic.Keys;

    public IEnumerable<KeyValuePair<K, IReadOnlyCollection<V>>> KeyValues => _dic;

    public void Add(V item) {
      var key = _getKey(item);
      _dic.Add(key, item);
    }

    public bool ContainsKey(K key) => _dic.ContainsKey(key);

    public void Remove(K key) => _dic.Remove(key);

    #region ICollection

    public void Clear() => _dic.Clear();

    public bool Contains(V item) => _dic.ContainsValue(item);

    public void CopyTo(V[] array, int startIndex) => _dic.Values.SelectMany(v => v).ToArray().CopyTo(array, startIndex);

    public bool Remove(V item) => _dic.Remove(_getKey(item));

    public int Count => _dic.Count;

    public bool IsReadOnly => false;

    public IEnumerator<V> GetEnumerator() => _dic.Values.SelectMany(v => v).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    #endregion
  }

  public static class KeyedCollectionExtensions {
    /// <summary>Constructs a dictionary where items key is used for the dictionary and null in each item</summary>
    public static IDictionary<K, V> ToDictionaryForSerialization<K, V>(this IKeyedCollection<K, V> list) {
      var dic = list.ToList().JsonClone().ToDictionary(list.GetKey);
      foreach (var kv in dic.Values)
        list.SetKey(kv, default);
      return dic;
    }

    /// <summary>Initializes the collection from a dictionary. Clones of items are created and their key properties set
    ///   according to the dictionary</summary>
    public static void FromDictionaryForSerialization<K, V>(this IKeyedCollection<K, V> collection, IDictionary<K, V> dic) {
      foreach (var kv in dic) {
        var val = kv.Value.JsonClone();
        collection.SetKey(val, kv.Key);
        collection.Add(val);
      }
    }

    /// <summary>Convert a lambda expression for a getter into a setter</summary>
    public static Action<T, TProperty> GetSetter<T, TProperty>(Expression<Func<T, TProperty>> expression) {
      var memberExpression = (MemberExpression) expression.Body;
      var property = (PropertyInfo) memberExpression.Member;
      var setMethod = property.GetSetMethod();

      var parameterT = Expression.Parameter(typeof(T), "x");
      var parameterTProperty = Expression.Parameter(typeof(TProperty), "y");

      var newExpression =
        Expression.Lambda<Action<T, TProperty>>(
          Expression.Call(parameterT, setMethod, parameterTProperty),
          parameterT,
          parameterTProperty
        );

      return newExpression.Compile();
    }
  }
}