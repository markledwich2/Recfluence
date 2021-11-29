using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading.Tasks;
using SysExtensions.Reflection;

namespace SysExtensions.Collections; 

public static class EnumerableExtensions {
  /// <summary>Returns a collection if it is already one, or enumerates and creates one. Useful to not iterate multiple times
  ///   and still re-use collections</summary>
  public static ICollection<T> AsCollection<T>(this IEnumerable<T> items) => items as ICollection<T> ?? items.ToList();

  public static T[] InArray<T>(this T o) => new[] {o};

  /// <summary>Sames as Enumerable.Concat but makes it nicer when you just have a single item</summary>
  /// <typeparam name="T"></typeparam>
  /// <param name="items"></param>
  /// <param name="additionalItems"></param>
  /// <returns></returns>
  public static IEnumerable<T> Concat<T>(this IEnumerable<T> items, params T[] additionalItems) =>
    Enumerable.Concat(items, additionalItems);

  /// <summary>If items is null return an empty set, if an item is null remove it from the list</summary>
  [return: NotNull]
  public static IEnumerable<T> NotNull<T>(this IEnumerable<T> items)
    => items?.Where(i => !i.NullOrDefault()) ?? Array.Empty<T>();

  /// <summary>If items is null return an empty set, if an item is null remove it from the list</summary>
  public static IEnumerable<T> NotNull<T>(this IEnumerable<T?> items) where T : struct
    => items?.Where(i => i.HasValue).Select(i => i.Value) ?? Array.Empty<T>();

  public static IEnumerable<T> Randomize<T>(this IEnumerable<T> source) {
    var rnd = new Random();
    return source.OrderBy(_ => rnd.Next());
  }

  public static bool None<T>(this IEnumerable<T> items) => items?.Any() != true;

  public static IEnumerable<T> SelectMany<T>(this IEnumerable<IEnumerable<T>> items) => items.SelectMany(i => i);

  public static IEnumerable<T> WithDescendants<T>(this IEnumerable<T> items, Func<T, IEnumerable<T>> children) {
    var toRecurse = new Queue<T>(items);
    while (toRecurse.Count > 0) {
      var item = toRecurse.Dequeue();
      yield return item;
      foreach (var c in children(item)) toRecurse.Enqueue(c);
    }
  }

  public static async Task<(IReadOnlyCollection<T> included, IReadOnlyCollection<T> excluded)> Split<T>(this IAsyncEnumerable<T> items, Func<T, bool> where) {
    var included = new List<T>();
    var excluded = new List<T>();
    await foreach (var item in items)
      if (where(item))
        included.Add(item);
      else
        excluded.Add(item);
    return (included, excluded);
  }

  public static (IReadOnlyCollection<T> included, IReadOnlyCollection<T> excluded) Split<T>(this IEnumerable<T> items, Func<T, bool> where) {
    var included = new List<T>();
    var excluded = new List<T>();
    foreach (var item in items)
      if (where(item))
        included.Add(item);
      else
        excluded.Add(item);
    return (included, excluded);
  }

  /// <summary>Batches into x chunks</summary>
  public static IEnumerable<IReadOnlyCollection<T>> BatchFixed<T>(this IReadOnlyCollection<T> items, int maxBatches) =>
    items.Batch(items.Count / maxBatches);

  /// <summary>Batches items into batchsize or maxBatches batches, whatever has the least batches</summary>
  public static IEnumerable<IReadOnlyCollection<T>> Batch<T>(this IReadOnlyCollection<T> items, int batchSize, int maxBatches) =>
    items.Batch(Math.Max(items.Count / maxBatches, batchSize));

  public static IEnumerable<IReadOnlyCollection<T>> Batch<T>(this IEnumerable<T> items, int batchSize) {
    var b = new List<T>(batchSize);
    foreach (var item in items) {
      b.Add(item);
      if (b.Count != batchSize) continue;
      yield return b;
      b = new(batchSize);
    }
    if (b.Count > 0)
      yield return b;
  }

  public static IEnumerable<(T item, int index)> WithIndex<T>(this IEnumerable<T> items) => items.Select((item, index) => (item, index));

  public static IEnumerable<TResult> WithPrevious<TSource, TResult>(this IEnumerable<TSource> source, Func<TSource, TSource, TResult> map) {
    using var e = source.GetEnumerator();
    if (!e.MoveNext()) yield break;
    var previous = e.Current;
    while (e.MoveNext()) {
      yield return map(previous, e.Current);
      previous = e.Current;
    }
  }

  public static IEnumerable<IGrouping<TKey, TSource>> ChunkBy<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector) =>
    source.ChunkBy(keySelector, EqualityComparer<TKey>.Default);

  public static IEnumerable<IGrouping<TKey, TSource>> ChunkBy<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector,
    IEqualityComparer<TKey> comparer) {
    const bool noMoreSourceElements = true;
    var enumerator = source.GetEnumerator();
    if (!enumerator.MoveNext()) yield break;
    Chunk<TKey, TSource> current = null;
    while (true) {
      var key = keySelector(enumerator.Current);
      current = new(key, enumerator, value => comparer.Equals(key, keySelector(value)));
      yield return current;
      if (current.CopyAllChunkElements() == noMoreSourceElements) yield break;
    }
  }

  // from https://docs.microsoft.com/en-us/dotnet/csharp/linq/group-results-by-contiguous-keys
  // A Chunk is a contiguous group of one or more source elements that have the same key. A Chunk 
  // has a key and a list of ChunkItem objects, which are copies of the elements in the source sequence.
  class Chunk<TKey, TSource> : IGrouping<TKey, TSource> {
    readonly ChunkItem head;
    readonly object    m_Lock;

    IEnumerator<TSource> enumerator;
    internal bool        isLastSourceElement;
    Func<TSource, bool>  predicate;
    ChunkItem            tail;

    public Chunk(TKey key, IEnumerator<TSource> enumerator, Func<TSource, bool> predicate) {
      Key = key;
      this.enumerator = enumerator;
      this.predicate = predicate;
      head = new ChunkItem(enumerator.Current);
      tail = head;
      m_Lock = new object();
    }

    // Indicates that all chunk elements have been copied to the list of ChunkItems, 
    // and the source enumerator is either at the end, or else on an element with a new key.
    // the tail of the linked list is set to null in the CopyNextChunkElement method if the
    // key of the next element does not match the current chunk's key, or there are no more elements in the source.
    bool DoneCopyingChunk => tail == null;

    public TKey Key { get; }

    // Invoked by the inner foreach loop. This method stays just one step ahead
    // of the client requests. It adds the next element of the chunk only after
    // the clients requests the last element in the list so far.
    public IEnumerator<TSource> GetEnumerator() {
      //Specify the initial element to enumerate.
      var current = head;

      // There should always be at least one ChunkItem in a Chunk.
      while (current != null) {
        // Yield the current item in the list.
        yield return current.Value;

        // Copy the next item from the source sequence, 
        // if we are at the end of our local list.
        lock (m_Lock)
          if (current == tail)
            CopyNextChunkElement();

        // Move to the next ChunkItem in the list.
        current = current.Next;
      }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    // Adds one ChunkItem to the current group
    // REQUIRES: !DoneCopyingChunk && lock(this)
    void CopyNextChunkElement() {
      // Try to advance the iterator on the source sequence.
      // If MoveNext returns false we are at the end, and isLastSourceElement is set to true
      isLastSourceElement = !enumerator.MoveNext();

      // If we are (a) at the end of the source, or (b) at the end of the current chunk
      // then null out the enumerator and predicate for reuse with the next chunk.
      if (isLastSourceElement || !predicate(enumerator.Current)) {
        enumerator = null;
        predicate = null;
      }
      else {
        tail.Next = new ChunkItem(enumerator.Current);
      }

      // tail will be null if we are at the end of the chunk elements
      // This check is made in DoneCopyingChunk.
      tail = tail.Next;
    }

    // Called after the end of the last chunk was reached. It first checks whether
    // there are more elements in the source sequence. If there are, it 
    // Returns true if enumerator for this chunk was exhausted.
    internal bool CopyAllChunkElements() {
      while (true)
        lock (m_Lock)
          if (DoneCopyingChunk) // If isLastSourceElement is false,
            // it signals to the outer iterator
            // to continue iterating.
            return isLastSourceElement;
          else
            CopyNextChunkElement();
    }

    class ChunkItem {
      public readonly TSource   Value;
      public          ChunkItem Next;
      public ChunkItem(TSource value) => Value = value;
    }
  }
}