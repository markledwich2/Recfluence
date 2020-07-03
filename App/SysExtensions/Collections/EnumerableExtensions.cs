using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace SysExtensions.Collections {
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
      => items?.Where(i => i != null) ?? new T[] { };

    public static IEnumerable<T> Randomize<T>(this IEnumerable<T> source) {
      var rnd = new Random();
      return source.OrderBy(item => rnd.Next());
    }

    public static bool None<T>(this IEnumerable<T> items) => items?.Any() != true;

    public static IEnumerable<T> SelectMany<T>(this IEnumerable<IEnumerable<T>> items) => items.SelectMany(i => i);

    public static async IAsyncEnumerable<T> SelectMany<T>(this IAsyncEnumerable<IEnumerable<T>> items) {
      await foreach (var g in items)
      foreach (var i in g) 
        yield return i;
    }

    public static IEnumerable<T> WithDescendants<T>(this IEnumerable<T> items, Func<T, IEnumerable<T>> children) {
      var toRecurse = new Queue<T>(items);
      while (toRecurse.Count > 0) {
        var item = toRecurse.Dequeue();
        yield return item;
        foreach (var c in children(item)) toRecurse.Enqueue(c);
      }
    }

    /// <summary>Batches items into batchsize or maxBatches batches, whatever has the last batches</summary>
    public static IEnumerable<IReadOnlyCollection<T>> Batch<T>(this IReadOnlyCollection<T> items, int batchSize, int maxBatches) =>
      items.Batch(Math.Max(items.Count / maxBatches, batchSize));

    public static IEnumerable<IReadOnlyCollection<T>> Batch<T>(this IEnumerable<T> items, int batchSize) {
      var b = new List<T>(batchSize);
      foreach (var item in items) {
        b.Add(item);
        if (b.Count != batchSize) continue;
        yield return b;
        b = new List<T>(batchSize);
      }
      if (b.Count > 0)
        yield return b;
    }

    public static IEnumerable<(T item, int index)> WithIndex<T>(this IEnumerable<T> items) => items.Select((item, index) => (item, index));

    public static IEnumerable<IGrouping<TKey, TSource>> ChunkBy<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector) =>
      source.ChunkBy(keySelector, EqualityComparer<TKey>.Default);

    public static IEnumerable<IGrouping<TKey, TSource>> ChunkBy<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector,
      IEqualityComparer<TKey> comparer) {
      // Flag to signal end of source sequence.
      const bool noMoreSourceElements = true;

      // Auto-generated iterator for the source array.       
      var enumerator = source.GetEnumerator();

      // Move to the first element in the source sequence.
      if (!enumerator.MoveNext()) yield break;

      // Iterate through source sequence and create a copy of each Chunk.
      // On each pass, the iterator advances to the first element of the next "Chunk"
      // in the source sequence. This loop corresponds to the outer foreach loop that
      // executes the query.
      Chunk<TKey, TSource> current = null;
      while (true) {
        // Get the key for the current Chunk. The source iterator will churn through
        // the source sequence until it finds an element with a key that doesn't match.
        var key = keySelector(enumerator.Current);

        // Make a new Chunk (group) object that initially has one GroupItem, which is a copy of the current source element.
        current = new Chunk<TKey, TSource>(key, enumerator, value => comparer.Equals(key, keySelector(value)));

        // Return the Chunk. A Chunk is an IGrouping<TKey,TSource>, which is the return value of the ChunkBy method.
        // At this point the Chunk only has the first element in its source sequence. The remaining elements will be
        // returned only when the client code foreach's over this chunk. See Chunk.GetEnumerator for more info.
        yield return current;

        // Check to see whether (a) the chunk has made a copy of all its source elements or 
        // (b) the iterator has reached the end of the source sequence. If the caller uses an inner
        // foreach loop to iterate the chunk items, and that loop ran to completion,
        // then the Chunk.GetEnumerator method will already have made
        // copies of all chunk items before we get here. If the Chunk.GetEnumerator loop did not
        // enumerate all elements in the chunk, we need to do it here to avoid corrupting the iterator
        // for clients that may be calling us on a separate thread.
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
}