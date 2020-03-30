﻿using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;

namespace SysExtensions.Collections {
  public static class EnumerableExtensions {

    /// <summary>
    ///   Returns a collection if it is already one, or enumerates and creates one. Useful to not iterate multiple times
    ///   and still re-use collections
    /// </summary>
    public static ICollection<T> AsCollection<T>(this IEnumerable<T> items) => items as ICollection<T> ?? items.ToList();

    public static IEnumerable<T> AsEnumerable<T>(this T o) => new[] {o};

    /// <summary>
    ///   Sames as Enumerable.Concat but makes it nicer when you just have a single item
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="items"></param>
    /// <param name="additionalItems"></param>
    /// <returns></returns>
    public static IEnumerable<T> Concat<T>(this IEnumerable<T> items, params T[] additionalItems) =>
      Enumerable.Concat(items, additionalItems);

    /// <summary>
    ///   If items is null return an empty set, if an item is null remove it from the list
    /// </summary>
    [return: NotNull]
    public static IEnumerable<T> NotNull<T>(this IEnumerable<T> items) 
      => items?.Where(i => i != null) ?? new T[] { };
    
    public static IEnumerable<T> Randomize<T>(this IEnumerable<T> source) {
      var rnd = new Random();
      return source.OrderBy(item => rnd.Next());
    }

    public static bool None<T>(this IEnumerable<T> items) => !items.Any();
    
    public static IEnumerable<T> SelectMany<T>(this IEnumerable<IEnumerable<T>> items) => items.SelectMany(i => i);
    
    public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> source, int size) {
      using IEnumerator<T> enumerator = source.GetEnumerator();
      while (enumerator.MoveNext())
        yield return TakeIEnumerator(enumerator, size);
    }
    
    public static IEnumerable<IEnumerable<T>> Batch<T>(this IEnumerable<T> source, int size, int maxBatches) {
      var items = source as IReadOnlyCollection<T> ?? source.ToArray();
      return items.Batch(Math.Max(items.Count / maxBatches, size));
    }

    static IEnumerable<T> TakeIEnumerator<T>(IEnumerator<T> source, int size) {
      var i = 0;
      do
        yield return source.Current;
      while (++i < size && source.MoveNext());
    }
  }
}