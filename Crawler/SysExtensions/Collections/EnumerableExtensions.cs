using System;
using System.Collections.Generic;
using System.Linq;

namespace SysExtensions.Collections
{
    public static class EnumerableExtensions
    {
        public static T[] AsArray<T>(this T o) => new[] { o };

        /// <summary>
        ///     Returns a collection if it is already one, or enumerates and creates one. Useful to not iterate multiple times
        ///     and still re-use collections
        /// </summary>
        public static ICollection<T> AsCollection<T>(this IEnumerable<T> items) => items as ICollection<T> ?? items.ToList();

        public static IEnumerable<T> AsEnumerable<T>(this T o) => new[] { o };

        /// <summary>
        ///     Sames as Enumerable.Concat but makes it nicer when you just have a single item
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="items"></param>
        /// <param name="additionalItems"></param>
        /// <returns></returns>
        public static IEnumerable<T> Concat<T>(this IEnumerable<T> items, params T[] additionalItems) => Enumerable.Concat(items, additionalItems);

        public static IEnumerable<T> NotNull<T>(this IEnumerable<T> items) => items.Where(i => i != null);

        public static ulong Sum<T>(this IEnumerable<T> items, Func<T, ulong> f) => items.Aggregate(0UL, (a, i) => a + f(i));

        /// <summary>
        /// Given a list of items, returns the value of the given percentile
        /// </summary>
        public static double Percentile<T>(this IEnumerable<T> items, Func<T, double> f,  double percentile) {
            var s = items.Select(f).OrderBy(i => i).ToArray();
            var len = s.Length;
            var n = (len - 1) * percentile + 1;

            if (n <= 1d) return s[0];
            if (n >= len) return s[len - 1];

            var k = (int)n;
            var d = n - k;
            return s[k - 1] + d * (s[k] - s[k - 1]);
        }

        public static IEnumerable<int> For(this int count) => Enumerable.Range(0, count);

        public static IEnumerable<int> To(this int from, int to) => from < to
            ? Enumerable.Range(from, to - from + 1)
            : Enumerable.Range(to, from - to + 1).Reverse();

        public static IEnumerable<double> To(this double from, double to, double step)
        {
            for (var d = from; d < to; d += step)
                yield return d;
        }


        public static IEnumerable<IEnumerable<TValue>> Chunk<TValue>(this IEnumerable<TValue> values, int chunkSize)
        {
            using (var enumerator = values.GetEnumerator()) {
                while (enumerator.MoveNext())
                    yield return GetChunk(enumerator, chunkSize).ToList();
            }
        }

        static IEnumerable<T> GetChunk<T>(IEnumerator<T> enumerator, int chunkSize)
        {
            do
                yield return enumerator.Current;
            while (--chunkSize > 0 && enumerator.MoveNext());
        }
    }
}