using System;
using System.Collections.Generic;
using System.Linq;

namespace SysExtensions {
  public static class NumberExtensions {
    public static ulong RoundToULong(this double value) => (ulong) Math.Round(value);
    public static long RoundToLong(this double value) => (long) Math.Round(value);
    public static int RoundToInt(this double value) => (int) Math.Round(value);
    public static double Pow(this int x, int y) => Math.Pow(x, y);
    public static IEnumerable<int> RangeTo(this int from, int to) => Enumerable.Range(from, to);
    public static int Abs(this int num) => Math.Abs(num);
    public static bool Between(this int num, int from, int to) => num >= from && num <= to;
  }
}