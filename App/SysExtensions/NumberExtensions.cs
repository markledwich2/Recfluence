using System;

namespace SysExtensions {
    public static class NumberExtensions {
        public static int RoundToInt(this double value) { return (int) Math.Round(value); }

        public static double Pow(this int x, int y) => Math.Pow(x, y);
    }
}