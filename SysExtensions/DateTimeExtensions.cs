using System;

namespace SysExtensions {
    public static class DateTimeExtensions {
        public static string FileSafeTimestamp(this DateTime value) { return value.ToString("yyyy-MM-dd_HH-mm-ss-ffff"); }

        public static DateTime Epoc { get; } = new DateTime(1970, 1, 1);
    }
}