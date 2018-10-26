using System;

namespace SysExtensions.Text
{
    public static class HumanizeExtensions
    {
        public static Speed Speed(this double amount, string unit, TimeSpan duration) => new Speed { Amount = amount, Unit = unit, Duration = duration };

        public static Speed Speed(this int amount, string unit, TimeSpan duration) => new Speed { Amount = amount, Unit = unit, Duration = duration };

        public static Speed Speed(this long amount, string unit, TimeSpan duration) => new Speed { Amount = amount, Unit = unit, Duration = duration };

        public static string Humanize(this Speed speed, string format = "#.#") => speed.Amount == 0 || speed.Duration.TotalSeconds == 0 ?
            $"0 {speed.Unit}/s" : $"{speed.AmountPerSecond.ToMetric(format)} {speed.Unit}/s";
    }

    public class Speed
    {
        public string Unit { get; set; }
        public TimeSpan Duration { get; set; }
        public double Amount { get; set; }

        public double AmountPerSecond => Amount / Duration.TotalSeconds;


        public override string ToString() => this.Humanize();
    }
}
