using System;

namespace SysExtensions.Text {
  public static class HumanizeExtensions {
    public static Speed Speed(this double amount, string unit, TimeSpan duration) =>
      new Speed {Amount = amount, Unit = unit, Duration = duration};

    public static Speed Speed(this int amount, string unit, TimeSpan duration) =>
      new Speed {Amount = amount, Unit = unit, Duration = duration};

    public static Speed Speed(this long amount, string unit, TimeSpan duration) =>
      new Speed {Amount = amount, Unit = unit, Duration = duration};

    public static string Humanize(this Speed speed, string format = "#.#") {
      if (speed.Amount <= 0 || speed.Duration.TotalSeconds <= 0) return $"0 {speed.Unit}/s";
      var timeUnit = speed.AmountPerSecond > 1 ? TimeUnits.Seconds : TimeUnits.Minutes;
      switch (timeUnit) {
        case TimeUnits.Minutes:
          return $"{speed.AmountPerMinute.ToMetric(format)} {speed.Unit}/min";
        case TimeUnits.Seconds:
          return $"{speed.AmountPerSecond.ToMetric(format)} {speed.Unit}/s";
        default:
          throw new ArgumentOutOfRangeException();
      }
    }
  }

  public class Speed {
    public string   Unit     { get; set; }
    public TimeSpan Duration { get; set; }
    public double   Amount   { get; set; }

    public double AmountPerSecond => Amount / Duration.TotalSeconds;
    public double AmountPerMinute => Amount / Duration.TotalMinutes;

    public override string ToString() => this.Humanize();
  }

  enum TimeUnits {
    Seconds,
    Minutes
  }
}