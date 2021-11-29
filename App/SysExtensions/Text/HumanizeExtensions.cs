using System;
using System.Diagnostics;
using System.Linq;
using Humanizer.Localisation;
using static Humanizer.Localisation.TimeUnit;

namespace SysExtensions.Text; 

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

  public static string HumanizeShort(this Stopwatch sw) => sw.Elapsed.HumanizeShort();

  public static string HumanizeShort(this TimeSpan t) {
    var units = new (int v, string s, TimeUnit u)[]
      {(t.Days, "d", Day), (t.Hours, "h", Hour), (t.Minutes, "m", Minute), (t.Seconds, "s", Second), (t.Milliseconds, "ms", Millisecond)};
    var res = units
      .SkipWhile(s => s.v == 0)
      .Take(2).ToArray();

    var time = (
      a: res.Length > 0 ? res[0] : default,
      b: res.Length > 1 ? res[1] : default
    );
    static string Format((int v, string s, TimeUnit u) time) => $"{time.v}{time.s}";

    return time switch {
      (a: (_, _, Second), _) => $"{t.TotalSeconds:0.##}s", // special case for seconds, because its shorter to use decimals
      (a: (0, _, _), (0, _, _)) => "0s",
      _ => res.Join(" ", Format)
    };
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