using System;
using Humanizer;
using Troschuetz.Random;

namespace SysExtensions.Net {
  public static class RetryPolicy {
    const double DeviationPercent = 0.2;
    static readonly TRandom _rand = new TRandom();
    static readonly TimeSpan MinWait = 10.Milliseconds();

    public static TimeSpan ExponentialBackoff(int attempt, TimeSpan? firstWait = null) {
      var firstWaitValue = firstWait ?? 200.Milliseconds();
      var waitValue = firstWaitValue.TotalMilliseconds * Math.Pow(2, attempt - 1);
      var waitWithRandomness = _rand.Normal(waitValue, waitValue * DeviationPercent).Milliseconds();
      if (waitWithRandomness < MinWait) waitWithRandomness = MinWait;
      return waitWithRandomness;
    }
  }
}