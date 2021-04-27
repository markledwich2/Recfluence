using System;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using Humanizer;
using Polly;
using Polly.Retry;
using Serilog;
using Troschuetz.Random;

namespace SysExtensions.Net {
  public static class Policies {
    const           double   DeviationPercent = 0.2;
    static readonly TRandom  _rand            = new TRandom();
    static readonly TimeSpan MinWait          = 10.Milliseconds();
    static readonly TimeSpan MaxWait          = 1.Minutes();

    public static TimeSpan ExponentialBackoff(this int attempt, TimeSpan? firstWait = null) {
      var firstWaitValue = firstWait ?? MinWait;
      var waitValue = firstWaitValue.TotalMilliseconds * Math.Pow(2, attempt - 1);
      var waitWithRandomness = _rand.Normal(waitValue, waitValue * DeviationPercent).Milliseconds();
      if (waitWithRandomness < MinWait) waitWithRandomness = MinWait;
      if (waitWithRandomness > MaxWait) waitWithRandomness = MaxWait;
      return waitWithRandomness;
    }


    public static AsyncRetryPolicy<T> RetryWithBackoff<T>(this PolicyBuilder<T> policy, string description, int retryCount = 3, 
      Action<DelegateResult<T>, int, TimeSpan> onError = null,
      ILogger log = null) =>
      policy.RetryAsync(retryCount, async (e, i, c) => {
        var delay = i.ExponentialBackoff(1.Seconds());
        if (onError == null)
          log?.Debug("retryable error with {Description}: '{Error}'. Retrying in {Duration}, attempt {Attempt}/{Total}",
            description, e.Exception?.Message ?? "Unknown error", delay, i, retryCount);
        else
          onError(e, i, delay);
        await Task.Delay(delay);
      });

    public static AsyncRetryPolicy RetryBackoff(this PolicyBuilder policy, string description, int retryCount = 3, ILogger log = null) =>
      policy.RetryAsync(retryCount, async (e, i) => {
        var delay = i.ExponentialBackoff(1.Seconds());
        log?.Debug("retryable error with {Description}: '{Error}'. Retrying in {Duration}, attempt {Attempt}/{Total}",
          description, e.Message, delay, i, retryCount);
        await Task.Delay(delay);
      });
  }
}