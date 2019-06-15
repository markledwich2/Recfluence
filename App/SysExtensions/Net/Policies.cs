using System;
using System.Net;
using System.Net.Http;
using Humanizer;
using Polly;
using Polly.Retry;
using Troschuetz.Random;

namespace SysExtensions.Net {
  public static class Policies {
    const double DeviationPercent = 0.2;
    static readonly TRandom _rand = new TRandom();
    static readonly TimeSpan MinWait = 10.Milliseconds();

    public static TimeSpan ExponentialBackoff(this int attempt, TimeSpan? firstWait = null) {
      var firstWaitValue = firstWait ?? 200.Milliseconds();
      var waitValue = firstWaitValue.TotalMilliseconds * Math.Pow(2, attempt - 1);
      var waitWithRandomness = _rand.Normal(waitValue, waitValue * DeviationPercent).Milliseconds();
      if (waitWithRandomness < MinWait) waitWithRandomness = MinWait;
      return waitWithRandomness;
    }
    
    public static bool IsTransient(this HttpStatusCode code) {
      if (code < HttpStatusCode.InternalServerError)
        return code == HttpStatusCode.RequestTimeout;
      return true;
    }
    
    public static AsyncRetryPolicy<T> RetryWithBackoff<T>(this PolicyBuilder<T> policy, int retryCount = 3) =>
      policy.RetryAsync(retryCount, (r, i) => i.ExponentialBackoff(1.Seconds()));
    
    public static AsyncRetryPolicy RetryWithBackoff(this PolicyBuilder policy, int retryCount = 3) =>
      policy.RetryAsync(retryCount, (e, i) => i.ExponentialBackoff(1.Seconds()));
  }
}