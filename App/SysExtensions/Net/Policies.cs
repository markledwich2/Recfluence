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
    
    public static AsyncRetryPolicy RetryWithBackoff(this PolicyBuilder policy, string description, int retryCount = 3, ILogger log = null) =>
      policy.RetryAsync(retryCount, async (e, i) => {
        var delay = i.ExponentialBackoff(1.Seconds());
        log?.Debug("retryable error with {Description}: '{Error}'. Retrying in {Duration}, attempt {Attempt}/{Total}", 
          description, e.Message, delay, i, retryCount);
        await Task.Delay(delay);
      });
  }
}