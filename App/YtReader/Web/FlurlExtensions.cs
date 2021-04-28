using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web;
using Flurl;
using Flurl.Http;
using Flurl.Http.Content;
using Flurl.Util;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;
using static SysExtensions.Net.HttpExtensions;

namespace YtReader.Web {
  public static class FlurlExtensions {
    public static Url AsUrl(this string url) => new(url);
    public static Url AsUrl(this Uri uri) => new(uri.ToString());
    public static IFlurlRequest AsRequest(this Url url) => new FlurlRequest(url);

    public static T QueryObject<T>(this Uri uri) => QueryObject<T>(uri.Query);
    public static T QueryObject<T>(this Url url) => QueryObject<T>(url.Query);
    
    static T QueryObject<T>(string queryString) {
      var dict = HttpUtility.ParseQueryString(queryString);
      var json = JsonConvert.SerializeObject(dict.Cast<string>().ToDictionary(k => k, v => dict[v]));
      return JsonConvert.DeserializeObject<T>(json);
    }

    /// <summary>Reads the content as Json (and unzip if required)</summary>
    public static Task<JObject> JsonObject(this IFlurlResponse response) =>
      response.GetStreamAsync().Then(s => JObject.LoadAsync(new JsonTextReader(new StreamReader(s)) {CloseInput = true}));

    /// <summary>Reads the content as Json (and unzip if required)</summary>
    public static Task<JArray> JsonArray(this IFlurlResponse response) =>
      response.GetStreamAsync().Then(s => JArray.LoadAsync(new JsonTextReader(new StreamReader(s)) {CloseInput = true}));

    public static Url SetParams(this Url url, object values, bool isEncoded = false) {
      if (values == null)
        return url;
      foreach (var (key, value) in values.ToKeyValuePairs())
        if (value is string s)
          url.SetQueryParam(key, s, isEncoded);
        else
          url.SetQueryParam(key, value);
      return url;
    }

    public static void EnsureSuccess(ILogger log, string desc, Url url, int? statusCode, Exception ex = null) {
      if (statusCode != null && IsSuccess(statusCode.Value)) return;
      var error = statusCode?.ToString() ?? ex?.Message ?? "unknown";
      log?.Warning(ex, "Flurl {Desc} - failed {Status}: {Url}", desc, error, url);
      throw new($"Flurl '{desc}' failed ({error})", ex);
    }

    public static void EnsureSuccess(this IFlurlResponse res, ILogger log, string desc, IFlurlRequest request, Exception ex = null, HttpMethod verb = null,
      string content = null) {
      if (res != null && IsSuccess(res.StatusCode)) return;
      var error = res?.StatusCode.ToString() ?? ex?.Message ?? "unknown";
      var curl = request.FormatCurl(verb, content);
      log?.Warning(ex, "Flurl {Desc} - failed {Status}: {Curl}", desc, error, curl);
      throw new($"Flurl '{desc}' failed ({error})", ex);
    }

    public static CapturedUrlEncodedContent FormUrlContent(this IFlurlRequest req, object data) =>
      new(req.Settings.UrlEncodedSerializer.Serialize(data));

    public static string FormatCurl(this IFlurlRequest req, HttpMethod verb = null, string content = null) {
      verb ??= HttpMethod.Get;
      var args = Array.Empty<string>()
        .Concat(req.Url.ToString(), "-X", verb.Method.ToUpper())
        .Concat(req.Headers.SelectMany(h => new[] {"-H", $"'{h.Name}:{h.Value}'"}));
      if (content != null) args = args.Concat("-d", $"'{content}'");
      var curl = $"curl {args.NotNull().Join(" ")}";
      return curl;
    }
  }
}