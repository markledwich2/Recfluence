using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;
using Humanizer;
using Microsoft.Net.Http.Headers;
using Newtonsoft.Json;
using Polly;
using Serilog;
using SysExtensions.Security;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Collections;

namespace SysExtensions.Net {
  public static class HttpClientExtensions {
    public static HttpRequestMessage Post(this Uri uri) => new(HttpMethod.Post, uri.ToString());
    public static HttpRequestMessage Get(this Uri uri) => new(HttpMethod.Get, uri.ToString());
    public static HttpRequestMessage Put(this Uri uri) => new(HttpMethod.Put, uri.ToString());
    public static HttpRequestMessage Delete(this Uri uri) => new(HttpMethod.Delete, uri.ToString());
    public static HttpRequestMessage Post(this UriBuilder uri) => new(HttpMethod.Post, uri.ToString());
    public static HttpRequestMessage Get(this UriBuilder uri) => new(HttpMethod.Get, uri.ToString());
    public static HttpRequestMessage Put(this UriBuilder uri) => new(HttpMethod.Put, uri.ToString());
    public static HttpRequestMessage Delete(this UriBuilder uri) => new(HttpMethod.Delete, uri.ToString());
    public static HttpRequestMessage Request(this UriBuilder uri, HttpMethod method) => new(method, uri.ToString());

    public static HttpClient AcceptJson(this HttpClient client) => client.Accept("application/json");

    public static HttpClient Accept(this HttpClient client, string mediaType) {
      client.DefaultRequestHeaders.Accept.Add(new(mediaType));
      return client;
    }

    public static HttpRequestMessage AddHeader(this HttpRequestMessage request, string name, string value) {
      request.Headers.Add(name, value);
      return request;
    }

    public static HttpRequestMessage AcceptJson(this HttpRequestMessage request) => request.Accept("application/json");

    public static HttpRequestMessage Accept(this HttpRequestMessage request, string mediaType) {
      request.Headers.Accept.Add(new(mediaType));
      return request;
    }

    public static HttpClient BasicAuth(this HttpClient client, NameSecret credentials) {
      client.DefaultRequestHeaders.Authorization = Authorization(credentials);
      return client;
    }

    public static HttpRequestMessage BasicAuth(this HttpRequestMessage request, NameSecret credentials) {
      request.Headers.Authorization = Authorization(credentials);
      return request;
    }

    static AuthenticationHeaderValue Authorization(NameSecret credentials) =>
      new("Basic", Convert.ToBase64String(Encoding.ASCII.GetBytes($"{credentials.Name}:{credentials.Secret}")));

    public static HttpRequestMessage Auth(this HttpRequestMessage request, string scheme, string value) {
      request.Headers.Authorization = new(scheme, value);
      return request;
    }

    public static HttpClient BaseUrl(this HttpClient client, Uri baseUrl) {
      if (!baseUrl.OriginalString.EndsWith("/"))
        baseUrl = new(baseUrl.OriginalString + "/");
      client.BaseAddress = baseUrl;
      return client;
    }
    
    public static string FormatCurl(this  HttpWebRequest req, HttpMethod verb = null, string content = null) {
      verb ??= HttpMethod.Get;
      var args = Array.Empty<string>()
        .Concat(req.RequestUri.ToString(), "-X", verb.Method.ToUpper())
        .Concat(req.Headers.ToTuples().SelectMany(h => new[] {"-H", $"'{h.Name}:{h.Value}'"}));

      var cookies = req.CookieContainer?.GetCookieHeader(req.Address);
      if (cookies.HasValue())
        args = args.Concat($"--cookie {cookies}");
      
      if (content != null) args = args.Concat("-d", $"'{content}'");
      var curl = $"curl {args.NotNull().Join(" ")}";
      return curl;
    }

    public static string FormatAsCurl(this HttpRequestMessage request, string content = null) {
      var curl = $"curl -X {request.Method.ToString().ToUpper()} "
                 // headers can have mutliple values. Spec is to repeat them
                 + request.Headers.Concat(request.Content?.Headers.ToArray() ?? new KeyValuePair<string, IEnumerable<string>>[] { })
                   .SelectMany(h => h.Value, (h, v) => new {Name = h.Key, Value = v})
                   .Join("", h => $" -H \"{h.Name}:{h.Value}\"");

      if (content != null)
        curl += $" -d '{content.Replace("'", @"\u0027")}'";

      curl += $" \"{request.RequestUri}\"";

      return curl;
    }

    public static string FormatHostPart(this Uri url) {
      var portS = url.IsDefaultPort ? "" : ":" + url.Port;
      return $"{url.Scheme}://{url.Host}{portS}";
    }

    public static string FormatCompact(this HttpRequestMessage url) => $"{url.Method} {url.RequestUri}";

    public static Task<HttpResponseMessage> Get(this HttpClient client, string requestUri, ILogger log = null) {
      var msg = new HttpRequestMessage(HttpMethod.Get, requestUri);
      return client.SendAsyncWithLog(msg, log);
    }

    public static Task<HttpResponseMessage> PostJson(this HttpClient client, string requestUri, string json, ILogger log = null,
      HttpCompletionOption completion = HttpCompletionOption.ResponseHeadersRead) {
      var msg = new HttpRequestMessage(HttpMethod.Post, requestUri).WithJsonContent(json);
      return client.SendAsyncWithLog(msg, log, completion);
    }

    public static Task<HttpResponseMessage> SendAsyncWithLog(this HttpClient client, HttpRequestMessage request, ILogger log = null,
      HttpCompletionOption completion = HttpCompletionOption.ResponseHeadersRead) {
      try {
        return InnerSendAsyncWithLog(client, getRequest: null, request, completion, log);
      }
      catch (TaskCanceledException e) {
        throw
          new HttpRequestException("Request timed out",
            e); // throw a diffferent exceptions. Otherwise TPL and other libraries treat this a an intentional cancellation and swallow
      }
    }

    public static async Task<HttpResponseMessage> SendAsyncWithLog(this HttpClient client, Func<HttpRequestMessage> request, ILogger log,
      HttpCompletionOption completion, AsyncPolicy<HttpResponseMessage> policy) {
      try {
        return await policy.ExecuteAsync(() => InnerSendAsyncWithLog(client, getRequest: null, request(), completion, log));
      }
      catch (TaskCanceledException e) {
        throw
          new HttpRequestException("Request timed out",
            e); // throw a diffferent exceptions. Otherwise TPL and other libraries treat this a an intentional cancellation and swallow
      }
    }

    public static async Task<HttpResponseMessage> SendAsyncWithLog(this HttpClient client, Func<Task<HttpRequestMessage>> getRequest, ILogger log,
      HttpCompletionOption completion, AsyncPolicy<HttpResponseMessage> policy) {
      try {
        return await policy.ExecuteAsync(() => InnerSendAsyncWithLog(client, getRequest, request: null, completion, log));
      }
      catch (TaskCanceledException e) {
        throw
          new HttpRequestException("Request timed out",
            e); // throw a diffferent exceptions. Otherwise TPL and other libraries treat this a an intentional cancellation and swallow
      }
    }

    public static HttpRequestMessage WithStreamContent(this HttpRequestMessage request, Stream stream) {
      request.Content = new StreamContent(stream);
      return request;
    }

    public static HttpRequestMessage WithJsonContent(this HttpRequestMessage request, string json) {
      request.Content = new StringContent(json, Encoding.UTF8, "application/json");
      return request;
    }

    public static HttpRequestMessage WithJsonContent(this HttpRequestMessage request, object data, JsonSerializerSettings settings)
      => request.WithJsonContent(data.ToJson(settings));

    public static async Task<T> JsonContentAs<T>(this HttpResponseMessage response, JsonSerializerSettings settings = null) {
      var json = await response.ContentAsString();
      return json.ToObject<T>(settings);
      //return JsonSerializer.Create(settings).Deserialize<T>(reader);
    }

    /// <summary>Reads the content as a string (and unzip if required)</summary>
    public static async Task<string> ContentAsString(this HttpResponseMessage response) {
      using var stream = await response.ContentAsStream();
      return await stream.ReadToEndAsync();
    }

    /// <summary>Reads the content as a text stream (and unzip if required)</summary>
    public static async Task<StreamReader> ContentAsStream(this HttpResponseMessage response) {
      var stream = await response.Content.ReadAsStreamAsync();
      if (response.Content.Headers.ContentEncoding.Contains("gzip"))
        stream = new GZipStream(stream, CompressionMode.Decompress);
      return new(stream);
    }

    /// <summary>Reads the content as Json (and unzip if required)</summary>
    public static async Task<JsonReader> ContentAsJsonReader(this HttpResponseMessage response) =>
      new JsonTextReader(await response.ContentAsStream()) {CloseInput = true};

    public static async Task EnsureSuccesWithFullError(this HttpResponseMessage response) {
      if (!response.IsSuccessStatusCode) {
        var body = await response.Content.ReadAsStringAsync();
        throw new HttpRequestException($"{response.StatusCode}: '{body}'. Original request: {response.RequestMessage.FormatAsCurl()}");
      }
    }

    public static bool IsTransientError(this HttpResponseMessage msg) => (int) msg.StatusCode >= 500 || msg.StatusCode == HttpStatusCode.RequestTimeout;

    static async Task<HttpResponseMessage> InnerSendAsyncWithLog(HttpClient client,
      Func<Task<HttpRequestMessage>> getRequest,
      HttpRequestMessage request,
      HttpCompletionOption completion, ILogger log) {
      if (request == null)
        request = await getRequest().ConfigureAwait(false);

      var url = request.RequestUri;

      var timer = Stopwatch.StartNew();
      log?.Verbose("{Method} {Server}{Path} sending",
        request.Method, FormatHostPart(url), request.RequestUri.PathAndQuery);
      var response = await client.SendAsync(request, completion);

      var errorContent = response.IsSuccessStatusCode
        ? null
        : response.Content == null
          ? ""
          : await response.Content.ReadAsStringAsync();

      log?.Verbose("{Method} {Server}{Path} {Status} in {Duration}. {ErrorMessage}",
        request.Method, FormatHostPart(request.RequestUri), request.RequestUri.PathAndQuery, response.StatusCode, timer.Elapsed.Humanize(2),
        errorContent);

      return response;
    }

    public static string UrlEncode(this string url) => WebUtility.UrlEncode(url);

    public static string UrlDecode(this string url) => WebUtility.UrlDecode(url);

    public static (string Name, string Value)[] Cookies(this HttpResponseHeaders headers) =>
      headers.TryGetValues("Set-Cookie", out var values)
        ? SetCookieHeaderValue.ParseList(values.ToList()).Select(cookie => (cookie.Name.Value, cookie.Value.Value)).ToArray()
        : Array.Empty<(string, string)>();
  }
}