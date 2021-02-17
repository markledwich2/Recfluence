using System.IO;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using SysExtensions.Threading;

namespace YtReader {
  public static class FlurlExtensions {
    public static Url AsUrl(this string url) => new(url);
    public static IFlurlRequest AsRequest(this Url url) => new FlurlRequest(url);

    /// <summary>Reads the content as Json (and unzip if required)</summary>
    public static Task<JObject> JsonObject(this IFlurlResponse response) =>
      response.GetStreamAsync().Then(s => JObject.LoadAsync(new JsonTextReader(new StreamReader(s)) {CloseInput = true}));
  }
}