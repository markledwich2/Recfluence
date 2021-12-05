using System.Net;

namespace SysExtensions.Net;

public static class FunctionExtensions {
  public static HttpResponseMessage AsyncResponse(this HttpRequestMessage req, string message) => new HttpResponseMessage(HttpStatusCode.OK)
    { RequestMessage = req, Content = new StringContent(message) };
}