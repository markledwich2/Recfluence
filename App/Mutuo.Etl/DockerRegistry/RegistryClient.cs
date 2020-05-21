using System;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using SysExtensions.Net;
using SysExtensions.Security;
using SysExtensions.Serialization;

namespace Mutuo.Etl.DockerRegistry {
  public class RegistryClient {
    readonly string     Host;
    readonly NameSecret Creds;
    readonly HttpClient Http = new HttpClient();

    public RegistryClient(string host, NameSecret creds) {
      Host = host;
      Creds = creds;
    }

    public async Task<string[]> Catalogs() => (await Get<Catalog>("_catalog")).Repositories;

    public Task<TagList> TagList(string name) => Get<TagList>($"{name}/tags/list");

    public Task<Manifest> Manifest(string name, string tag) => Get<Manifest>($"{name}/manifests/{tag}");

    public async Task<string> ManifestContentDigestV2(string name, string tag) {
      var req = Req($"{name}/manifests/{tag}", HttpMethod.Head)
        .Accept("application/vnd.docker.distribution.manifest.v2+json");
      var res = await Res(req);
      return res.Headers.GetValues("Docker-Content-Digest").Single();
    }

    /// <summary>Delete an image given reference (the content-digest of the image)</summary>
    /// <param name="name">name of the repository</param>
    /// <param name="reference">This must be the content digest of the image</param>
    public async Task DeleteImage(string name, string reference) => await Delete($"{name}/manifests/{reference}");

    async Task<T> Get<T>(string path) => await (await Res(Req(path, HttpMethod.Get))).JsonContentAs<T>();

    async Task<HttpResponseMessage> Res(HttpRequestMessage msg) {
      var res = await Http.SendAsync(msg);
      res.EnsureSuccessStatusCode();
      return res;
    }

    async Task Delete(string path) {
      var req = Req(path, HttpMethod.Delete);
      var res = await Http.SendAsync(req);
      res.EnsureSuccessStatusCode();
    }

    HttpRequestMessage Req(string path, HttpMethod method) => new UriBuilder(Uri.UriSchemeHttps, Host)
      .WithPathSegment($"v2/{path}")
      .Request(method)
      .BasicAuth(Creds);

    class Catalog {
      public string[] Repositories { get; set; }
    }
  }

  public class Manifest {
    public int       SchemaVersion { get; set; }
    public string    Name          { get; set; }
    public string    Tag           { get; set; }
    public string    Architecture  { get; set; }
    public History[] History       { get; set; }
  }

  public class History {
    public string V1Compatibility { get; set; }
  }

  public class TagList {
    public string   Name { get; set; }
    public string[] Tags { get; set; }
  }

  public static class RegistryClientHelper {
    public static DateTime TagCreated(this Manifest manifest) => manifest.History.FirstOrDefault()
      .V1Compatibility.ParseJObject().PropertyValue<DateTime>("created");
  }
}