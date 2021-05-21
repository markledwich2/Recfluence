using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Flurl;
using Flurl.Http;
using Mutuo.Etl.Blob;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Text;
using YtReader.Store;
using YtReader.Web;

// ReSharper disable InconsistentNaming

namespace YtReader.Reddit {
  record PushPost {
    public string          id                  { get; init; }
    public string          title               { get; init; }
    public string          author              { get; init; }
    public long            created_utc         { get; init; }
    public long            retrieved_on        { get; init; }
    public string          full_link           { get; init; }
    public string          selftext            { get; init; }
    public string          subreddit           { get; init; }
    public string          subreddit_id        { get; init; }
    public string          url                 { get; init; }
    public string          removed_by_category { get; init; }
    public int             num_comments        { get; init; }
    public double          upvote_ratio        { get; init; }
    public PushSecureMedia secure_media        { get; init; }
  }

  record PushResult<T>(T[] data, PushMetadata metadata = null);

  record PushMetadata(long total_results);

  record PushSecureMedia {
    public string  type   { get; init; }
    public JObject oembed { get; init; }
  }

  public record Pushshift(YtStore Store, Stage stage) {
    readonly string SearchUrl = "https://api.pushshift.io/reddit/search".AsUrl();

    static readonly string[] Fields = typeof(PushPost).GetProperties().Select(p => p.Name).ToArray();

    public record PushParams {
      public string q         { get; init; }
      public string fields    { get; init; }
      public string sort_type { get; init; }
      public string sort      { get; init; }
      public int    size      { get; init; }
      public long?  after     { get; init; }
      public bool   metadata  { get; set; }
    }

    public async Task Process(ILogger log) {
      var store = new JsonlStore<PushPost>(Store.Store, "reddit/posts/corona", r => r.retrieved_on.ToString("000000000000"), log);
      var latestFile = await store.LatestFile();
      if (latestFile != null) throw new InvalidOperationException("Don't support incremental yet :( pls delete files if you intend to re-get");
      long saved = 0;
      var (total, subs) = await GetSubmissions(new() {
        q = "(covid|\"covid-19\"|coronavirus|sars|\"SARS-CoV-2\"|vaccine|\"Wuhan flu\"|\"China virus\"|vaccinated|\"Bill Gates\""
          + "|Pfizer|Moderna|BioNTech|AstraZeneca|CDC|\"world health organization\"|\"Herd immunity\"|Pandemic|Lockdown) +(youtu.be|youtube.com|youtube)",
        fields = Fields.Join(","),
        sort_type = "created_utc",
        sort = "asc",
        size = 100,
        after = new DateTimeOffset(year: 2020, month: 1, day: 1, hour: 0, minute: 0, second: 0, TimeSpan.Zero)
          .ToUnixTimeSeconds() //latestFile?.Ts.ParseLong() ?? 
      }, log);

      await subs.Batch(10).ForEachAsync(async r => {
        var items = r.SelectMany().ToArray();
        await store.Append(items);
        saved += items.Length;
        log.Information("Pushshift - saved {Rows}/{Total} last created {Created}",
          saved, total, DateTimeOffset.FromUnixTimeSeconds(items.Last().created_utc).ToString("s"));
      });

      await stage.UpdateTable(new(store.Path, "reddit_post_stage", isNativeStore: false), fullLoad: true, log);
    }

    async Task<(long total_results, IAsyncEnumerable<PushPost[]>)> GetSubmissions(PushParams pushParams, ILogger log) {
      var page = await GetPosts(pushParams with {metadata = true});
      return (page.metadata.total_results, EnumerateSubmissions());

      async Task<PushResult<PushPost>> GetPosts(PushParams p) {
        var url = SearchUrl.AppendPathSegment("submission").SetParams(p, isEncoded: true);
        log.Debug("Pushshift -  loaded data from: {Url}", url.ToString());
        return await url.GetJsonAsync<PushResult<PushPost>>();
      }

      async IAsyncEnumerable<PushPost[]> EnumerateSubmissions() {
        while (page.data?.Any() == true) {
          yield return page.data;
          pushParams = pushParams with {after = page.data.Last().created_utc};
          page = await GetPosts(pushParams);
        }
      }
    }
  }
}