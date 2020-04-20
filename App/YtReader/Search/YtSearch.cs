using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Algolia.Search.Clients;
using Algolia.Search.Exceptions;
using Mutuo.Etl.Db;
using Nest;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Net;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using Policy = Polly.Policy;
using TimeUnit = Humanizer.Localisation.TimeUnit;

namespace YtReader.Search {
  public class YtSearch {
    readonly AlgoliaCfg    Angolia;
    readonly AppDb         Db;
    readonly ElasticClient Elastic;
    readonly ILogger       Log;
    readonly SolrCfg       Solr;

    public YtSearch(AlgoliaCfg angolia, SolrCfg solr, AppDb db, ElasticClient elastic, ILogger log) {
      Angolia = angolia;
      Solr = solr;
      Db = db;
      Elastic = elastic;
      Log = log;
    }

    public async Task BulkElasticCaptionIndex() {
      //trying to allow aggregation on categorical fields. https://www.elastic.co/guide/en/elasticsearch/reference/current/fielddata.html

      var dir = $"captionIndex/{DateTime.UtcNow.FileSafeTimestamp()}".AsPath()
        .InAppData("recfluence").CreateDirectories();

      var files = new List<FPath>();
      using (var conn = await Db.OpenLoggedConnection(Log))
        foreach (var (batch, index) in conn.Query<VideoCaption>(nameof(GetCaptionsRecords),
          "select * from caption where views > 10000 limit 200000", buffered: false).Batch(50000).WithIndex()) {
          var file = dir.Combine($"captions.{index}.jsonl.gz");
          batch.ToJsonlGz(file.FullPath);
          Log.Debug("cached caption file for indexing: {File}", file.FullPath);
          files.Add(file);
        }

      await files.BlockAction(async f => {
        var captons = f.OpenText().LoadJsonlGz<VideoCaption>().ToArray();
        var res = await Elastic.IndexManyAsync(captons);
        if (res.ItemsWithErrors.Any())
          Log.Information("Indexed {Success}/{Total} elastic documents. Top 5 Error items: {@ItemsWithErrors}",
            res.Items.Count - res.ItemsWithErrors.Count(), captons.Length, res.ItemsWithErrors.Take(5));
        else
          Log.Information("Indexed {Success}/{Total} elastic documents", res.Items.Count, captons.Length);
      }, 4);
    }

    public async Task BuildSolrCaptionIndex() {
      using var conn = await Db.OpenLoggedConnection(Log);
      var caps = GetCaptionsRecords(conn);
      var http = new HttpClient();
      await caps.Batch(1000).BlockAction(async batch => {
        var req = Solr.Url.Build()
          .WithPathSegment("captions/update").WithParameter("commit", "true")
          .Post()
          .WithJsonContent(batch.ToJson());
        var res = await http.SendAsyncWithLog(req);
        res.EnsureSuccessStatusCode();
        Log.Information("Indexed {Objects} solr objects", batch.Count);
      }, 4);
    }

    public async Task BuildAlgoliaVideoIndex() {
      using var conn = await Db.OpenLoggedConnection(Log);
      var client = new SearchClient(Angolia.Creds.Name, Angolia.Creds.Secret);
      var index = client.InitIndex("captions");
      var rawCaps = GetCaptionsRecords(conn);

      var algoliaPolicy = Policy.Handle<AlgoliaUnreachableHostException>().RetryWithBackoff("saving algolia objects", 3, Log);

      await rawCaps
        .ChunkBy(c => c.video_id).Select(VideoCaptions).SelectMany(c => c) // ungroup
        .Batch(1000) // batch 1000 videos
        .BlockAction(async caps => {
          // TODO fix this to work with new POCO (No objectid)
          var existingObjects = await index.GetObjectsAsync<VideoCaption>(caps.Select(c => c.caption_id), attributesToRetrieve: new[] {"caption_id"});
          var existingIds = existingObjects.NotNull().ToArray().Select(c => c.caption_id).ToHashSet();
          var toUpload = caps.Where(c => !existingIds.Contains(c.caption_id)).ToArray();
          var sw = Stopwatch.StartNew();
          if (toUpload.Any()) {
            var res = await algoliaPolicy.ExecuteAsync(_ => index.SaveObjectsAsync(caps), CancellationToken.None);
          }
          Log.Information("Indexed {NewCaptions}/{Total} in {Duration}", toUpload.Length, caps.Count, sw.Elapsed.HumanizeShort(minUnit: TimeUnit.Millisecond));
        });
    }

    static IEnumerable<VideoCaption> GetCaptionsRecords(LoggedConnection conn, int limit = 0) {
      var limitStr = limit == 0 ? "" : $"limit {limit}";

      return conn.Query<VideoCaption>(nameof(GetCaptionsRecords), $@"
with captions_carona as (
  select *
  from caption c
  where upload_date >= :upload_from
    and views > :min_views
    and c.caption rlike :corona_regex
)
select *
from caption c
where exists(select *
             from captions_carona cc
             where c.video_id = cc.video_id
               and cc.offset_seconds between c.offset_seconds - :leway_seconds and c.offset_seconds + :leway_seconds)
{limitStr}",
        buffered: false,
        param: new {
          upload_from = "2019-12-01",
          min_views = 100000,
          corona_regex = @".*\W(corona(-?virus)?|covid(-?19)?|(SARS-CoV-2)|pandemic)\W.*",
          leway_seconds = 120
        });
    }

    static IEnumerable<VideoCaption> VideoCaptions(IGrouping<string, VideoCaption> vidGroup) {
      var caps = vidGroup
        .OrderBy(v => v.offset_seconds) // make sure that this will give the same objectID's with the same data
        .Batch(3) // 3 captions per line (the riginal is too narrow
        .Batch(5) // 5 lines per document
        .Select(g => {
          var res = g.First().First().JsonClone(); // clone the first to use as the main record
          res.caption = CaptionForBatch(g);
          return res;
        });
      return caps;
    }

    static string CaptionForBatch(IReadOnlyCollection<IReadOnlyCollection<VideoCaption>> g) =>
      g.Join("\n", g2 => g2.Join(" ", c => c.caption));

    //$"<a href='{f.url}'>{f.offset_seconds.Seconds().HumanizeShort()}</a> {microCaps.Join(" ", c => c.caption)}";
  }

  [ElasticsearchType(IdProperty = nameof(caption_id))]
  public class VideoCaption {
    /*public string ObjectID {
      get => _objectId ?? $"{video_id}|{offset_seconds}";
      set => _objectId = value;
    }*/
    public string   caption_id     { get; set; }
    public string   video_id       { get; set; }
    public string   ideology       { get; set; }
    public string   media          { get; set; }
    public string   country        { get; set; }
    public string   lr             { get; set; }
    public string   video_title    { get; set; }
    public string   channel_title  { get; set; }
    public string   channel_id     { get; set; }
    public string   keywords       { get; set; }
    public string   description    { get; set; }
    public string   thumb_high     { get; set; }
    public long     offset_seconds { get; set; }
    public string   caption        { get; set; }
    public DateTime upload_date    { get; set; }
    public long     views          { get; set; }
    public string url {
      get => $"https://youtu.be/{video_id}?t={offset_seconds}";
      set { }
    }
  }
}