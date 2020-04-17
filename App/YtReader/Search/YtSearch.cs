using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Algolia.Search.Clients;
using Algolia.Search.Exceptions;
using Dapper;
using Humanizer.Localisation;
using Polly;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;

namespace YtReader.Search {
  public class YtSearch {
    readonly AlgoliaCfg Angolia;
    readonly AppDb      Db;
    readonly ILogger    Log;
    readonly SolrCfg    Solr;

    public YtSearch(AlgoliaCfg angolia, SolrCfg solr, AppDb db, ILogger log) {
      Angolia = angolia;
      Solr = solr;
      Db = db;
      Log = log;
    }

    public async Task BuildSolrCaptionIndex() {
      using var conn = await Db.OpenConnection();
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
      using var conn = await Db.OpenConnection();
      var client = new SearchClient(Angolia.Creds.Name, Angolia.Creds.Secret);
      var index = client.InitIndex("captions");
      var rawCaps = GetCaptionsRecords(conn);

      var algoliaPolicy = Policy.Handle<AlgoliaUnreachableHostException>().RetryWithBackoff("saving algolia objects", 3, Log);

      await rawCaps
        .ChunkBy(c => c.video_id)
        .Select(VideoCaptions)
        .SelectMany(c => c) // ungroup
        .Batch(1000) // batch 1000 videos
        .BlockAction(async caps => {
          var existingObjects = await index.GetObjectsAsync<VideoCaption>(caps.Select(c => c.ObjectID), attributesToRetrieve: new[] {"objectID"});
          var existingIds = existingObjects.NotNull().ToArray().Select(c => c.ObjectID).ToHashSet();
          var toUpload = caps.Where(c => !existingIds.Contains(c.ObjectID)).ToArray();
          var sw = Stopwatch.StartNew();
          if (toUpload.Any()) {
            var res = await algoliaPolicy.ExecuteAsync(_ => index.SaveObjectsAsync(caps), CancellationToken.None);
          }
          Log.Information("Indexed {NewCaptions}/{Total} in {Duration}", toUpload.Length, caps.Count, sw.Elapsed.HumanizeShort(minUnit: TimeUnit.Millisecond));
        });
    }

    static IEnumerable<VideoCaption> GetCaptionsRecords(IDbConnection conn, int limit = 0) {
      var limitStr = limit == 0 ? "" : $"limit {limit}";

      return conn.Query<VideoCaption>($@"
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

  public class VideoCaption {
    string _objectId;
    public string ObjectID {
      get => _objectId ?? $"{video_id}|{offset_seconds}";
      set => _objectId = value;
    }
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
    public string url {
      get => $"https://youtu.be/{video_id}?t={offset_seconds}";
      set { }
    }
  }
}