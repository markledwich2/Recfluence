using System;
using System.Linq;
using System.Threading.Tasks;
using Humanizer;
using Mutuo.Etl.Pipe;
using Nest;
using Polly;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Net;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using Policy = Polly.Policy;

namespace YtReader.Search {
  public class YtSearch {
    readonly AppDb         Db;
    readonly ElasticClient Elastic;
    readonly ILogger       Log;

    public YtSearch(AppDb db, ElasticClient elastic, ILogger log) {
      Db = db;
      Elastic = elastic;
      Log = log;
    }

    [Pipe]
    public async Task CaptionIndex(bool fullLoad = false, long? limit = null) {
      using var conn = await Db.OpenLoggedConnection(Log);
      var lastUpdateRes = await Elastic.SearchAsync<VideoCaption>(c => c.Aggregations(a => a.Max("max_updated", m => m.Field(p => p.updated))));
      var lastUpdate = lastUpdateRes.Aggregations.Max("max_updated")?.ValueAsString.ParseDate();
      var param = lastUpdate == null || fullLoad ? null : new {max_updated = lastUpdate};
      var sqlConditions = new[] {
        "views > 1000",
        param?.max_updated == null ? null : "updated > :max_updated"
      }.NotNull().Join(" and ");
      var sqlLimit = limit == null ? "" : $" limit {limit}";
      var batchSize = 10000;
      var allCaps = conn.Query<VideoCaption>(nameof(CaptionIndex),
          $"select * from caption where {sqlConditions}{sqlLimit}",
          param, buffered: false)
        .Batch(batchSize).WithIndex();

      var elasticPolicy = Policy
        .HandleResult<BulkResponse>(r => r.ItemsWithErrors.Any(i => i.Status == 403))
        .RetryAsync(3, async (r, i) => {
          var delay = i.ExponentialBackoff(1.Seconds());
          Log?.Debug("Retryable error indexing captions: Retrying in {Duration}, attempt {Attempt}/{Total}",
            delay, i, 3);
          await Task.Delay(delay);
        });

      var completedNo = await allCaps.BlockAction(async b => {
        var (item, index) = b;
        var res = await elasticPolicy.ExecuteAsync(() => Elastic.IndexManyAsync(item));
        if (res.ItemsWithErrors.Any())
          Log.Information("Indexed {Success}/{Total} elastic documents (batch {Batch}). Top 5 Error items: {@ItemsWithErrors}",
            res.Items.Count - res.ItemsWithErrors.Count(), item.Count, index, res.ItemsWithErrors.Select(r => r.Error).Take(5));
        else
          Log.Information("Indexed {Success}/{Total} elastic documents (batch {Batch})",
            res.Items.Count, item.Count, index);
      }, 3, 8);

      Log.Information("Completed indexing {Captions} captions", completedNo * batchSize);
    }
  }

  [ElasticsearchType(IdProperty = nameof(caption_id))]
  public class VideoCaption {
    /*public string ObjectID {
      get => _objectId ?? $"{video_id}|{offset_seconds}";
      set => _objectId = value;
    }*/
    public string caption_id    { get; set; }
    public string video_id      { get; set; }
    public string ideology      { get; set; }
    public string media         { get; set; }
    public string country       { get; set; }
    public string lr            { get; set; }
    public string video_title   { get; set; }
    public string channel_title { get; set; }
    public string channel_id    { get; set; }
    public string keywords      { get; set; }
    //public string description { get; set; }
    public string   thumb_high     { get; set; }
    public long     offset_seconds { get; set; }
    public string   caption        { get; set; }
    public DateTime upload_date    { get; set; }
    public DateTime updated        { get; set; }
    public double?  pcd_ads        { get; set; }
    public long     views          { get; set; }
    public string url {
      get => $"https://youtu.be/{video_id}?t={offset_seconds}";
      set { }
    }
  }
}