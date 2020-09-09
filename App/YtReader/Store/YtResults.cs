using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using CsvHelper;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions;
using SysExtensions.Fluent.IO;
using SysExtensions.IO;
using SysExtensions.Net;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;

namespace YtReader.Store {
  class ResQuery {
    public ResQuery(string name, string query = null, string desc = null, object parameters = null, bool inSharedZip = false,
      ResFilType fileType = ResFilType.Csv, JsonSource jsonSource = default) {
      Name = name;
      Query = query;
      Desc = desc;
      Parameters = parameters;
      InSharedZip = inSharedZip;
      FileType = fileType;
      JsonSource = jsonSource;
    }

    public string     Name        { get; }
    public string     Query       { get; }
    public string     Desc        { get; }
    public object     Parameters  { get; }
    public bool       InSharedZip { get; }
    public ResFilType FileType    { get; }
    public JsonSource JsonSource { get; }
  }

  public enum JsonSource {
    AllColumns,
    FirstColumn
  }

  enum ResFilType {
    Csv,
    Json
  }

  class FileQuery : ResQuery {
    public FileQuery(string name, StringPath path, string desc = null, object parameters = null, bool inSharedZip = false)
      : base(name, desc: desc, parameters: parameters, inSharedZip: inSharedZip) =>
      Path = path;

    public StringPath Path { get; set; }
  }

  public class YtResults {
    const    string                      Version = "v2.4";
    readonly HttpClient                  Http    = new HttpClient();
    readonly SnowflakeConnectionProvider Sf;
    readonly ResultsCfg                  ResCfg;
    readonly ISimpleFileStore            Store;
    readonly UserScrapeCfg               UserScrapeCfg;

    public YtResults(SnowflakeConnectionProvider sf, ResultsCfg resCfg, ISimpleFileStore store, UserScrapeCfg userScrapeCfg) {
      Sf = sf;
      ResCfg = resCfg;
      Store = store;
      UserScrapeCfg = userScrapeCfg;
    }

    public async Task SaveBlobResults(ILogger log, IReadOnlyCollection<string> queryNames = null) {
      using var db = await Sf.OpenConnection(log);
      queryNames ??= new string[] { };

      var now = DateTime.Now;
      var dateRangeParams = new {from = "2019-11-01", to = now.ToString("yyyy-MM-01")};

      const string classChannelsSelect = @"
select c.*
     , cr.lr_human
     , cr.tags_human
     , cr.relevance_human
from channel_latest c
       left join channel_review cr on cr.channel_id=c.channel_id
where c.reviews_all>0";

      var queries = new[] {
          new FileQuery("vis_channel_stats", "sql/vis_channel_stats.sql",
            "data combined from classifications + information (from the YouTube API)", dateRangeParams, inSharedZip: true),

          new FileQuery("vis_category_recs", "sql/vis_category_recs.sql",
            "aggregate recommendations between all combinations of the categories available on recfluence.net", dateRangeParams, inSharedZip: true),

          new FileQuery("vis_channel_recs2", "sql/vis_channel_recs.sql",
            "aggregated recommendations between channels (scraped form the YouTube website)", dateRangeParams, inSharedZip: true),
          
          new FileQuery("vis_tag_recs", "sql/vis_tag_recs.sql",
            "aggregated recommendations between channels (scraped form the YouTube website)", dateRangeParams, inSharedZip: true),

          new FileQuery("channel_review", "sql/channel_review.sql",
            desc: "each reviewers classifications and the calculated majority view (data entered independently from reviewers)", inSharedZip: true),

          // userscrape data
          new FileQuery("us_seeds", "sql/us_seeds.sql", parameters: new {videos_per_tag = UserScrapeCfg.SeedsPerTag}),
          new FileQuery("us_tests", "sql/us_tests.sql", parameters: new {videos = UserScrapeCfg.Tests}),
          
          // classification data
          
          new ResQuery("review_channels", @"select channel_id as ""ChannelId""
     , channel_title as ""ChannelTitle""
     , description as ""Description""
     , logo_url as ""LogoUrl""
     , channel_views as ""ChannelViews""
     , reviews_all as ""ReviewsAll""
    , reviews_algo as ""ReviewsAlgo""
from channel_latest where meets_sub_criteria", fileType: ResFilType.Json),
          
          new ResQuery("class_channels", classChannelsSelect, fileType: ResFilType.Json),

          new ResQuery("class_snippets", $@"with reviewed as ({classChannelsSelect})
  , video_snippets as (
  select channel_id
       , listagg(video_title, '\n') as titles
       , listagg(keywords, '\n') as keywords
       , listagg(description, '\n') as descriptions
  from (
         select channel_id
              , v.video_title
              , v.description
              , array_to_string(v.keywords, ', ') as keywords
         from video_latest v
           qualify row_number() over (partition by channel_id order by random())<=20
       )
  group by channel_id
)
select c.channel_id, c.channel_title
     , concat_ws('\n', c.channel_title, c.description, coalesce(v.titles, ''), coalesce(v.keywords, ''), coalesce(v.descriptions, '')) as snippets
from reviewed c
       left join video_snippets v on v.channel_id=c.channel_id",
            fileType: ResFilType.Json),

          new ResQuery("class_comments", $@"with reviewed as ({classChannelsSelect})
select channel_id
     , any_value(channel_title) as channel_title
     , listagg(comment, '\n') as comments
     , count(comment) as comment_count
from (
       select c.channel_id, c.channel_title, vc.comment
       from reviewed c
              left join video_comments vc on vc.channel_id=c.channel_id
         qualify row_number() over (partition by c.channel_id order by random())<=500
     )
group by channel_id",
            fileType: ResFilType.Json),

          new ResQuery("class_captions", $@"with reviewed as ({classChannelsSelect})
select channel_id
     , any_value(channel_title) as channel_title
     , listagg(caption, '\n') as captions
from (
       select c.channel_id, c.channel_title, vc.caption
       from reviewed c
              left join video_latest v on v.channel_id=c.channel_id
              left join caption vc on vc.video_id=v.video_id
       where caption is not null
         qualify row_number() over (partition by c.channel_id order by random())<=100
     )
group by channel_id",
            fileType: ResFilType.Json),

          /*new ResQuery("icc_tags", desc: "channel classifications in a format used to calculate how consistent reviewers are when tagging"),
          new ResQuery("icc_lr", desc: "channel classifications in a format used to calculate how consistent reviewers are when deciding left/right/center"),
          new FileQuery("rec_accuracy", "sql/rec_accuracy.sql", "Calculates the accuracy of our estimates vs exported recommendations")*/

          // videos & recs are too large to share in a file. Use snowflake directly to share this data at-cost.
          /*("video_latest", @"select video_id, video_title, channel_id, channel_title,
           upload_date, avg_rating, likes, dislikes, views, thumb_standard from video_latest")*/
        }
        .Where(q => !queryNames.Any() || queryNames.Contains(q.Name, StringComparer.OrdinalIgnoreCase))
        .ToList();

      var tmpDir = TempDir();

      var results = await queries.BlockFunc(async q => (file: await SaveResult(log, db, tmpDir, q), query: q), ResCfg.Parallel);

      if (queryNames?.Any() != true) await SaveResultsZip(log, results);
    }

    async Task SaveResultsZip(ILogger log, IReadOnlyCollection<(FPath file, ResQuery query)> results) {
      var sw = Stopwatch.StartNew();
      var zipPath = results.First().file.Parent().Combine("recfluence_shared_data.zip");
      using (var zipFile = ZipFile.Open(zipPath.FullPath, ZipArchiveMode.Create)) {
        var readmeFile = TempDir().CreateFile("readme.txt", $@"Recfluence data generated {DateTime.UtcNow:yyyy-MM-dd}

{results.Join("\n\n", r => $"*{r.query.Name}*\n  {r.query.Desc}")}
        ");
        zipFile.CreateEntryFromFile(readmeFile.FullPath, readmeFile.FileName);

        foreach (var f in results.Where(r => r.query.InSharedZip).Select(r => r.file)) {
          var name = f.FileNameWithoutExtension;
          var e = zipFile.CreateEntry(name);
          using var ew = e.Open();
          var fr = f.Open(FileMode.Open, FileAccess.Read);
          var gz = new GZipStream(fr, CompressionMode.Decompress);
          await gz.CopyToAsync(ew);
        }
      }

      await Save(log, zipPath.FileName, zipPath);
      Log.Information("Result - saved zip {Name} in {Duration}", zipPath.FileName, sw.Elapsed);
    }

    static FPath TempDir() {
      var path = Path.GetTempPath().AsPath().Combine(Guid.NewGuid().ToShortString());
      if (!path.Exists)
        path.CreateDirectory();
      return path;
    }

    /// <summary>Saves the result for the given query to Storage and a local tmp file</summary>
    async Task<FPath> SaveResult(ILogger log, LoggedConnection db, FPath tempDir, ResQuery q) {
      var reader = await ResQuery(db, q);
      var fileName = q.FileType switch {
        ResFilType.Csv => $"{q.Name}.csv.gz",
        ResFilType.Json => $"{q.Name}.jsonl.gz",
        _ => throw new NotImplementedException()
      };
      var tempFile = tempDir.Combine(fileName);
      using (var fw = tempFile.Open(FileMode.CreateNew, FileAccess.Write))
      using (var zw = new GZipStream(fw, CompressionLevel.Optimal, leaveOpen: true))
      using (var sw = new StreamWriter(zw)) {
        var task = q.FileType switch {
          ResFilType.Csv => reader.WriteCsvGz(sw, fileName, log),
          ResFilType.Json => reader.WriteJsonGz(sw, q.JsonSource),
          _ => throw new NotImplementedException()
        };
        await task;
      }
      await Save(log, fileName, tempFile);
      return tempFile;
    }

    async Task<IDataReader> ResQuery(LoggedConnection db, ResQuery q) {
      var query = q.Query ?? $"select * from {q.Name}";
      if (q is FileQuery f) {
        var req = new Uri(ResCfg.FileQueryUri + "/" + f.Path.StringValue).Get()
          .AddHeader("Cache-Control", "no-cache");

        var res = await Http.SendAsync(req);
        query = await res.ContentAsString();
      }
      Log.Information("Saving result {Name}: {Query}", q.Name, query);
      try {
        var reader = await db.ExecuteReader("save result", query, q.Parameters);
        return reader;
      }
      catch (Exception ex) {
        throw new InvalidOperationException($"Error when executing '{q.Name}': {ex.Message}", ex);
      }
    }

    async Task Save(ILogger log, string fileName, FPath tempFile) {
      async Task Save() {
        await Store.Save(fileName, tempFile, log);
        var url = Store.Url(fileName);
        Log.Information("Result - saved {Name} to {Url}", fileName, url);
      }

      await Save();
    }
  }

  public static class SnowflakeResultHelper {
    public static async Task WriteCsvGz(this IDataReader reader, StreamWriter stream, string desc, ILogger log) {
      using var csvWriter = new CsvWriter(stream, CultureInfo.InvariantCulture);

      foreach (var col in reader.FieldRange().Select(reader.GetName)) csvWriter.WriteField(col);
      await csvWriter.NextRecordAsync();

      while (reader.Read()) {
        foreach (var i in reader.FieldRange()) {
          var o = reader[i];
          if (o is DateTime d)
            csvWriter.WriteField(d.ToString("O"));
          else
            csvWriter.WriteField(o);
        }
        await csvWriter.NextRecordAsync();
      }
    }

    public static async Task WriteJsonGz(this IDataReader reader, StreamWriter stream, JsonSource jsonSource) {
      while (reader.Read()) {
        var j = new JObject();
        if (jsonSource == JsonSource.FirstColumn)
          j = JObject.Parse(reader.GetString(0));
        else
          foreach (var i in reader.FieldRange())
            j.Add(reader.GetName(i), JToken.FromObject(reader[i]));
        await stream.WriteLineAsync(j.ToString(Formatting.None));
      }
    }

    static IEnumerable<int> FieldRange(this IDataRecord reader) => Enumerable.Range(0, reader.FieldCount);
  }
}