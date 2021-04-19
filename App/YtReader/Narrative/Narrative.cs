using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AirtableApiClient;
using Mutuo.Etl.Db;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;

// ReSharper disable InconsistentNaming

namespace YtReader.Narrative {
  public record AirtableCfg(string ApiKey = null, string BaseId = "appwfe3XfYqxn7v7I");
  public record NarrativesCfg;
  public record MentionRowKey(string mentionId);
  public record ChannelRowKey(string channelId);
  public record VideoRowKey(string videoId);

  public enum AirtablePart {
    Mention,
    Channel,
    Video
  }

  public record NarrativeOpts(string MentionQuery, int? Limit, AirtablePart[] Parts = null, string[] Videos = null);

  public static class NarrativeSql {
    public static string NamedQuery(string name) => NamedSql.TryGet(name) ?? throw new($"no sql called {name}");

    public static readonly Dictionary<string, string> NamedSql = new() {
      {
        "Activewear v2", @"
  select n.video_id, part, context, offset_seconds, m.value::string keyword
  from activewear_mentions n
  join video_latest v on v.video_id = n.video_id
  , table (flatten(matches)) m
  where keyword in ('lululemon')
"
      }
    };
  }

  public record Narrative(NarrativesCfg Cfg, AirtableCfg AirCfg, SnowflakeConnectionProvider Sf) {
    public async Task MargeIntoAirtable(NarrativeOpts op, ILogger log) {
      using var db = await Sf.Open(log);

      await db.Execute("create tmp mentions table", $@"
create or replace temporary table _mentions as 
(
  with q as ({NarrativeSql.NamedQuery(op.MentionQuery)}) 
  select * from q
  {op.Videos.Do(vids => $"where video_id in ({vids.Join(", ", v => v.SingleQuote())})")}
  {op.Limit.Do(l => $"limit {l}")}
)");

      var mentionSql = "select * from _mentions";

      if (op.Parts.ShouldRun(AirtablePart.Channel))
        await Sync<ChannelRowKey>($"{op.MentionQuery} Channels", db.ReadAsJson("narrative channels", @$"
  with mention as ({mentionSql})
  select c.channel_id, c.channel_title, c.subs, c.channel_views 
from channel_latest c
    where exists(select * from mention n join video_latest v on v.video_id = n.video_id where v.channel_id = c.channel_id)
  "), log);


      if (op.Parts.ShouldRun(AirtablePart.Video))
        await Sync<VideoRowKey>($"{op.MentionQuery} Videos", db.ReadAsJson("narrative channels", @$"
  with mention as ({mentionSql})
  select video_id, video_title, views, channel_id from video_latest v
    where exists(select * from mention m where m.video_id = v.video_id)
  ").Select(r => {
          // linking records need to ba an array
          r["CHANNEL_ID"] = new JArray(r["CHANNEL_ID"]);
          return r;
        }), log);

      if (op.Parts.ShouldRun(AirtablePart.Mention))
        await Sync<MentionRowKey>(op.MentionQuery, db.ReadAsJson("narrative mentions", @$"
  with mention as ({mentionSql})
  select 
  n.video_id||'|'||n.part||'|'||n.keyword||coalesce('|'||n.offset_seconds, '') as mention_id
  , n.video_id
       , n.part
       , '['||iff(n.offset_seconds is null,n.part,to_varchar(to_time(n.offset_seconds::string),'HH24:MI:SS'))
  ||'](https://youtube.com/watch?v='||n.video_id||iff(n.offset_seconds is null,'','&t='||n.offset_seconds)||')'
  ||' '||n.context context
  , n.offset_seconds
       , n.keyword
       , v.channel_id
       , v.views::int views
       , v.description
       , v.upload_date
       , v.error_type
  from mention n
         join video_latest v on v.video_id=n.video_id
  ").Select(r => {
          // linking records need to ba an array
          r["CHANNEL_ID"] = new JArray(r["CHANNEL_ID"]);
          r["VIDEO_ID"] = new JArray(r["VIDEO_ID"]);
          return r;
        }), log);
    }

    public async Task Sync<TKey>(string airTableName, IAsyncEnumerable<JObject> sourceRows, ILogger log) where TKey : class {
      using var airTable = new AirtableBase(AirCfg.ApiKey, AirCfg.BaseId);
      var keyFields = typeof(TKey).GetProperties().Select(p => p.Name).ToArray();
      var airRows = await airTable.Rows<TKey>(airTableName, keyFields, log).ToListAsync()
        .Then(rows => rows.ToKeyedCollection(r => r.Fields));
      const int batchSize = 10;
      await sourceRows.Select(v => v.ToCamelCase())
        .Batch(batchSize).BlockAction(async (rows, i) => {
          var batchRows = rows.Select(r => new {Key = r.ToObject<TKey>(), Row = r, AirFields = r.ToAirFields()}).ToArray();
          var (update, create) = batchRows.Split(r => airRows.ContainsKey(r.Key));
          if (create.Any()) {
            var createFields = create.Select(c => c.AirFields).ToArray();
            var res = await airTable.CreateMultipleRecords(airTableName, createFields, typecast: true);
            res.EnsureSuccess(log, airTableName);
            log.Information("CovidNarrative - created {Rows} in {Airtable}, batch {Batch}", create.Count, airTableName, i + 1);
          }

          if (update.Any()) {
            var updateFields = update.Select(u => new IdFields(airRows[u.Key].Id) {FieldsCollection = u.AirFields.FieldsCollection}).ToArray();
            var res = await airTable.UpdateMultipleRecords(airTableName, updateFields, typecast: true);
            res.EnsureSuccess(log, airTableName);
            log.Information("CovidNarrative - updated {Rows} rows in {Airtable}, batch {Batch}", update.Count, airTableName, i + 1);
          }
        });
    }
  }

  public static class AirtableExtensions {
    public static async IAsyncEnumerable<AirtableRecord<T>> Rows<T>(this AirtableBase at, string table, string[] fields = null, ILogger log = null) {
      string offset = null;
      while (true) {
        var res = await at.ListRecords<T>(table, offset, fields);
        res.EnsureSuccess(log, table);
        foreach (var r in res.Records)
          yield return r;
        offset = res.Offset;
        if (offset == null) break;
      }
    }

    public static async IAsyncEnumerable<AirtableRecord> Rows(this AirtableBase at, string table, string[] fields = null, ILogger log = null) {
      string offset = null;
      while (true) {
        var res = await at.ListRecords(table, offset, fields);
        res.EnsureSuccess(log, table);
        foreach (var r in res.Records)
          yield return r;
        offset = res.Offset;
        if (offset == null) break;
      }
    }

    public static void EnsureSuccess(this AirtableApiResponse res, ILogger log, string desc = null) {
      if (res.Success) return;
      var msg = res.AirtableApiError switch {
        AirtableInvalidRequestException r => r.DetailedErrorMessage ?? r.ErrorMessage,
        _ => null
      } ?? res.AirtableApiError.ErrorMessage ?? "not successful";
      log.Error(res.AirtableApiError, "Airtable {Desc}: {Error}", desc, msg);
      throw res.AirtableApiError as Exception ?? new(msg);
    }

    public static Fields ToAirFields(this JObject j) {
      var dic = j.ToObject<Dictionary<string, object>>();
      var fields = new Fields {FieldsCollection = dic};
      return fields;
    }

    public static T Value<T>(this Fields fields, string field) => (T) fields.FieldsCollection[field];

    public static JObject RecordJObject(this AirtableRecord record) {
      var j = new JObject(new JProperty("id", record.Id), new JProperty("createdTime", record.CreatedTime));
      foreach (var field in record.Fields)
        j.Add(field.Key, JToken.FromObject(field.Value));
      return j;
    }
  }
}