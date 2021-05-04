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

namespace YtReader.Airtable {
  public record AirtableCfg(string ApiKey = null);
  public record MentionRowKey(string mentionId);
  public record ChannelRowKey(string channelId);
  public record VideoRowKey(string videoId);

  public enum AtLabelPart {
    Mention,
    Channel,
    Video
  }
  
  public enum AtUpdateMode {
    Create,
    CreateAndUpdate
  }

  //"appwfe3XfYqxn7v7I"
  public record AtOps(string BaseId, string Name, int? Limit, AtLabelPart[] Parts = null, string[] Videos = null, AtUpdateMode Mode = AtUpdateMode.Create);

  public static class AtLabelSql {
    public static string NamedQuery(string name) => NamedSql.TryGet(name) ?? throw new($"no sql called {name}");

    public static readonly Dictionary<string, string> NamedSql = new() {
      {
        "Activewear", @"
  select n.video_id, part, context, offset_seconds, m.value::string keyword
  from mention_activewear n
  join video_latest v on v.video_id = n.video_id
  , table (flatten(matches)) m
  where keyword in ('lululemon')
"
      }, {
        "Vaccine", @"
  select n.video_id, part, context, offset_seconds, m.value::string keyword
from mention_vaccine n
join video_latest v on v.video_id = n.video_id
, table (flatten(matches)) m
where v.views > 10000 and v.upload_date >= '2021-04-13'
"
      },
      {
        "vaccine-personal",
        @"select n.video_id, 'cation' as part, caption as context, offset_seconds, '' keyword from mention_vaccine_personal n"
      }
    };
  }

  public record AtLabel(AirtableCfg AirCfg, SnowflakeConnectionProvider Sf) {
    public async Task MargeIntoAirtable(AtOps op, ILogger log) {
      using var db = await Sf.Open(log);

      await db.Execute("create tmp mentions table", $@"
create or replace temporary table _mentions as 
(
  with q as ({AtLabelSql.NamedQuery(op.Name)}) 
  select * from q
  {op.Videos.Do(vids => $"where video_id in ({vids.Join(", ", v => v.SingleQuote())})")}
  {op.Limit.Do(l => $"limit {l}")}
)");

      var mentionSql = "select * from _mentions";

      if (op.Parts.ShouldRun(AtLabelPart.Channel))
        await Sync<ChannelRowKey>(op, "Channels", db.ReadAsJson("narrative channels", @$"
  with mention as ({mentionSql})
  select c.channel_id, c.channel_title, c.subs, c.channel_views , c.tags
from channel_latest c
    where exists(select * from mention n join video_latest v on v.video_id = n.video_id where v.channel_id = c.channel_id)
  "), log);

      if (op.Parts.ShouldRun(AtLabelPart.Video))
        await Sync<VideoRowKey>(op, "Videos", db.ReadAsJson("narrative channels", @$"
  with mention as ({mentionSql})
  select video_id, video_title, views, channel_id from video_latest v
    where exists(select * from mention m where m.video_id = v.video_id)
  ").Select(r => {
          // linking records need to ba an array
          r["CHANNEL_ID"] = new JArray(r["CHANNEL_ID"]);
          return r;
        }), log);

      if (op.Parts.ShouldRun(AtLabelPart.Mention))
        await Sync<MentionRowKey>(op, "Mentions", db.ReadAsJson("narrative mentions", @$"
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
  , substr(md5(n.video_id), 0, 5) || ' - ' || v.channel_title || ' - ' || v.video_title as video_group
  from mention n
         join video_latest v on v.video_id=n.video_id
qualify row_number() over (partition by mention_id order by 1) = 1
order by video_group -- use group to randomize the order
  ").Select(r => {
          // linking records need to ba an array
          r["CHANNEL_ID"] = new JArray(r["CHANNEL_ID"]);
          r["VIDEO_ID"] = new JArray(r["VIDEO_ID"]);
          return r;
        }), log);
    }
    
    const int AtBatchSize = 10;

    public async Task Sync<TKey>(AtOps op, string airTableName, IAsyncEnumerable<JObject> sourceRows, ILogger log) where TKey : class {
      using var airTable = new AirtableBase(AirCfg.ApiKey, op.BaseId);
      var keyFields = typeof(TKey).GetProperties().Select(p => p.Name).ToArray();
      var airRows = await airTable.Rows<TKey>(airTableName, keyFields, log).ToListAsync()
        .Then(rows => rows.ToKeyedCollection(r => r.Fields));
      
      var (update, create) = await sourceRows
        .Select(v => v.ToCamelCase())
        .Select(r => new {Key = r.ToObject<TKey>(), Row = r, AirFields = r.ToAirFields()})
        .Split(r => airRows.ContainsKey(r.Key));
      
      await create.Batch(AtBatchSize).BlockAction(async (rows, i) => {
        var createFields = rows.Select(r => r.AirFields).ToArray();
        var res = await airTable.CreateMultipleRecords(airTableName, createFields, typecast: true);
        res.EnsureSuccess(log, airTableName);
        log.Information("Airtable - created {Rows} in {Airtable}, batch {Batch}", createFields.Length, airTableName, i + 1);
      });

      if (op.Mode.In(AtUpdateMode.CreateAndUpdate)) {
        await update.Batch(AtBatchSize).BlockAction(async (rows, i) => {
          var updateFields = rows.Select(u => new IdFields(airRows[u.Key].Id) {FieldsCollection = u.AirFields.FieldsCollection}).ToArray();
          var res = await airTable.UpdateMultipleRecords(airTableName, updateFields, typecast: true);
          res.EnsureSuccess(log, airTableName);
          log.Information("Airtable - updated {Rows} rows in {Airtable}, batch {Batch}", updateFields.Length, airTableName, i + 1);
        });
      }
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