using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AirtableApiClient;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;

// ReSharper disable InconsistentNaming

namespace YtReader.Narrative {
  public record AirtableCfg(string ApiKey = null, string BaseId = "appwfe3XfYqxn7v7I");
  public record NarrativesCfg(string CovidAirtable = "Covid");
  public record VideoIdRow(string videoId);

  public record CovidNarrative(NarrativesCfg Cfg, AirtableCfg AirCfg, SnowflakeConnectionProvider Sf) {
    public async Task MargeIntoAirtable(ILogger log) {
      using var airTable = new AirtableBase(AirCfg.ApiKey, AirCfg.BaseId);
      var airRows = await airTable.Rows<VideoIdRow>(Cfg.CovidAirtable, new[] {"videoId"}).ToListAsync()
        .Then(rows => rows.ToKeyedCollection(r => r.Fields.videoId));
      using var db = await Sf.Open(log);
      var batchSize = 10;
      await db.ReadAsJson("covid narrative", @"
select n.video_id
     , n.video_title
     , n.channel_id
     , n.channel_title
     , n.views::number views
     , n.description
      , n.upload_date
     , arrayjoin(n.captions,object_construct('video_id', n.video_id)
  ,'\n','[{offset}](https://youtube.com/watch?v={video_id}&t={offset}) {caption}') captions
, e.error_type
, x.last_seen
from covid_narrative_review n
left join video_extra e on e.video_id = n.video_id 
left join video_error x on x.video_id = n.video_id
order by views desc
limit 1000")
        .Select(v => v.ToCamelCase())
        .Batch(batchSize).BlockAction(async (rows, i) => {
          var (update, create) = rows.Select(r => r.ToAirFields()).Split(r => airRows.ContainsKey(r.Value<string>("videoId")));

          if (create.Any()) {
            var res = await airTable.CreateMultipleRecords(Cfg.CovidAirtable, create.ToArray());
            res.EnsureSuccess();
            log.Information("CovidNarrative - created airtable records {Rows}, batch {Batch}", update.Count, i+1);
          }

          if (update.Any()) {
            var updateFields = update.Select(u => new IdFields(airRows[u.Value<string>("videoId")].Id) {FieldsCollection = u.FieldsCollection}).ToArray();
            var res = await airTable.UpdateMultipleRecords(Cfg.CovidAirtable, updateFields);
            res.EnsureSuccess();
            /*await update.BlockAction(async u => {
              var airRow = airRows[u.Value<string>("videoId")];
              var res = await airTable.UpdateRecord(Cfg.CovidAirtable, u, airRow.Id);
              res.EnsureSuccess();
            });*/
            log.Information("CovidNarrative - updated airtable records {Rows}, batch {Batch}", update.Count, i+1);
          }
        });
    }
  }

  public static class AirtableExtensions {
    public static async IAsyncEnumerable<AirtableRecord<T>> Rows<T>(this AirtableBase at, string table, string[] fields = null) {
      string offset = null;
      while (true) {
        var res = await at.ListRecords<T>(table, offset, fields);
        res.EnsureSuccess();
        foreach (var r in res.Records)
          yield return r;
        offset = res.Offset;
        if (offset == null) break;
      }
    }

    public static async IAsyncEnumerable<AirtableRecord> Rows(this AirtableBase at, string table, string[] fields = null) {
      string offset = null;
      while (true) {
        var res = await at.ListRecords(table, offset, fields);
        res.EnsureSuccess();
        foreach (var r in res.Records)
          yield return r;
        offset = res.Offset;
        if (offset == null) break;
      }
    }

    public static void EnsureSuccess(this AirtableApiResponse res) {
      if (!res.Success) throw res.AirtableApiError as Exception ?? new InvalidOperationException("Airtable unknown error");
    }

    public static Fields ToAirFields(this JObject j) {
      var dic = j.ToObject<Dictionary<string, object>>();
      var fields = new Fields {FieldsCollection = dic};
      return fields;
    }

    public static T Value<T>(this Fields fields, string field) => (T)fields.FieldsCollection[field];

    public static JObject RecordJObject(this AirtableRecord record) {
      var j = new JObject(new JProperty("id", record.Id), new JProperty("createdTime", record.CreatedTime));
      foreach (var field in record.Fields)
        j.Add(field.Key, JToken.FromObject(field.Value));
      return j;
    }
  }
}