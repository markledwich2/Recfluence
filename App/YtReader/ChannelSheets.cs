using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using CsvHelper;
using CsvHelper.Configuration.Attributes;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Services;
using Google.Apis.Sheets.v4;
using Serilog;
using SysExtensions.Collections;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YtReader {
  public static class ChannelSheets {
    static string Quote(this string value) {
      if (value.NullOrEmpty()) return value;
      return "\"" + value.Replace("\"", "\\\"") + "\"";
    }

    static IReadOnlyCollection<T> RangeWithHeaderToClass<T>(ICollection<IList<object>> range, ILogger log) {
      if (range.Count == 0) return new T[] { };
      var csvText = range.Join("\n", l => l.Join(",", o => Quote(o?.ToString() ?? "")));
      using var reader = new StringReader(csvText);
      using var csv = new CsvReader(reader);
      csv.Configuration.HasHeaderRecord = true;
      csv.Configuration.MissingFieldFound = null;
      csv.Configuration.Escape = '\\';
      csv.Configuration.BadDataFound = c => log.Warning("Error reading csv data at {RowNumber}: {RowData}", c.Row, c.RawRecord);
      csv.Configuration.LineBreakInQuotedFieldIsBadData = false;
      var rows = csv.GetRecords<T>().ToList();
      return rows;
    }

    static async Task<IReadOnlyCollection<T>> SheetValues<T>(SheetsService service, string sheetId, string range, ILogger log) {
      var request = service.Spreadsheets.Values.Get(sheetId, range);
      var res = await request.ExecuteAsync();
      return RangeWithHeaderToClass<T>(res.Values, log).ToList();
    }

    static async Task<IReadOnlyCollection<MainChannelSheet>> MainChannels(SheetsCfg sheetsCfg, SheetsService service, ILogger log) =>
      await SheetValues<MainChannelSheet>(service, sheetsCfg.MainChannelSheetId, "Channels", log);

    public static async Task<IReadOnlyCollection<ChannelWithUserData>> Channels(SheetsCfg sheetsCfg, ILogger log) {
      var service = GetService(sheetsCfg);
      var userChannelSheets = await sheetsCfg.UserChannelSheetIds
        .Select((v, i) => new {SheetId = v, Weight = 1 - i / 100d})
        .BlockTransform(async s => new {
          Channels = await SheetValues<UserChannelSheet>(service, s.SheetId, "Channels", log),
          s.SheetId,
          s.Weight
        }, 4);

      var userChannelsById = userChannelSheets
        .SelectMany(u => u.Channels.Select(c => new {u.SheetId, u.Weight, Channel = c}))
        .ToMultiValueDictionary(c => c.Channel.Id);

      var mainChannels = await MainChannels(sheetsCfg, service, log);
      var channels = mainChannels
        .Select(mc => {
            var ucs = userChannelsById[mc.Id]
              .Where(uc => uc.Channel.Complete == "TRUE").ToList();

            var allSoftTags = ucs.Select(c =>
              new {
                Tags = new[] {c.Channel.SoftTag1, c.Channel.SoftTag2, c.Channel.SoftTag3, c.Channel.SoftTag4}.Where(t => t.HasValue()).ToList(),
                c.Weight
              }).ToList();
            var totalWeight = allSoftTags.Sum(t => t.Weight);
            var distinctTags = allSoftTags.SelectMany(t => t.Tags).Distinct();
            var softTags = distinctTags.Select(t => allSoftTags.Sum(s => s.Tags.Contains(t) ? s.Weight : 0) > totalWeight / 2d ? t : null).NotNull().ToList();

            var res = new ChannelWithUserData {
              Id = mc.Id,
              Title = mc.Title,
              LR = MajorityValue(ucs, c => c.Channel.LR, c => c.Weight),
              MainChannelId = mc.MainChannelId,
              HardTags = new[] {mc.HardTag1, mc.HardTag2, mc.HardTag3}.Where(t => t.HasValue()).OrderBy(t => t).ToList(),
              SoftTags = softTags,
              SheetIds = ucs.Select(s => s.SheetId).ToList(),
              Relevance = ucs.Average(s => s.Channel.Relevance) / 10d
            };
            return res;
          }
        ).ToList();

      return channels;
    }

    static V MajorityValue<T, V>(IEnumerable<T> items, Func<T, V> getValue, Func<T, double> getWeight) =>
      items.GroupBy(getValue)
        .Select(g => new {Wieght = g.Sum(getWeight), Value = g.Key})
        .OrderByDescending(g => g.Wieght)
        .Select(i => i.Value).FirstOrDefault();

    static SheetsService GetService(SheetsCfg sheetsCfg) {
      //var creds = new ClientSecrets {ClientId = sheetsCfg.Creds.Name, ClientSecret = sheetsCfg.Creds.Secret};
      var creds = GoogleCredential.FromJson(sheetsCfg.CredJson.ToString()).CreateScoped(SheetsService.Scope.SpreadsheetsReadonly);
      var service = new SheetsService(new BaseClientService.Initializer {
        HttpClientInitializer = creds,
        ApplicationName = Setup.AppName
      });
      return service;
    }
  }

  public class MainChannelSheet {
    public                           string Id            { get; set; }
    public                           string Title         { get; set; }
    [Name("Main Channel ID")] public string MainChannelId { get; set; }
    public                           string HardTag1      { get; set; }
    public                           string HardTag2      { get; set; }
    public                           string HardTag3      { get; set; }
  }

  public class UserChannelSheet {
    public string Id        { get; set; } = default!;
    public string LR        { get; set; }
    public int    Relevance { get; set; }
    public string SoftTag1  { get; set; }
    public string SoftTag2  { get; set; }
    public string SoftTag3  { get; set; }
    public string SoftTag4  { get; set; }

    [Name("Notes and Video Links")] public string Notes    { get; set; }
    public                                 string Complete { get; set; }
  }

  public class ChannelWithUserData {
    public string                      Id            { get; set; }
    public string                      Title         { get; set; }
    public double                      Relevance     { get; set; } // between 0 and 1
    public string                      LR            { get; set; }
    public string                      MainChannelId { get; set; }
    public IReadOnlyCollection<string> HardTags      { get; set; } = new List<string>();
    public IReadOnlyCollection<string> SoftTags      { get; set; } = new List<string>();
    public IReadOnlyCollection<string> SheetIds      { get; set; } = new List<string>();
  }
}