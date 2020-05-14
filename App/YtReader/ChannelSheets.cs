using System;
using System.Collections.Generic;
using System.Globalization;
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
using YtReader.Store;

namespace YtReader {
  public static class ChannelSheets {
    #region Write

    public static async Task SyncNewChannelsForTags(SheetsCfg sheetsCfg, string mainChannelSheetId, IReadOnlyCollection<string> sheetIds) {
      var service = GetService(sheetsCfg);
      var mainSheet = await service.Spreadsheets.Get(mainChannelSheetId).ExecuteAsync();
      var channelSheet = mainSheet.Sheets.FirstOrDefault(s => s.Properties.Title == "Channels");
      var lastRow = channelSheet.Data.Max(d => d.StartRow);
    }

    #endregion

    static SheetsService GetService(SheetsCfg sheetsCfg) {
      //var creds = new ClientSecrets {ClientId = sheetsCfg.Creds.Name, ClientSecret = sheetsCfg.Creds.Secret};
      var creds = GoogleCredential.FromJson(sheetsCfg.CredJson.ToString()).CreateScoped(SheetsService.Scope.SpreadsheetsReadonly);
      var service = new SheetsService(new BaseClientService.Initializer {
        HttpClientInitializer = creds,
        ApplicationName = Setup.AppName
      });
      return service;
    }

    static string Quote(this string value) {
      if (value.NullOrEmpty()) return value;
      return "\"" + value.Replace("\"", "\\\"") + "\"";
    }

    static IReadOnlyCollection<T> RangeWithHeaderToClass<T>(ICollection<IList<object>> range, ILogger log) {
      if (range.Count == 0) return new T[] { };
      var csvText = range.Join("\n", l => l.Join(",", o => Quote(o?.ToString() ?? "")));
      using var reader = new StringReader(csvText);
      using var csv = new CsvReader(reader, CultureInfo.InvariantCulture);
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

    #region Read

    public static Task<IReadOnlyCollection<MainChannelSheet>> MainChannels(SheetsCfg sheetsCfg, ILogger log) =>
      MainChannels(sheetsCfg, GetService(sheetsCfg), log);

    static async Task<IReadOnlyCollection<MainChannelSheet>> MainChannels(SheetsCfg sheetsCfg, SheetsService service, ILogger log) =>
      await SheetValues<MainChannelSheet>(service, sheetsCfg.MainChannelSheetId, "Channels", log);

    public static async Task<IReadOnlyCollection<ChannelSheet>> Channels(SheetsCfg sheetsCfg, ILogger log) {
      var service = GetService(sheetsCfg);
      var userChannelSheets = await sheetsCfg.UserChannelSheetIds
        .Select((v, i) => new
          {SheetId = v, Weight = 1}) // I used to weight some users higher (that's why there is that logic). Now we weight them all the same (1)
        .BlockFunc(async s => new {
          Channels = await SheetValues<UserChannelSheet>(service, s.SheetId, "Channels", log)
            .WithWrappedException($"eror reading user sheet {s.SheetId}", log),
          s.SheetId,
          s.Weight
        });

      var userChannelsById = userChannelSheets
        .SelectMany(u => u.Channels.Select(c => new {u.SheetId, u.Weight, Channel = c}))
        .ToMultiValueDictionary(c => c.Channel.Id);

      var mainChannels = await MainChannels(sheetsCfg, service, log);
      var channels = mainChannels
        .Select(mc => {
            var ucs = userChannelsById[mc.Id].NotNull()
              .Where(uc => uc.Channel.Complete == "TRUE").ToList();

            var classifications = ucs.Select(uc =>
              new UserChannelStore2 {
                SoftTags = SoftTags(uc.Channel),
                Relevance = uc.Channel.Relevance,
                LR = uc.Channel.LR,
                SheetId = uc.SheetId,
                Notes = uc.Channel.Notes,
                Weight = uc.Weight
              }).ToList();

            var totalWeight = classifications.Sum(c => c.Weight);
            var distinctTags = classifications.SelectMany(t => t.SoftTags).Distinct();
            var softTags = distinctTags
              .Select(t => classifications.Sum(s => s.SoftTags.Contains(t) ? s.Weight : 0) > totalWeight / 2d ? t : null).NotNull().ToList();

            var res = new ChannelSheet {
              Id = mc.Id,
              Title = mc.Title,
              LR = AverageLR(ucs.Select(c => c.Channel.LR).ToList()),
              MainChannelId = mc.MainChannelId,
              HardTags = new[] {mc.HardTag1, mc.HardTag2, mc.HardTag3}.Where(t => t.HasValue()).OrderBy(t => t).ToList(),
              SoftTags = softTags,
              Relevance = ucs.Any() ? ucs.Average(s => s.Channel.Relevance) / 10d : 1,
              UserChannels = classifications
            };
            return res;
          }
        ).ToList();

      return channels;
    }

    static IReadOnlyCollection<string> SoftTags(UserChannelSheet sheet) =>
      new[] {sheet.SoftTag1, sheet.SoftTag2, sheet.SoftTag3, sheet.SoftTag4}
        .Where(t => t.HasValue()).ToList();

    static string AverageLR(IReadOnlyCollection<string> lrs) {
      double? LrNum(string lr) => lr switch {
        "L" => -1,
        "R" => 1,
        "C" => 0,
        _ => null
      };

      if (!lrs.Any()) return null;

      var vg = Math.Round(lrs.Select(LrNum).NotNull().Select(v => v.Value).Average(), 0);
      var lr = vg switch {
        -1 => "L",
        0 => "C",
        1 => "R",
        _ => throw new InvalidOperationException("rounded to outside expected bounds")
      };
      return lr;
    }

    #endregion
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

  /// <summary>Main & User sheet combined into a record representing the majority view</summary>
  public class ChannelSheet {
    public string                                 Id            { get; set; }
    public string                                 Title         { get; set; }
    public double                                 Relevance     { get; set; } // between 0 and 1
    public string                                 LR            { get; set; }
    public string                                 MainChannelId { get; set; }
    public IReadOnlyCollection<string>            HardTags      { get; set; } = new List<string>();
    public IReadOnlyCollection<string>            SoftTags      { get; set; } = new List<string>();
    public IReadOnlyCollection<UserChannelStore2> UserChannels  { get; set; } = new List<UserChannelStore2>();
  }
}