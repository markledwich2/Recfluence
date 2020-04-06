using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Mutuo.Etl.Blob;
using Newtonsoft.Json.Linq;
using Serilog;
using SysExtensions;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YtReader {
  public class StoreUpgrader {
    readonly AppCfg           Cfg;
    readonly ISimpleFileStore Store;
    readonly ILogger          Log;
    static   DateTime         V0UpdateTime;

    public StoreUpgrader(AppCfg cfg, ISimpleFileStore store, ILogger log) {
      Cfg = cfg;
      Store = store;
      Log = log;
    }

    public async Task UpgradeStore() {
      async Task Run(Task<int> upgrade, string desc) {
        Log.Information("Started upgrade of {desc}", desc);
        var res = await upgrade.WithDuration();
        Log.Information("Completed {desc} upgrade of {FileCount} in {Duration}", desc, res.Result, res.Duration);
      }

      await Run(UpdateVids_0to1(), "videos");
      await Run(UpdateRecs_0to1(), "recs");
      await Run(UpdateCaptions_0to1(), "captions");
    }

    async Task<int> UpdateVids_0to1() {
      var filesToUpgrade = await FilesToUpgrade("videos", 0);
      await filesToUpgrade.BlockAction(async f => {
        var existingJs = await Jsonl(f);
        var upgradedJs = existingJs.Select(j => {
          var newJ = j.DeepClone();
          newJ["Updated"] = V0UpdateTime;
          return newJ;
        }).ToList();
        var newPath = NewFilePath(f, 1);
        await ReplaceJsonLFile(f, newPath, upgradedJs);
      }, Cfg.DefaultParallel);

      return filesToUpgrade.Count;
    }

    async Task<int> UpdateRecs_0to1() {
      var toUpgrade = await FilesToUpgrade("recs", 0);
      V0UpdateTime = DateTime.Parse("2019-11-02T13:50:00Z").ToUniversalTime();
      await toUpgrade.BlockAction(async f => {
        var existingJs = await Jsonl(f);
        var upgradedJs = existingJs.GroupBy(j => j["FromVideoId"].Value<string>()).SelectMany(g => {
          return g.Select((j, i) => {
            var newJ = j.DeepClone();
            newJ["Updated"] = V0UpdateTime;
            newJ["Rank"] = i + 1;
            return newJ;
          });
        });
        var newPath = StoreFileMd.FilePath(f.Path.Parent, V0UpdateTime.FileSafeTimestamp(), "1");
        await ReplaceJsonLFile(f, newPath, upgradedJs);
      }, Cfg.DefaultParallel);

      return toUpgrade.Count;
    }

    async Task<int> UpdateCaptions_0to1() {
      var toUpgrade = await FilesToUpgrade("captions", 0);
      await toUpgrade.BlockAction(async f => {
        var js = await Jsonl(f);
        foreach (var j in js) j["Updated"] = V0UpdateTime;
        await ReplaceJsonLFile(f, NewFilePath(f, 1), js);
      }, 4);
      return toUpgrade.Count;
    }

    static StringPath NewFilePath(StoreFileMd f, int version) =>
      StoreFileMd.FilePath(f.Path.Parent, StoreFileMd.GetTs(f.Path), version.ToString());

    async Task<IReadOnlyCollection<JObject>> Jsonl(StoreFileMd f) {
      await using var sr = await Store.Load(f.Path);
      var existingJs = sr.LoadJsonlGz<JObject>();
      return existingJs;
    }

    async Task<List<StoreFileMd>> FilesToUpgrade(StringPath path, int fromVersion) {
      var files = (await Store.List(path, true).SelectManyList()).Select(StoreFileMd.FromFileItem).ToList();
      var toUpgrade = files.Where(f => (f.Version ?? "0").ParseInt() == fromVersion).ToList();
      return toUpgrade;
    }

    async Task ReplaceJsonLFile(StoreFileMd f, StringPath newPath, IEnumerable<JToken> upgradedJs) {
      await using var stream = upgradedJs.ToJsonlGzStream();
      await Store.Save(newPath, stream);
      var deleted = await Store.Delete(f.Path);
      if (!deleted) throw new InvalidOperationException($"Didn't delete old file {f.Path}");
      Log.Information("Upgraded {OldFile} to {File}", f.Path, newPath);
    }
  }
}