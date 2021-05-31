using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Mutuo.Etl.Blob;
using Newtonsoft.Json.Linq;
using Semver;
using Serilog;
using SysExtensions;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YtReader.Store {
  public class StoreUpgrader {
    static DateTime V0UpdateTime;

    public static readonly SPath            VersionFile = new("_version.json");
    readonly               AppCfg           Cfg;
    readonly               ILogger          Log;
    readonly               ISimpleFileStore Store;

    public StoreUpgrader(AppCfg cfg, ISimpleFileStore store, ILogger log) {
      Cfg = cfg;
      Store = store;
      Log = log;
    }

    public Task UpgradeIfNeeded() => throw new NotImplementedException("this is untested/not finished. Complete next time we need to upgrade");

    async Task Upgrade() {
      var versionFile = await Store.Info(VersionFile);
      var md = versionFile == null ? new() {Version = new(0)} : await Store.Get<StoreMd>(VersionFile);

      var upgradeMethods = GetType().GetMethods()
        .Select(m => (m, a: m.GetCustomAttribute<UpgradeAttribute>())).Where(m => m.a != null)
        .Select(m => (m.m.Name, Upgrade: new Func<Task>(async () => {
          Log.Information("Upgrade {Name} - started", m.m.Name);
          var sw = Stopwatch.StartNew();
          await (Task) m.m.Invoke(this, new object[] { });
          Log.Information("Upgrade {Name} - completed in {Duration}", m.m.Name, sw.Elapsed.HumanizeShort());
        }), Version: SemVersion.Parse(m.a.Version)));

      var toRun = upgradeMethods
        .Where(m => m.Version > md.Version || m.Version == md.Version && !md.Ran.Contains(m.Name))
        .OrderBy(m => m.Version).ToArray();

      foreach (var run in toRun) {
        await run.Upgrade();
        md.Ran.Add(run.Name);
        md.Version = run.Version;
        await Save(md);
      }
    }

    Task Save(StoreMd md) => Store.Set(VersionFile, md);

    [Upgrade("0.1")] Task AddVersionFile() => Task.CompletedTask;

    [Upgrade("0")]
    async Task UpdateVids_0to1() {
      var filesToUpgrade = await FilesToUpgrade("videos", fromVersion: 0);
      await filesToUpgrade.BlockDo(async f => {
        var existingJs = await Jsonl(f);
        var upgradedJs = existingJs.Select(j => {
          var newJ = j.DeepClone();
          newJ["Updated"] = V0UpdateTime;
          return newJ;
        }).ToList();
        var newPath = NewFilePath(f, version: 1);
        await ReplaceJsonLFile(f, newPath, upgradedJs);
      }, Cfg.DefaultParallel);
    }

    [Upgrade("0")]
    async Task UpdateRecs_0to1() {
      var toUpgrade = await FilesToUpgrade("recs", fromVersion: 0);
      V0UpdateTime = DateTime.Parse("2019-11-02T13:50:00Z").ToUniversalTime();
      await toUpgrade.BlockDo(async f => {
        var existingJs = await Jsonl(f);
        var upgradedJs = existingJs.GroupBy(j => j["FromVideoId"].Value<string>()).SelectMany(g => {
          return g.Select((j, i) => {
            var newJ = j.DeepClone();
            newJ["Updated"] = V0UpdateTime;
            newJ["Rank"] = i + 1;
            return newJ;
          });
        });
        var newPath = JsonlStoreExtensions.FilePath(f.Path.Parent, V0UpdateTime.FileSafeTimestamp(), "1");
        await ReplaceJsonLFile(f, newPath, upgradedJs);
      }, Cfg.DefaultParallel);
    }

    [Upgrade("0")]
    async Task UpdateCaptions_0to1() {
      var toUpgrade = await FilesToUpgrade("captions", fromVersion: 0);
      await toUpgrade.BlockDo(async f => {
        var js = await Jsonl(f);
        foreach (var j in js) j["Updated"] = V0UpdateTime;
        await ReplaceJsonLFile(f, NewFilePath(f, version: 1), js);
      }, parallel: 4);
    }

    static SPath NewFilePath(StoreFileMd f, int version) =>
      JsonlStoreExtensions.FilePath(f.Path.Parent, StoreFileMd.GetTs(f.Path), version.ToString());

    async Task<IReadOnlyCollection<JObject>> Jsonl(StoreFileMd f) {
      await using var sr = await Store.Load(f.Path);
      var existingJs = sr.LoadJsonlGz<JObject>();
      return existingJs;
    }

    async Task<List<StoreFileMd>> FilesToUpgrade(SPath path, int fromVersion) {
      var files = (await Store.List(path, allDirectories: true).SelectManyList()).Select(StoreFileMd.FromFileItem).ToList();
      var toUpgrade = files.Where(f => (f.Version ?? "0").ParseInt() == fromVersion).ToList();
      return toUpgrade;
    }

    async Task ReplaceJsonLFile(StoreFileMd f, SPath newPath, IEnumerable<JToken> upgradedJs) {
      using var stream = await upgradedJs.ToJsonlGzStream();
      await Store.Save(newPath, stream);
      var deleted = await Store.Delete(f.Path);
      if (!deleted) throw new InvalidOperationException($"Didn't delete old file {f.Path}");
      Log.Information("Upgraded {OldFile} to {File}", f.Path, newPath);
    }
  }

  [AttributeUsage(AttributeTargets.Method, Inherited = false)]
  sealed class UpgradeAttribute : Attribute {
    public readonly string Version;

    public UpgradeAttribute(string version) => Version = version;
  }

  public class StoreMd {
    public SemVersion      Version { get; set; }
    public HashSet<string> Ran     { get; set; } = new();
  }
}