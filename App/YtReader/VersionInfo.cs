using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Semver;
using Serilog;
using SysExtensions.Build;
using SysExtensions.Text;
using SysExtensions.Threading;

namespace YtReader {
  public class VersionInfo {
    public VersionInfo(SemVersion version, GitVersionInfo info) {
      Version = version;
      Info = info;
    }

    public SemVersion     Version     { get; set; }
    public GitVersionInfo Info        { get; set; }
    public SemVersion     ProdVersion => Version.Change(prerelease: "");
  }

  public class VersionInfoProvider {
    readonly Defer<VersionInfo> LazyVersion;

    public VersionInfoProvider(ILogger log, RootCfg rootCfg) =>
      LazyVersion = new Defer<VersionInfo>(async () => {
        var (version, info) = await GitVersionInfo.DiscoverVersion(typeof(VersionInfo), log);
        var prefix = GetVersionPrefix(rootCfg, version, info);
        version = version.Change(prerelease: prefix);
        return new VersionInfo(version, info);
      });

    static readonly Regex NonAlphaNum = new Regex("[^a-zA-Z0-9]", RegexOptions.Compiled);

    public static string GetVersionPrefix(RootCfg rootCfg, SemVersion version, GitVersionInfo info = null) {
      if (rootCfg.IsProd()) return rootCfg.BranchEnv ?? "";
      var prerelease = version.Prerelease.HasValue() ? version.Prerelease : null;
      var prefix = rootCfg.BranchEnv ?? info?.BranchName ?? prerelease ?? rootCfg.Env.ToLower();
      prefix = NonAlphaNum.Replace(prefix, "");
      return prefix;
    }

    public Task<VersionInfo> Version() => LazyVersion.GetOrCreate();
  }
}