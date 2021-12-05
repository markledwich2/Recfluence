using Mutuo.Etl.Db;
using Octokit;
using static System.Text.RegularExpressions.RegexOptions;

// ReSharper disable InconsistentNaming

namespace YtReader.Db;

public record RepoCfg {
  public string Owner { get; init; } = "markledwich2";
  public string Name  { get; init; } = "YouTubeNetworks_Dataform";
  public string Path  { get; init; } = "definitions";
  /// <summary>A personal access token with read access to the repo</summary>
  public string AccessToken { get;  init; }
  public string[] TableNames { get; init; }
}

/// <summary> Sync's dataform descriptions with table & view comments in snowflake. Dataform does this when you update a
///   table, but often you want to do this quickly without any data updates</summary>
public record DataformDescriptions(SnowflakeConnectionProvider sf) {
  readonly Regex DescRe = new(@"\s*config\s*{.*?description:\s*(?>'([^']*)'|`([^`]*)`)", Singleline | Compiled);

  public async Task Sync(RepoCfg repo, ILogger log) {
    repo = repo with { TableNames = repo.TableNames?.Select(t => t.ToLowerInvariant()).ToArray() };

    IKeyedCollection<string, (string table, string tableType, string dscription)> tableMd;
    using (var db = await sf.Open(log)) {
      var sql = $@"select table_name, table_type, comment
from information_schema.tables
where table_schema = '{sf.Cfg.Schema.ToUpperInvariant()}' and table_catalog = '{sf.Cfg.DbName().ToUpperInvariant()}'";
      tableMd = await db.QueryAsync<(string table, string tableType, string dscription)>("table schema", sql)
        .ToArrayAsync().Then(r => r.KeyBy(a => a.table.ToLowerInvariant()));
    }

    var git = GitClient(repo.AccessToken);
    var gitContent = git.Repository.Content;

    await Sqlx(repo, log)
      .BlockDo(async path => {
        var content = await GitRetry(() => gitContent.GetRawContent(repo.Owner, repo.Name, path), log).Then(s => s.ToStringFromUtf8());
        return ParseDataformMd(path, content);
      }, parallel: 8).NotNull()
      .Batch(20) // batch to reuse open connections
      .BlockDo(async batch => {
        using var db = await sf.Open(log);
        foreach (var (name, desc) in batch) {
          var md = tableMd[name.ToLowerInvariant()];
          if (desc.NullOrEmpty() || md == default) return;
          if (desc == md.dscription) log.Information("DataformDescriptions - {Table} up to date", md.table);
          var tableType = md.tableType == "VIEW" ? "view" : "table";
          await db.Execute("update comment",
            @$"comment on {tableType} {md.table.InDoubleQuote()} is {desc.SingleQuote()}");
          log.Information("DataformDescriptions - Updated comment on {Table}: {Comment}", md.table, desc);
        }
      }, parallel: 4);
  }

  public (string Name, string Description) ParseDataformMd(string path, string contents) {
    var description = DescRe.Match(contents).Groups.Values.Skip(1).FirstOrDefault(g => g.Success)?.Value;
    var name = NameFromPath(path);
    return (name, description);
  }

  static string NameFromPath(string path) => path.Split("/").Last().ToLowerInvariant()
    .Split(".").Dot(s => s.Length > 1 ? s.Take(s.Length - 1) : s).Join(".");

  async Task<TRes> GitRetry<TRes>(Func<Task<TRes>> req, ILogger log) {
    while (true) {
      var (res, ex) = await req.Try();
      if (ex != null) {
        if (ex is RateLimitExceededException limit) {
          var retryAfter = limit.GetRetryAfterTimeSpan();
          log.Information("DataformDescriptions - github rate limited. Waiting for {Dur}", retryAfter.HumanizeShort());
          await retryAfter.Delay();
        }
        else {
          throw ex;
        }
      }
      else {
        return res;
      }
    }
  }

  public async IAsyncEnumerable<string> Sqlx(RepoCfg repo, ILogger log) {
    var client = GitClient(repo.AccessToken);
    var items = await GitRetry(() => client.Repository.Content.GetAllContents(repo.Owner, repo.Name, repo.Path), log);
    foreach (var item in items.Where(f => f.Type == "file" && f.Name.Split(".").Last() == "sqlx"
               && (repo.TableNames == null || repo.TableNames.Contains(NameFromPath(f.Path)))))
      yield return item.Path;
    await foreach (var file in items.Where(f => f.Type == "dir")
                     .BlockDo(async d => await Sqlx(repo with { Path = d.Path }, log).ToArrayAsync(), parallel: 4)
                     .SelectMany())
      yield return file;
  }

  static GitHubClient GitClient(string pat) => new(new ProductHeaderValue("Recfluence")) { Credentials = new(pat) };
}