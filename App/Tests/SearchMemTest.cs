using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using Humanizer;
using NUnit.Framework;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Threading;
using YtReader;
using YtReader.Db;
using YtReader.Search;
using DbCaption = YtReader.Search.DbCaption;

namespace Tests {
  public class SearchMemTest {
    [Test]
    public async Task LoadAllCaptionsFromProd() {
      var basePath = Setup.SolutionDir?.Combine("YtCli").ToString() ?? throw new InvalidOperationException("Expecting solution file for loading settings");
      var (app, root, version) = await Setup.LoadCfg(rootLogger: Setup.ConsoleLogger(), basePath: basePath);

      app.Snowflake.DbSuffix = null;
      var log = Setup.CreateTestLogger();
      var scope = Setup.MainScope(root, app, Setup.PipeAppCtxEmptyScope(root, app, version.Version), version, log);
      var db = scope.Resolve<SnowflakeConnectionProvider>();
      using var conn = await db.OpenConnection(log);
      await conn.SetSessionParams((SfParam.ClientPrefetchThreads, 2));

      var sql = "select * from caption";

      var res = await conn.QueryBlocking<DbCaption>("select all captions", sql)
        .SelectMany(SelectCaptions)
        .Batch(5_000).WithIndex()
        .BlockFunc(async b => {
          await 200.Milliseconds().Delay();
          log.Information("Pretended to upload batch {Batch}", b.index);
          return b.item.Count;
        }, parallel: 2, capacity: 2);
    }

    static IEnumerable<DbCaption> SelectCaptions(DbCaption dbCaption) {
      static DbCaption CreatePart(DbCaption c, CaptionPart part) {
        var partCaption = c.JsonClone();
        partCaption.caption_id = $"{c.video_id}|{part.ToString().ToLowerInvariant()}";
        partCaption.caption = part switch {
          CaptionPart.Title => c.video_title,
          CaptionPart.Description => c.description,
          CaptionPart.Keywords => c.keywords,
          _ => throw new NotImplementedException($"can't create part `{part}`")
        };
        partCaption.part = part;
        return partCaption;
      }

      // instead of searching across title, description, captions. We create new caption records for each part
      var caps = dbCaption.caption_group == 0
        ? new[] {
          dbCaption,
          CreatePart(dbCaption, CaptionPart.Title),
          CreatePart(dbCaption, CaptionPart.Description),
          CreatePart(dbCaption, CaptionPart.Keywords)
        }
        : new[] {dbCaption};
      foreach (var newCap in caps) {
        // even tho we use EsCaption  in the NEST api, it will still serialize and store instance properties. Remove the extra ones
        newCap.keywords = null;
        newCap.description = null;
      }
      return caps;
    }
  }
}