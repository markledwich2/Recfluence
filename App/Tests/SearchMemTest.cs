using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Autofac;
using Humanizer;
using NUnit.Framework;
using SysExtensions.Serialization;
using SysExtensions.Threading;
using YtReader;
using YtReader.Search;

namespace Tests {
  public class SearchMemTest {
    [Test]
    public async Task LoadAllCaptionsFromProd() {
      var basePath = Setup.SolutionDir?.Combine("YtCli").ToString() ?? throw new InvalidOperationException("Expecting solution file for loading settings");
      var (app, root, version) = await Setup.LoadCfg(rootLogger: Setup.ConsoleLogger(), basePath: basePath);

      app.Snowflake.Db = "yt";
      var log = Setup.CreateTestLogger();
      var scope = Setup.MainScope(root, app, Setup.PipeAppCtxEmptyScope(root, app), version, log);
      var db = scope.Resolve<SnowflakeConnectionProvider>();
      using var conn = await db.OpenConnection(log);

      await conn.Execute("session", "alter session set CLIENT_PREFETCH_THREADS = 2"); // reduce mem usage (default 4)

      var sql = "select * from caption";
      var allItems = await conn
        .QueryBlocking<DbCaption>("select all captions", sql)
        .SelectMany(SelectCaptions)
        .BlockBatch(async (b, i) => {
            await 10.Seconds().Delay();
            log.Information("Pretended to upload batch {Batch}", i);
          },
          5000, // lower mem pressure in smaller batches
          8,
          1); // low file parallelism to reduce mem
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