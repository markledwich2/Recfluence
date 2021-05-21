using System.Threading.Tasks;
using Autofac;
using NUnit.Framework;
using YtReader.Db;

namespace Tests {
  public class DbTests {
    record VideoResult(string video_id);

    [Test]
    public static async Task TestSfQuery() {
      using var ctx = await TestSetup.TextCtx();
      var conn = ctx.Scope.Resolve<SnowflakeConnectionProvider>();
      using var db = await conn.Open(ctx.Log);
      var res = await db.Query<VideoResult>("test", "select video_id from video_latest limit 10");
    }
  }
}