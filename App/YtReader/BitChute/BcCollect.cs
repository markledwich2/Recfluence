using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Reflection;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using static YtReader.BitChute.BcCollectPart;
using static YtReader.BitChute.BcLinkType;

namespace YtReader.BitChute {
  public enum BcLinkType {
    Video,
    Channel
  }

  public enum BcCollectPart {
    Channel,
    DiscoverChannels,
    Video
  }

  public class BcCollect {
    readonly SnowflakeConnectionProvider Sf;
    readonly BcWeb                       Web;
    readonly BitChuteCfg                 Cfg;
    readonly YtStore                     Db;

    public BcCollect(YtStores stores, SnowflakeConnectionProvider sf, BcWeb web, BitChuteCfg cfg, ILogger log) {
      Db = new(stores.Store(DataStoreType.Db), log);
      Sf = sf;
      Web = web;
      Cfg = cfg;
    }

    public async Task Collect(string[] channels, BcCollectPart[] parts, ILogger log, CancellationToken cancel) {
      var channelsToUpdate = new HashSet<string>();
      
      // TODO get channels from the DB
      
      if(channels != null) channelsToUpdate.AddRange(channels);
      if (channels == null && parts.ShouldRun(DiscoverChannels)) {
        // TODO: check that these do not exist in the DB
        var qDesc = await GetQDescription(log);
        var links = qDesc.SelectMany(d => ChanLink.Matches(d.description).Select(m => new {
          LinkType = m.Groups["type"].Value.TryParseEnum<BcLinkType>(out var t) ? t : (BcLinkType?) null,
          LinkId = m.Groups["id"].Value,
          ChannelId = d.channelId,
        }));
        channelsToUpdate.AddRange(links.Where(l => l.LinkType == BcLinkType.Channel).Select(l => l.LinkId));
      }

      await channelsToUpdate.Take(100).BlockAction(async c => {
        var ((chan, getVideos), ex) = await Def.F(() => Web.ChannelAndVideos(c, log)).Try();
        if (ex != null) {
          log.Warning(ex, "Unable to load channel {c}: {Message}", c, ex.Message);
          return;
        }

        await Db.BcChannels.Append(chan.InArray());
        log.Information("BcCollect - saved {Channel}: {@Channel}", chan.ChannelTitle, chan);

        var videos = await getVideos.SelectManyList();
        await Db.BvVideos.Append(videos);
        log.Information("BcCollect - saved {Videos} videos for {Channel}", videos.Count, chan.ChannelTitle);
      }, Cfg.CollectParallel, cancel:cancel); //Cfg.DefaultParallel
    }

    async Task<(string description, string channelId)[]> GetQDescription(ILogger log) {
      using var db = await Sf.OpenConnection(log);
      return (await db.Query<(string description, string channelId)>("", @"
with q as (
  select v.description, v.channel_id
  from video_latest v
left join channel_latest c on v.channel_id = c.channel_id
where array_contains('QAnon'::variant, tags) and v.description like '%bitchute.com%'
)
select * from q
union all
select description, channel_id from channel_latest
where array_contains('QAnon'::variant, tags) and description like '%bitchute.com%'
")).ToArray();
    }

    static readonly Regex ChanLink = new(@"bitchute\.com\/((?<type>channel|video|accounts)\/)?(?<id>[^\/\s]*)\/");
  }
}