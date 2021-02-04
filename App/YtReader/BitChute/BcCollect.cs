using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using static YtReader.StandardCollectPart;

namespace YtReader.BitChute {
  public enum BcLinkType {
    Video,
    Channel
  }

  public class BcCollect {
    readonly SnowflakeConnectionProvider Sf;
    readonly BcWeb                       Web;
    readonly BitChuteCfg                 Cfg;
    readonly YtStore                     Db;

    public BcCollect(BlobStores stores, SnowflakeConnectionProvider sf, BcWeb web, BitChuteCfg cfg, ILogger log) {
      Db = new(stores.Store(DataStoreType.DbStage), log);
      Sf = sf;
      Web = web;
      Cfg = cfg;
    }

    public static Channel NewChan(string sourceId) => new() {
      Platform = Platform.BitChute,
      ChannelId = Platform.BitChute.FullId(sourceId),
      SourceId = sourceId
    };

    public async Task Collect(string[] explicitChannels, StandardCollectPart[] parts, ILogger log, CancellationToken cancel) {
      var toUpdate = new KeyedCollection<string, Channel>(s => s.ChannelId);

      // add to update if it doesn't exist
      void ToUpdate(string desc, IReadOnlyCollection<Channel> channels) {
        toUpdate.AddRange(channels.NotNull().Where(c => !toUpdate.ContainsKey(c.ChannelId)));
        log.Debug("BcCollect planned {Channel} ({Desc}) channels for update", channels.Count, desc);
      }

      {
        using var db = await Sf.Open(log);
        var existing = await db.ExistingChannels(Platform.BitChute, explicitChannels?.Select(n => Platform.BitChute.FullId(n)).ToArray());

        ToUpdate("existing", existing);
        ToUpdate("explicit", explicitChannels.NotNull().Select(c => NewChan(c) with {DiscoverSource = new(ChannelSourceType.Manual, DestId: c)}).ToArray());

        if (parts.ShouldRun(DiscoverChannels) && explicitChannels?.Any() != true) {
          var discovered = await db.Query<(string idOrName, string sourceId)>("bitchute links", @"
with
  -- get all existing bitchute channels. we store attempts (even failed ones) in the discover_id part
  existing as (
    select coalesce(discover_source:DestId::string, source_Id) discover_id
         , discover_source:type::string discover_type
            , c.channel_id
          from channel_latest c
          where platform='BitChute' and discover_id is not null
            )
          -- find bc channels from links
            , bc_links as (
            select l.channel_id from_channel_id
            , regexmatch(description,
            'bitchute.com/(?:(?<type>channel|video|profile|search|hashtag|accounts/\\w|accounts)/(?<id>[\\w_-]*)/|(?:(?<name>[\\w_-]*))/\\s)',
            'i') parts
            , count(*) references

            from links l
          where domain='bitchute.com' and parts:type = 'channel'
          group by from_channel_id, parts
          qualify row_number() over (partition by parts:id order by references desc)=1
            )
          select parts:id::string discover_channel_id, from_channel_id
          from bc_links l
            left join channel_latest c on l.from_channel_id=c.channel_id
          left join existing e on e.discover_id=parts:id
            where e.discover_id is null -- don't discover existing channels with the same id/name
");

          ToUpdate("discovered",
            discovered.Select(l => NewChan(l.idOrName) with {DiscoverSource = new(ChannelSourceType.ChannelLink, l.sourceId, l.idOrName)}).ToArray());
        }
      }

      await toUpdate.WithIndex().BlockAction(async item => {
        var (c, i) = item;
        var ((freshChan, getVideos), ex) = await Def.F(() => Web.ChannelAndVideos(c.SourceId, log)).Try();
        if (ex != null) {
          log.Warning(ex, "Unable to load channel {c}: {Message}", c, ex.Message);
          freshChan = new() {Status = ChannelStatus.NotFound};
        }

        var chan = c.JsonMerge(freshChan); // keep existing values like DiscoverSource, but replace whatever comes from the web update

        await Db.Channels.Append(chan, log);
        log.Information("BcCollect - saved {Channel} {Num}/{Total}", chan.ToString(), i + 1, toUpdate.Count);

        if (parts.ShouldRun(Video) && getVideos != null) {
          var videos = await getVideos.SelectManyList();
          await Db.Videos.Append(videos);
          log.Information("BcCollect - saved {Videos} videos for {Channel}", videos.Count, chan.ToString());
        }
      }, Cfg.CollectParallel, cancel: cancel); //Cfg.DefaultParallel
    }
  }
}