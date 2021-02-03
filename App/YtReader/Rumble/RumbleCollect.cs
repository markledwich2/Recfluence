using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Flurl;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using static YtReader.StandardCollectPart;

namespace YtReader.Rumble {
  public record RumbleCollect(RumbleWeb Web, BlobStores Stores, SnowflakeConnectionProvider Sf, RumbleCfg Cfg) {
    public async Task Collect(string[] explicitChannels, StandardCollectPart[] parts, ILogger log, CancellationToken cancel) {
      var toUpdate = new KeyedCollection<string, Channel>(s => s.ChannelId);

      // add to update if it doesn't exist
      void ToUpdate(string desc, IReadOnlyCollection<Channel> channels) {
        toUpdate.AddRange(channels.NotNull().Where(c => !toUpdate.ContainsKey(c.ChannelId)));
        log.Debug("RumbleCollect planned {Channel} ({Desc}) channels for update", channels.Count, desc);
      }

      {
        using var db = await Sf.Open(log);
        var existing = await db.ExistingChannels(Platform.Rumble, explicitChannels);
        ToUpdate("existing", existing);
        ToUpdate("explicit",
          explicitChannels.NotNull().Select(c => new Channel(Platform.Rumble, c) {DiscoverSource = new(ChannelSourceType.Manual, DestId: c)}).ToArray());

        if (parts.ShouldRun(DiscoverChannels) && explicitChannels?.Any() != true) {
          var discovered = await db.Query<(string channelPart, string fromChannelId)>("bitchute links", @"
with
  -- get all existing bitchute channels. we store attempts (even failed ones) in the discover_id part so we don't keep attempting failed ones
  existing as (
    select coalesce(discover_source:DestId::string, source_id) discover_id
         , discover_source: type::string discover_type
         , c.channel_id
    from channel_latest c
    where platform='Rumble'
      and discover_id is not null
  )

   , rumble_links as (
  select from_channel_id, p:id::string discover_channel_id, count(*) references
  from (
         select distinct l.channel_id from_channel_id, url
              -- we use the canonical url for rumble channel_id, so match on that
              , regexmatch(url,
                           'https?://(?:www\\.)?rumble\\.com/(?:(?<path>c|user|account|register|embed)/)?(?<id>[\\w-]*)/?$',
                           'i') p
         from links l
         where l.domain='rumble.com'
           and p is not null
       )
  where (p:path is null and not startswith(p:id, 'v') or p:path='c') and length(discover_channel_id)> 0
  group by 1,2
    qualify row_number() over (partition by discover_channel_id order by references desc)=1
)
select discover_channel_id, from_channel_id
from rumble_links l
       left join channel_latest c on l.from_channel_id=c.channel_id
       left join existing e on e.discover_id=discover_channel_id
where e.discover_id is null -- don't discover existing channels with the same id/name
");

          string ChannelUrl(string id) => RumbleWeb.RumbleDotCom.AppendPathSegments("c", id);

          ToUpdate("discovered",
            discovered.Select(l => new Channel(Platform.Rumble, ChannelUrl(l.channelPart))
              {DiscoverSource = new(ChannelSourceType.ChannelLink, l.fromChannelId, l.channelPart)}).ToArray());
        }
      }

      var store = new YtStore(Stores.Store(DataStoreType.DbStage), log);

      await toUpdate.WithIndex().BlockAction(async item => {
        var (c, i) = item;
        var ((freshChan, getVideos), ex) = await Def.F(() => Web.ChannelAndVideos(c.SourceId, log)).Try();
        if (ex != null) {
          log.Warning(ex, "Unable to load channel {c}: {Message}", c, ex.Message);
          freshChan = new() {Status = ChannelStatus.NotFound};
        }
        var chan = c.JsonMerge(freshChan); // keep existing values like DiscoverSource, but replace whatever comes from the web update

        await store.Channels.Append(chan, log);
        log.Information("RumbleCollect - saved {Channel} {Num}/{Total}", chan.ChannelTitle ?? chan.ChannelId, i + 1, toUpdate.Count);

        if (parts.ShouldRun(Video) && getVideos != null) {
          var videos = await getVideos.SelectManyList();
          await store.Videos.Append(videos);
          log.Information("RumbleCollect - saved {Videos} videos for {Channel}", videos.Count, chan.ChannelTitle ?? chan.ChannelId);
        }
      }, Cfg.CollectParallel, cancel: cancel); //Cfg.DefaultParallel
    }
  }
}