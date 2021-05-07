using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Flurl.Http;
using Mutuo.Etl.Blob;
using Mutuo.Etl.Db;
using Newtonsoft.Json.Linq;
using Serilog;
using Snowflake.Data.Client;
using SysExtensions;
using SysExtensions.Collections;
using SysExtensions.Serialization;
using SysExtensions.Text;
using SysExtensions.Threading;
using YtReader.Db;
using YtReader.Store;
using static System.Array;
using static YtReader.StandardCollectPart;
using static YtReader.Store.ChannelSourceType;
using ChanById = SysExtensions.Collections.IKeyedCollection<string, YtReader.Store.Channel>;

namespace YtReader {
  [AttributeUsage(AttributeTargets.Field)]
  public class CollectPartAttribute : Attribute {
    /// <summary>When true, the part will only run if included explicitly</summary>
    public bool Explicit { get; init; }
  }

  public enum StandardCollectPart {
    ExistingChannel,
    [CollectPart(Explicit = true)] Discover,
    [CollectPart(Explicit = true)] DiscoverFromVideo,
    Video
  }

  public enum LinkType {
    Channel,
    Video
  }

  public record DiscoverChannelOrVid(LinkType LinkType, string LinkId, Platform FromPlatform);

  public record SimpleCollectOptions {
    public string[]              ExplicitVideos   { get; set; }
    public string[]              ExplicitChannels { get; init; }
    public StandardCollectPart[] Parts            { get; init; }
  }

  public record SimpleCollectCtx(SimpleCollectOptions Options, IScraper Web, ILogger Log, CancellationToken Cancel) {
    public IReadOnlyCollection<DiscoverSource> VideosToCrawl { get; init; } = Empty<DiscoverSource>();
    public ChanById                            ToUpdate      { get; init; } = Empty<Channel>().ById();
    public ChanById                            Existing      { get; init; } = Empty<Channel>().ById();
    public Platform                            Platform      => Web.Platform;

    public string[]              ExplicitVideos   => Options.ExplicitVideos;
    public string[]              ExplicitChannels => Options.ExplicitChannels;
    public StandardCollectPart[] Parts            => Options.Parts;
  }

  public interface IScraper {
    Task<(Channel Channel, IAsyncEnumerable<Video[]> Videos)> ChannelAndVideos(string sourceId, ILogger log);
    Task<VideoExtra> Video(string sourceId, ILogger log);
    string SourceToFullId(string sourceId, LinkType type);
    int      CollectParallel { get; }
    Platform Platform        { get; }
  }

  public static class CollectExtensions {
    /// <summary>Creates a new channel with ChannelId set according to web's implementation. Also initialises updated</summary>
    public static Channel NewChan(this IScraper web, string sourceId) => new(web.Platform, web.SourceToFullId(sourceId, LinkType.Channel), sourceId)
      {Updated = DateTime.UtcNow};

    public static Video NewVid(this IScraper web, string sourceId) => new(web.Platform, web.SourceToFullId(sourceId, LinkType.Video), sourceId)
      {Updated = DateTime.UtcNow};

    public static VideoExtra NewVidExtra(this IScraper web, string sourceId) => new(web.Platform, web.SourceToFullId(sourceId, LinkType.Video), sourceId)
      {Updated = DateTime.UtcNow};

    public static ChanById ById(this IEnumerable<Channel> channels) => new KeyedCollection<string, Channel>(c => c.ChannelId, channels);

    public static DiscoverSource ToDiscoverSource(this DiscoverChannelOrVid l) => new(l.LinkType switch {
      LinkType.Channel => ChannelLink,
      LinkType.Video => VideoLink,
      _ => null
    }, l.LinkId, l.FromPlatform);

    static SimpleCollectCtx WithForUpdate(this SimpleCollectCtx ctx, string desc, IReadOnlyCollection<Channel> newChannels) {
      // add to update if it doesn't exist
      var actualNew = newChannels.NotNull().Where(c => !ctx.ToUpdate.ContainsKey(c.ChannelId)).ToArray();
      ctx.Log.Information("Collect {Platform} - planned {Channels} ({Desc}) channels for update", ctx.Platform, actualNew.Length, desc);
      return ctx with {ToUpdate = ctx.ToUpdate.Concat(actualNew).ById()};
    }

    public static async ValueTask<SimpleCollectCtx> PlanSimpleCollect(this SimpleCollectOptions options, SnowflakeConnectionProvider sf, IScraper web,
      ILogger log, CancellationToken cancel) {
      var ctx = new SimpleCollectCtx(options, web, log, cancel);

      var platform = ctx.Platform;
      var parts = ctx.Parts;

      if (ctx.ExplicitVideos != null)
        return ctx;

      using var db = await sf.Open(ctx.Log);

      ctx = ctx with {Existing = (await db.ExistingChannels(platform)).ById()};

      void WithUp(string desc, IReadOnlyCollection<Channel> channels) => ctx = ctx.WithForUpdate(desc, channels);

      if (ctx.Parts.ShouldRun(ExistingChannel)) // ad existing channels limit to explicit
        WithUp("existing", ctx.Existing.Where(c => c.ForUpdate(ctx.ExplicitChannels)).ToArray());

      // add explicit channel, no need to lookup existing, because that will already be in the list
      WithUp("explicit",
        ctx.ExplicitChannels.NotNull().Select(c => ctx.Web.NewChan(c) with {DiscoverSource = new(Manual, c)}).ToArray());

      if (!parts.ShouldRunAny(Discover, DiscoverFromVideo) || ctx.ExplicitChannels?.Any() == true) return ctx;
      var discovered = await db.DiscoverChannelsAndVideos(platform);

      if (parts.ShouldRun(Discover))
        WithUp("discovered", discovered.Where(d => d.LinkType == LinkType.Channel)
          .Select(selector: l => ctx.Web.NewChan(l.LinkId) with {DiscoverSource = l.ToDiscoverSource()}).ToArray());

      if (!parts.ShouldRun(DiscoverFromVideo)) return ctx;

      ctx = ctx with {
        VideosToCrawl = discovered.Where(d => d.LinkType == LinkType.Video)
          .Select(l => new DiscoverSource(VideoLink, l.LinkId, l.FromPlatform)).ToArray()
      };
      ctx.Log.Information("Collect {Platform} - planned {Videos} videos for crawl", platform, ctx.VideosToCrawl.Count);
      return ctx;
    }

    /// <summary>Scrape videos from the plan, save to the store, and append new channels to the plan</summary>
    public static async ValueTask<SimpleCollectCtx> CrawlVideoLinksToFindNewChannels(this SimpleCollectCtx ctx, JsonlStore<Video> store) {
      var log = ctx.Log;
      var crawledVideos = await ctx.VideosToCrawl.BlockTrans(async (discover, i) => {
        var video = await ctx.Web.Video(discover.LinkId, log)
          .Swallow(e => log.Error(e, "Collect {Platform} - error crawling video {Video}: {Error}", ctx.Platform, discover.LinkId, e.Message));
        log.Debug("Collect {Platform} - crawled video {VideoId} {Vid}/{Total}", ctx.Platform, video?.VideoId, i, ctx.VideosToCrawl.Count);
        return (discover, video);
      }, ctx.Web.CollectParallel).ToListAsync();

      await store.Append(crawledVideos.Select(r => r.video).NotNull().ToArray());
      log.Information("Collect {Platform} - saved {Videos} videos", ctx.Platform, crawledVideos.Count);

      var crawledChannels = crawledVideos.Where(v => v.video?.ChannelId.HasValue() == true)
        .Select(v => new Channel(ctx.Web.Platform, v.video.ChannelId, v.video.ChannelSourceId) {
          ChannelTitle = v.video.ChannelTitle,
          DiscoverSource = v.discover
        }).Distinct().ToArray();

      return ctx.WithForUpdate("video crawled channels", crawledChannels);
    }

    public static async ValueTask<SimpleCollectCtx> CollectChannelAndVideos(this SimpleCollectCtx ctx, YtStore store) {
      var log = ctx.Log;

      if (ctx.ExplicitVideos != null) {
        var videos = await ctx.ExplicitVideos.BlockTrans(v => ctx.Web.Video(v, log)).ToListAsync();
        await store.Videos.Append(videos);
      }

      await ctx.ToUpdate.BlockAction(async (c, i) => {
        var ((freshChan, getVideos), ex) = await Def.Fun(() => ctx.Web.ChannelAndVideos(c.SourceId, log)).Try();
        if (ex != null) {
          log.Warning(ex, "Collect {Platform} - Unable to load channel {c}: {Message}", ctx.Platform, c, ex.Message);
          return; // don't save exceptional case. Don't know if it is our problem or thiers
        }

        var chan = c.JsonMerge(freshChan, new() {
          MergeNullValueHandling = MergeNullValueHandling.Ignore,
          MergeArrayHandling = MergeArrayHandling.Union
        }); // merge in new values from the collect. Using merge array because we want alt SourceId's to accumulate

        await store.Channels.Append(chan, log);
        log.Information("Collect {Platform} - saved {Channel} {Num}/{Total}", ctx.Platform, chan.ToString(), i + 1, ctx.ToUpdate.Count);


        if (ctx.Parts.ShouldRun(StandardCollectPart.Video) && getVideos != null
          && chan.ForUpdate()) // check in case we discovered a channel but it doesn't have enough subs
        {
          var (videos, vEx) = await getVideos.SelectManyList().Try();
          if (vEx != null) {
            log.Warning(vEx, "Collect {Platform} - Unable to load videos for channel {Channel}: {Message}", ctx.Platform, chan.ToString(), vEx.Message);
            return;
          }
          await store.Videos.Append(videos);
          log.Information("Collect {Platform} - saved {Videos} videos for {Channel}", ctx.Platform, videos.Count, chan.ToString());
        }
      }, ctx.Web.CollectParallel, cancel: ctx.Cancel);
      return ctx;
    }

    public static bool ForUpdate(this Channel c, string[] explicitSourceIds = null) {
      var sourceIds = explicitSourceIds?.ToHashSet();
      var enoughSubs = c.Subs == null || c.Subs > 1000;
      var alive = c.Status.NotIn(ChannelStatus.NotFound, ChannelStatus.Blocked, ChannelStatus.Dead);
      return (sourceIds == null || sourceIds.Contains(c.SourceId)) && alive && enoughSubs;
    }

    public static async Task<IReadOnlyCollection<DiscoverChannelOrVid>> DiscoverChannelsAndVideos(this ILoggedConnection<SnowflakeDbConnection> db,
      Platform platform) => await db.Query<DiscoverChannelOrVid>("discover new channels and videos", $@"
select distinct link_type LinkType, link_id LinkId, platform_from FromPlatform
from link_detail l
       left join parler_posts p on l.post_id_from=p.id
where not link_found
  and platform_to=:platform
  -- for posts only take those that have Q tags
  and (post_id_from is null or arrays_overlap(array_construct('qanon','q','qanons','wwg1wga','wwg','thegreatawakening'),p.hashtags))
", new { platform=platform.EnumString()});

    public static async Task<IReadOnlyCollection<Channel>> ExistingChannels(this ILoggedConnection<SnowflakeDbConnection> db, Platform platform,
      IReadOnlyCollection<string> explicitIds = null) {
      var existing = await db.Query<string>(@"get channels", @$"
with latest as (
  -- use channel table as a filter for staging data. it filters out bad records.
  select channel_id, updated from channel_latest
  where platform=:platform
  {(explicitIds.HasItems() ? $"and channel_id in ({explicitIds.Join(",", c => $"'{c}'")})" : "")}
)
select s.v
from latest c join channel_stage s on s.v:ChannelId = c.channel_id and s.v:Updated = c.updated
", new { platform=platform.EnumString()});
      return existing.Select(e => e.ToObject<Channel>(IJsonlStore.JCfg)).ToArray();
    }
  }
}