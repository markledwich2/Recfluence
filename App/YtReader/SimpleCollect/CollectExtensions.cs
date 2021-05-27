using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Serilog;
using SysExtensions;
using SysExtensions.Collections;
using YtReader.Store;
using YtReader.Yt;
using static System.Array;
using static YtReader.Store.DiscoverSourceType;
using ChanById = SysExtensions.Collections.IKeyedCollection<string, YtReader.Store.Channel>;

namespace YtReader.SimpleCollect {
  [AttributeUsage(AttributeTargets.Field)]
  public class CollectPartAttribute : Attribute {
    /// <summary>When true, the part will only run if included explicitly</summary>
    public bool Explicit { get; init; }
  }

  public enum StandardCollectPart {
    Channel,
    [CollectPart(Explicit = true)] DiscoverHome,
    ChannelVideo,
    Extra
  }

  public enum LinkType {
    Channel,
    Video
  }

  public record DiscoverChannelOrVid(LinkType LinkType, string LinkId, Platform FromPlatform);

  public enum SimpleCollectMode {
    Standard,
    Dedupe
  }

  public record SimpleCollectOptions {
    public SimpleCollectMode     Mode             { get; init; }
    public string[]              ExplicitChannels { get; init; }
    public StandardCollectPart[] Parts            { get; init; }
    public Platform              Platform         { get; init; }
  }

  /// <summary>State & services for performing a collection of data from a video platform. Not serializable</summary>
  public record SimpleCollectPlan(SimpleCollectOptions Options) {
    public IReadOnlyCollection<DiscoverSource>         VideosToCrawl    { get; init; } = Empty<DiscoverSource>();
    public Platform                                    Platform         => Options.Platform;
    public string[]                                    ExplicitChannels => Options.ExplicitChannels;
    public StandardCollectPart[]                       Parts            => Options.Parts;
    public IKeyedCollection<string, ChannelUpdatePlan> ChannelPlans     { get; init; } = Empty<ChannelUpdatePlan>().KeyBy(c => c.Channel.ChannelId);
  }

  public interface IScraper {
    ICommonCollectCfg CollectCfg { get; }
    Task<(Channel Channel, IAsyncEnumerable<Video[]> Videos)> ChannelAndVideos(string sourceId, ILogger log);
    Task<(VideoExtra Video, VideoComment[] Comments)> VideoAndExtra(string sourceId, ILogger log);
    string SourceToFullId(string sourceId, LinkType type);
    int      CollectParallel { get; }
    Platform Platform        { get; }
    IAsyncEnumerable<Video[]> HomeVideos(ILogger log);
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
    public static ChanById BySourceId(this IEnumerable<Channel> channels) => new KeyedCollection<string, Channel>(c => c.SourceId, channels);

    public static DiscoverSource ToDiscoverSource(this DiscoverChannelOrVid l) => new(l.LinkType switch {
      LinkType.Channel => ChannelLink,
      LinkType.Video => VideoLink,
      _ => null
    }, l.LinkId, l.FromPlatform);

    public static SimpleCollectPlan WithAddedChannels(this SimpleCollectPlan plan, string desc, IEnumerable<Channel> newChannels, ILogger log) =>
      plan.WithAddedChannels(desc, newChannels.Select(c => c.AsPlan()), log);

    public static SimpleCollectPlan WithAddedChannels(this SimpleCollectPlan plan, string desc, IEnumerable<ChannelUpdatePlan> newChannels, ILogger log) {
      // add to update if it doesn't exist
      var actualNew = newChannels.NotNull().Where(c => !plan.ChannelPlans.ContainsKey(c.ChannelId)).ToArray();
      log.Information("Collect {Platform} - planned {Channels} ({Desc}) channels for update", plan.Platform, actualNew.Length, desc);
      return plan with {ChannelPlans = plan.ChannelPlans.Concat(actualNew).KeyBy(c => c.Channel.ChannelId)};
    }

    public static ChannelUpdatePlan AsPlan(this Channel c) => new() {Channel = c};

    public static bool ForUpdate(this Channel c, string[] explicitSourceIds = null) {
      var sourceIds = explicitSourceIds?.ToHashSet();
      var enoughSubs = c.Subs == null || c.Subs > 1000;
      var alive = c.Status.NotIn(ChannelStatus.NotFound, ChannelStatus.Blocked, ChannelStatus.Dead);
      return (sourceIds == null || sourceIds.Contains(c.SourceId)) && alive && enoughSubs;
    }
  }
}