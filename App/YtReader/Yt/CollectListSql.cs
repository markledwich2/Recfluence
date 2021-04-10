using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using SysExtensions.Collections;

namespace YtReader.Yt {
  public static class CollectListSql {
    public static (string Sql, JObject args) NamedQuery(string name, JObject args) =>
      (NamedSql.TryGet(name) ?? throw new($"no sql called {name}"), args);

    public static readonly Dictionary<string, string> NamedSql = new() {
      {
        "sans_comment", @"
-- comments on a sample of videos from highly subscribed channels that are missing comments
with chans as (
  select channel_id
  from channel_latest c
  where subs>:min_subs
    and not exists(select *
                   from comment t
                          join video_latest v on t.video_id=v.video_id
                   where v.channel_id=c.channel_id)
)
select video_id, v.channel_id
from chans c
       join video_latest v on v.channel_id=c.channel_id
  qualify row_number() over (partition by c.channel_id order by random())<:chan_per_vid
"
      }
    };
  }
}