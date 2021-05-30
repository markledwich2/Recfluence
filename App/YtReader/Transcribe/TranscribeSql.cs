using System.Collections.Generic;

namespace YtReader.Transcribe {
  public static class TranscribeSql {
    public static readonly Dictionary<string, string> Sql = new() {
      {
        "QAnonAlt", @"
select e.video_id
from video_extra e 
join channel_latest c on c.channel_id = e.channel_id
where array_contains('QAnon'::variant, tags) and e.platform in ('BitChute', 'Rumble')
qualify row_number() over (partition by c.channel_id order by e.views desc) < 3"
      }
    };
  }
}