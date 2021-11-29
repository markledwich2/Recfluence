using Newtonsoft.Json.Linq;

namespace YtReader.Yt; 

public static class CollectListSql {
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
  qualify row_number() over (partition by c.channel_id order by random())<:vids_per_chan
"
    }, {
      "fashion", @"
select channel_id
from channel_latest c
where array_contains('Fashion'::variant, topics)
and not exists(select * from video_latest v where v.channel_id=c.channel_id)
and subs > :min_subs
"
    }, {
      "collect_covid", @"
select v.video_id, c.channel_id
from collect_covid m
join video_latest v on v.video_id = m.video_id
join channel_latest c on c.channel_id =v.channel_id
where v.upload_date > '2020-01-01'
  and c.subs > 10000
  and c.meets_review_criteria is null or not c.meets_review_criteria
qualify max(v.updated) over (partition by m.video_id) < current_date()-2"
    }, {
      "stale_extra",
      @"
select video_id, channel_id from video_latest 
where platform = :platform and (extra_updated is null or extra_updated < current_date() - :older_than_days)
order by views desc nulls last
limit :limit
"
    }, {
      "qanon_dx_expansion_sans_comments",
      @"
with channels as (
  select $1 channel_id from @yt_data/import/channels/qanon_dx_expansion_channels_20210520.txt.gz (file_format => tsv)
)
, channel_users as (
  select v.channel_id, count(distinct u.user_id) commentors_with_subs
  from comment s
  join video_latest v on v.video_id = s.video_id
  join user u on u.user_id = s.author_channel_id
  where array_size(u.subscriptions) > 0
  group by channel_id
)
, channel_stats as (
  select v.channel_id, count(distinct s.video_id) videos_with_comments
  from comment s
  join video_latest v on v.video_id = s.video_id
  group by channel_id
)
, load_stats as (
  select r.channel_id, c.channel_title, u.commentors_with_subs, s.videos_with_comments
  from channels r
  left join channel_latest c on c.channel_id = r.channel_id
  left join channel_users u on u.channel_id = r.channel_id
  left join channel_stats s on s.channel_id = r.channel_id
)
select r.channel_id, v.video_id
from channels r
join channel_latest c on c.channel_id = r.channel_id
join video_latest v on v.channel_id = r.channel_id
qualify ROW_NUMBER() over (partition by r.channel_id order by v.views desc nulls last) <= 10
order by 1,2
"
    }, {
      "qanon_dx_expansion_users",
      @"
  with channels as (
    select $1 channel_id
    from @yt_data/import/channels/qanon_dx_expansion_channels_20210520.txt.gz (file_format => tsv)
  )
    , channel_users as (
    select s.author_channel_id user_id, r.channel_id, count(*) user_comments
    from comment s
           join video_latest v on v.video_id=s.video_id
           join channels r on r.channel_id=v.channel_id
    group by 1,2
  )
  select distinct user_id
  from channel_users
  qualify row_number() over (partition by channel_id order by user_comments desc) < 50 -- top commenter's form each channel
"
    }, {
      "bitchute_extra_broken_chan", @"
  select e.video_id, null as channel_id
  from video_extra e
         left join channel_latest c on c.channel_id=e.channel_id
  where c.channel_id is null and e.platform in ('BitChute') and error is null
  order by views nulls last
"
    }, {
      "rumble_discover_channels", @"
select null video_id, v:ChannelId::string channel_id, v:Platform::string platform
from video_stage vs
where v:Platform='Rumble'
  and v:DiscoverSource:Type='Home'
  and not exists(select * from channel_latest c where c.channel_id=vs.v:ChannelId and meets_review_criteria)
qualify row_number() over (partition by v:VideoId order by v:Updated desc) = 1
"
    }, {
      "q_alt_sans_caps", @"
select e.video_id, e.channel_id
from video_extra e
join channel_latest c on c.channel_id = e.channel_id
where array_contains('QAnon'::variant, tags) and e.platform in ('BitChute', 'Rumble') and e.media_url is not null
and not exists(select * from caption s where s.video_id = e.video_id)
order by e.views desc nulls last
"
    }
  };

  public static (string Sql, JObject args) NamedQuery(string name, JObject args) =>
    (NamedSql.TryGet(name) ?? throw new($"no sql called {name}"), args);
}