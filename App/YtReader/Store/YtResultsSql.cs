using Mutuo.Etl.Db;

namespace YtReader.Store; 

public static class YtResultsSql {
  public static class Narrative {
    public static readonly string[] FilterTags = {"MissingLinkMedia", "OrganizedReligion", "Educational", "Black", "LGBT"};

    public static readonly string VaccinePersonalHighlight = $@"
with channel_highlights as (
    with h1 as (select $1::string channel_id
       --, $7::string video_url
       , to_number(replace($4,',','')) mention_videos
       , to_number(replace($5,',','')) mention_video_views
       , regexmatch($7,'.*(?:v=(?<video>[\\w-]*)).*(?:t=(?<offset>\\d*))',NULL) link_meta
    from @public.yt_data/import/narratives/channel_highlights.tsv (file_format => tsv_header)
  )
  select channel_id, mention_videos, mention_video_views, link_meta:video::string video_id, link_meta: offset::int offset_seconds
  from h1
)
  , narrative_captions as (
  select video_id, v.value:caption::string caption, v.value:offset_seconds::int offset_seconds
  from video_narrative2
    , table (flatten(captions)) v
  where narrative='Vaccine Personal'
)
  , h1 as (
  select 
        h.*
       , c.channel_title
       , c.subs
       , arrayExclude(c.tags, array_construct({FilterTags.Join(", ", t => t.SingleQuote())})) tags
       , c.lr
       , c.logo_url channel_logo
       , v.video_title
       , v.views
       , datediff(seconds,'0'::time,v.duration) duration_secs
       , coalesce(n.caption,s.caption) caption
  from channel_highlights h
         join channel_latest c on c.channel_id=h.channel_id
         join video_latest v on v.video_id=h.video_id
         left join narrative_captions n on n.video_id=h.video_id and n.offset_seconds=h.offset_seconds
         left join caption s on n.caption is null and s.video_id=h.video_id
    qualify row_number() over (partition by h.video_id order by abs(h.offset_seconds-s.offset_seconds))=1
)
select *
from h1";

    public static readonly string VaccineDnaHighlight = $@"
with highlights as (
  with raw as (
    select $1::object v
    from @public.yt_data/import/narratives/covid_vaccine_dna_mod.top50_view_lab.jsonl.gz
  )
  select v:VIDEO_ID::string video_id, v:CAPTION::string caption, v:OFFSET_SECONDS::double offset_seconds
  from raw
)
select h.video_id, v.video_title
  , v.channel_id, v.channel_title
  , v.views::int video_views
  , timediff(seconds,'0'::time,v.duration) duration_secs
  , arrayExclude(c.tags, array_construct({FilterTags.Join(", ", t => t.SingleQuote())})) tags
  , c.subs, c.logo_url, c.lr
  , array_construct(object_construct('caption', h.caption, 'offsetSeconds', h.offset_seconds::int)) captions
from highlights h
       left join video_latest v on v.video_id=h.video_id
       left join channel_latest c on c.channel_id=v.channel_id
";
  }
}