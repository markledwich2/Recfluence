import React, { FunctionComponent, useContext } from "react"
import { compactInteger } from "humanize-plus"
import { dateFormat, secondsToHHMMSS } from "../../common/Utils"
import { ChannelTags, ChannelTagData } from "../channel_relations/ChannelTags"
import Highlighter from "react-highlight-words"
import _ from 'lodash'
import { queryHighlights } from '../../common/Elastic'
import styled from 'styled-components'
import { theme, media, CenterDiv } from '../MainLayout'
import { Link } from 'gatsby'
import ReactMarkdown from 'react-markdown/with-html'
import { Spinner } from '../Spinner'
import { SadTear as IconSad } from '@styled-icons/fa-solid'
import { UserContext } from '../UserContext'

interface CaptionSearchResult extends CaptionDocument {
  _doc_count: number
  captions: TimedCaption[]
}

interface TimedCaption {
  caption_id: string
  offset_seconds: number
  caption: string
  part: CaptionPart
}

export interface CaptionDocument {
  caption_id: string
  video_id: string
  ideology: string
  media: string
  country: string
  lr: string
  video_title: string
  channel_title: string
  channel_id: string
  keywords: string
  description: string
  thumb_high: string
  offset_seconds: number
  caption: string
  upload_date: Date
  views: number
  url: string
  part: CaptionPart
}

type CaptionPart = 'Caption' | 'Title' | 'Description' | 'Keywords'

const HelpStyle = styled.div`
  padding:1em;
  font-size: 1.2em;
  line-height: 1.5em;
  b, h1, h2, h3 {
      color:${theme.fontColor}
  }
  p {
      margin: 0.5em 0em 1em 0em;
  }
  code, inlineCode  {
      font-family:monospace;
      background-color:${theme.backColorBolder};
      padding: 0.1em 0.2em;
      border: 1px solid ${theme.backColorBolder2};
      border-radius: 5px;
  }
`

const ResultsPane = styled.div`
  padding: 0 1em;
`

const LoginOverlay = styled.div`
  position: absolute; 
  top:0px; left:0px;
  width:100%;
  display: flex;
  justify-content: space-around;
  align-items: center;

  > div {
    max-width:500px;
    padding: 1em 2em;
    margin: 5em auto;
    background-color: ${theme.backColorTransparent};
  }
`

const BlurOverlay = styled.div`
  position: relative;
  > div.blurred {
    position: absolute;
    top:0; left: 0;
    width: 100%; height: 100%;
    backdrop-filter: blur(2px);
  }
  > div.blurred.un-authed {
    backdrop-filter: blur(15px);
  }
`

const searchMd = `
Search titles and captions of political YouTube videos created since Jan 2019. 

### Quotes
Use \`""\` to find full phrases.

\`"Charlie Brown"\` will only return results that mention Charlie by his full name (just like Google search). 

This can be combined with other terms. 
Example: \`"Charlie Brown" Snoopy\` will also find results with Snoopy.

### Advanced

\`|\` will match if either of the terms are present. \`Snoopy|Charlie\` will return video segments that have either \`Snoopy\` or \`Charlie\`.

\`+\` will match only if both terms are present (default behavior). \`Snoopy Charlie\` or \`Snoopy+Charlie\` will only return video segments that have both \`Snoopy\` and \`Charlie\`.

\`-\` Will ensure a word is not present. \`Snoopy -Space\` will only return video segments that have \`Snoopy\` but no \`Space\`

\`()\` controls precedence. \`(dog|wolf) +collar\` will find video segments that have either \`dog\` or \`wolf\` but must have \`collar\`.

### Wildcard
Use \`*\` to match any characters in the part of a word. \`Snoo*\` will match \`Snoopy\`, \`Snooze\`, \`Snoop\`

### Near
Use \`~\` to match variations within a single character. E.g. \`Charlie~\` will match \`Charlie\` and \`Charlies\`. 

Use \`~N\` to match N character different. E.g. \`Utilitarian~3\` will match \`Utilitarians\` and \`Utilitarianism\`. 
`

export const SearchHelp = <HelpStyle><ReactMarkdown source={searchMd} escapeHtml={false} /></HelpStyle>

const captionPartOrder: { [P in CaptionPart]: number } = {
  Title: 0,
  Description: 1,
  Keywords: 2,
  Caption: 3,
}

export const VideoSearchResults = ({ data, query, error, loading }: { data: CaptionSearchResult[], query: string, error: string, loading: any }) => {
  if (!query) return <></> // don't show empty results


  const { user, logIn } = useContext(UserContext)

  if (!user)
    data = data.slice(0, 5)

  const byVid = _(data).groupBy(c => c.video_id).map(g => {
    const first = g[0]
    const grouped: CaptionSearchResult = _.assign({}, first,
      {
        captions: _(g).sortBy(c => `${captionPartOrder[c.part]}.${c.offset_seconds}`)
          .filter(c => c.part != 'Title') // don't display title separately
          .map(c => ({
            caption_id: c.caption_id,
            part: c.part,
            offset_seconds: c.offset_seconds,
            caption: c.caption
          })).value()
      })
    return grouped
  }).value()


  var words = query ? queryHighlights(query) : []

  return <ResultsPane>
    {loading && data.length == 0 && <Spinner size="80px" />}
    <BlurOverlay>
      {byVid.map(d => <VideoSearchResult caption={d} searchWords={words} key={d.caption_id} />)}
      {(loading || !user) && <>
        <div className={user ? "blurred" : "blurred un-authed"} />
      </>}
    </BlurOverlay>
    {!user && <LoginOverlay><div>
      <big><a onClick={_ => logIn()}>Sign in</a> to enable search</big><br /><br />
        Consider your searches public information. This service is free (for now) but we want to be are to use the search data however we wish in the future.<br /><br />

        Please use responsibly.
        </div></LoginOverlay>}
    {loading && data.length > 0 && <Spinner size="80px" />}
  </ResultsPane>
}

const ResultsRow = styled.div`
    display: flex;
    flex-direction: column;
    padding: 0.5em 0.1em;
    @media (${media.width.large}) {
        flex-direction: row;  /* images to left */
    }
    > * {
        padding: 0.5em 0em;
        @media (${media.width.large}) {
            padding: 0.5em 0.5em;
        }
    }
`

const DetailsRow = styled.div`
    display:flex;
    flex-wrap:wrap;
    b {
        color:${theme.fontColor}
    }
    > * {
        padding-right:1em;
    }
`

const CaptionP = styled.p`
  padding-bottom:0.3em;

  .part-name {
    color:${theme.fontColorSubtler}
  }

  .caption.part-Description, .caption.part-Keywords {
    color:${theme.fontColorSubtler}
  }
`

const VideoA: FunctionComponent<{ id: string, offset: number }> = ({ id, offset, children }) =>
  <Link to={`/video/${id}?t=${offset}`}>{children}</Link>


export const VideoSearchResult = (p: { caption: CaptionSearchResult, searchWords: string[] }) => {
  var c = p.caption
  var cd: ChannelTagData = {
    ideology: c.ideology,
    lr: c.lr,
    tags: [],
  }
  var maxCaptions = 4
  return (
    <ResultsRow key={c.caption_id}>
      <VideoA id={c.video_id} offset={c.offset_seconds}><img src={c.thumb_high} style={{ verticalAlign: "text-top", width: "300px" }} /></VideoA>
      <div style={{ width: "100%" }}>
        <h2>
          <Highlighter
            searchWords={p.searchWords}
            autoEscape
            textToHighlight={c.video_title}
          />
        </h2>
        <div style={{ display: "flex", justifyContent: "space-between" }}>
          <DetailsRow>
            <b>{c.channel_title}</b>
            <span><b>{compactInteger(c.views)}</b> views</span>
            <span>{dateFormat(c.upload_date)}</span>
          </DetailsRow>
          <div><ChannelTags channel={cd} /></div>
        </div>
        <span>
          {c.captions.slice(0, maxCaptions).map(t => (
            <CaptionP key={t.caption_id}>
              {t.part == 'Keywords' && <span className='part-name'>keywords: </span>}
              {t.part == 'Caption' && <VideoA id={c.video_id} offset={t.offset_seconds}>{secondsToHHMMSS(t.offset_seconds)}</VideoA>}
              <Highlighter
                searchWords={p.searchWords}
                autoEscape
                textToHighlight={t.caption}
                className={`caption part-${t.part}`}
              />
            </CaptionP>
          ))}
          {c.captions.length > maxCaptions ? <p>{c.captions.length - maxCaptions} more...</p> : <></>}
        </span>
      </div>
    </ResultsRow>
  )
}


export const NoResult = () => <CenterDiv>
  <span style={{ color: theme.fontColorSubtler, fontSize: '1.5em' }}>
    <IconSad color={theme.backColorBolder2} height='2em'
      style={{ position: 'relative', top: '0.5em', left: '-1em' }} />
                    Nothing found
    </span>
</CenterDiv>