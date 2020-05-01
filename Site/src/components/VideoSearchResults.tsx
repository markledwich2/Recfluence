import React, { FunctionComponent } from "react"
import { compactInteger } from "humanize-plus"
import { dateFormat, secondsToHHMMSS } from "../common/Utils"
import { ChannelTags, ChannelTagData } from "./ChannelTags"
import Highlighter from "react-highlight-words"
import _ from 'lodash'
import { queryHighlights } from '../common/Elastic'
import styled from 'styled-components'
import { theme, media } from './MainLayout'
import { Link } from 'gatsby'

interface CaptionSearchResult extends CaptionDocument {
    _doc_count: number
    captions: TimedCaption[]
}

interface TimedCaption {
    offset_seconds: number
    caption: string
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
}

export const VideoSearchResults = ({ data, query, error }: { data: CaptionSearchResult[], query: string, error: string }) => {
    if (!query) return <></> // don't show empty results

    const byVid = _(data).groupBy(c => c.video_id).map(g => {
        const first = g[0]
        const grouped: CaptionSearchResult = _.assign({}, first,
            {
                captions: _(g).sortBy(c => c.offset_seconds)
                    .map(c => ({
                        offset_seconds: c.offset_seconds,
                        caption: c.caption
                    })).value()
            })
        return grouped
    }).value()

    var words = query ? queryHighlights(query) : []

    return <>{byVid.map(d => <VideoSearchResult caption={d} searchWords={words} key={d.caption_id} />)}</>
}

const ResultsRow = styled.div`
    display: flex;
    flex-direction: column;
    padding: 0.5em 0.1em;
    @media (${media.width.large}) {
        /* images to left */
        flex-direction: row; 
    }
    > * {
        padding: 0.5em;
    }
`

const DetailsRow = styled.div`
    display:flex;
    b {
        color:${theme.fontColor}
    }
    > * {
        padding-right:1em;
        margin:auto;
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
                    <ChannelTags channel={cd} />
                </div>
                <span>
                    {c.captions.slice(0, maxCaptions).map(t => (
                        <p key={t.offset_seconds} style={{ paddingBottom: "0.3em" }}>
                            <VideoA id={c.video_id} offset={t.offset_seconds}>{secondsToHHMMSS(t.offset_seconds)} </VideoA>
                            <Highlighter
                                searchWords={p.searchWords}
                                autoEscape
                                textToHighlight={t.caption}
                            />
                        </p>
                    ))}
                    {c.captions.length > maxCaptions ? <p>{c.captions.length - maxCaptions} more...</p> : <></>}
                </span>
                {/* {c._doc_count > 1 ? <div style={{ paddingTop: "0.5em" }}><i>... {c._doc_count - 1} more for this video</i></div> : <></>} */}
            </div>
        </ResultsRow>
    )
}