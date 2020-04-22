import React, { useEffect, useState, FunctionComponent } from "react"
import { RouteComponentProps, Router } from "@reach/router"
import algoliasearch from "algoliasearch"
import { InstantSearch, SearchBox, Hits, RefinementList } from "react-instantsearch-dom"
import { ReactiveBase, CategorySearch, ReactiveList, ResultCard, DataSearch, MultiList, SelectedFilters, RangeSlider, SingleRange, SingleDataList } from '@appbaseio/reactivesearch'
import { BasicPageDiv, theme } from "./MainLayout"
import styled from 'styled-components'
import { compactInteger } from "humanize-plus"
import { dateFormat, secondsToHHMMSS } from "../common/Utils"
import { ChannelData } from "../common/YtModel"
import { ChannelTags, ChannelTagData } from "./ChannelTags"
import Highlighter from "react-highlight-words"
import _, { Dictionary } from 'lodash'
import { RequestParams } from '@elastic/elasticsearch'
import { luceneTerms } from '../common/Lucine'

const ParseSearchForHighlightWords = (query: string) => {

}

interface CaptionDocument {
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

const FlexRow = styled.div`
    display:flex;
    > * {
        padding-right:1em
    }
`

const MainContainer = styled.div`
    padding-top:1em;
    > div {
        padding-bottom:1em;
    }
`

const SearchRow = styled.div`
    input {
        font-size: 1.5rem;
    }
`

const FilterRow = styled(FlexRow)`
    height: 300px;
    padding-top:0.5em;
    
    > * {
        padding-right:4em;
    }
    ul {
        padding-right:0.5em;
        max-height:none;
        overflow-y:scroll;
    }
    h2 {
        text-transform: uppercase;
        color: ${theme.fontColor}
    }
`

const FilteredListStyle: React.CSSProperties = {
    display: "flex",
    flexDirection: "column",
}

interface VideoSearchProps extends RouteComponentProps { }

interface SortValue {
    field: string
    sort: 'asc' | 'desc'
}

const sortOptions: Dictionary<string> = {
    'Relevance': '_score',
    'Views': 'views',
    'Uploaded': 'upload_date'
}

const getSortOption = (label: string) => {
    const options = sortOptions as any
}

export const VideoSearch = (p: VideoSearchProps) => {
    const [searchText, setSearchText] = useState<string>(null)
    const [sort, setSort] = useState<SortValue>({ field: '_score', sort: 'desc' })

    return (
        <BasicPageDiv>
            <ReactiveBase
                app="caption"
                url="https://8999c551b92b4fb09a4df602eca47fbc.westus2.azure.elastic-cloud.com:9243"
                credentials="public:5&54ZPnh!hCg"
                themePreset="dark"
                theme={{
                    typography: { fontSize: theme.fontSize, fontFamily: theme.fontFamily },
                    colors: { textColor: theme.fontColor, primaryColor: theme.themeColor }
                }}
            >
                <MainContainer>
                    <SearchRow>
                        <DataSearch
                            componentId="searchbox"
                            filterLabel="Search"
                            dataField={["caption", "video_title", "channel_title"]}
                            placeholder="Search video captions"
                            autosuggest={false}
                            showIcon={false}
                            debounce={200}
                            //highlight
                            //highlightField={"caption"}
                            onValueChange={(value) => {
                                console.log('onValueChange', value)
                                setSearchText(value)
                            }}
                            onValueSelected={(value, cause, source) => {
                                console.log('onValueSelected', value)
                                setSearchText(value)
                            }}
                            searchOperators={true}
                            style={{ fontSize: "2em" }}
                            theme={{ fontSize: "2em" }}
                        //aggregationField="video_id.keyword"
                        />
                    </SearchRow>

                    <FilterRow>
                        <SingleRange
                            componentId="viewsSlider"
                            dataField="views"
                            data={[
                                // { start: null, end: null, label: 'any' },
                                { start: 0, end: 1000, label: '1k or less' },
                                { start: 1000, end: 10000, label: '1k - 10k' },
                                { start: 10000, end: 100000, label: '10k - 100k' },
                                { start: 100000, end: 1000000, label: '100k - 1M' },
                                { start: 1000000, end: null, label: '1M +' },
                            ]}
                            title="Views"
                            filterLabel="Views"
                            showRadio={false}
                        />

                        <MultiList
                            componentId="ideologyList"
                            filterLabel="Group"
                            dataField="ideology.keyword"
                            title="Ledwich & Zaitzev Group"
                            //selectAllLabel="All Groups"
                            showCheckbox
                            showCount
                            showMissing
                            showSearch={false}
                            react={{ and: ['searchbox', 'viewsSlider'] }}
                            style={FilteredListStyle}
                            defaultQuery={_ => ({
                                aggs: {
                                    "ideology.keyword": {
                                        aggs: {
                                            video_count: {
                                                cardinality: {
                                                    field: "video_id.keyword"
                                                }
                                            }
                                        },
                                        terms: {
                                            field: "ideology.keyword",
                                            size: 50,
                                            order: { "_count": "desc" }, "missing": "N/A"
                                        }
                                    }
                                }
                            })}
                            transformData={(data: any[]) => {
                                const res = data.map(d => ({ key: d.key as string, doc_count: +d.video_count.value }))
                                return res
                            }}
                        />

                        <MultiList
                            componentId="channelList"
                            filterLabel="Channel"
                            dataField="channel_title.keyword"
                            title="Channel"
                            //selectAllLabel="All Channels"
                            showCheckbox
                            showCount
                            showSearch={true}
                            react={{ and: ['searchbox', 'viewsSlider', 'ideologyList'] }}
                            style={FilteredListStyle}
                            defaultQuery={_ => ({
                                aggs: {
                                    "channel_title.keyword": {
                                        aggs: {
                                            video_count: {
                                                cardinality: {
                                                    field: "video_id.keyword"
                                                }
                                            }
                                        },
                                        terms: {
                                            field: "channel_title.keyword",
                                            size: 50,
                                            order: { "_count": "desc" }, "missing": "N/A"
                                        }
                                    }
                                }
                            })}
                            transformData={(data: any[]) => {
                                const res = data.map(d => ({ key: d.key as string, doc_count: +d.video_count.value }))
                                return res
                            }}
                        />

                        <SingleDataList
                            componentId='sortList'
                            title='Sort'
                            filterLabel='Sort'
                            dataField='value'
                            data={_(sortOptions).keys().map(k => ({ label: k })).value()}
                            showRadio={false}
                            //showFilter={false}
                            showCount={false}
                            showSearch={false}
                            onValueChange={(label: string) => {
                                console.log('sortList.onValueChange', label)
                                return setSort({ field: sortOptions[label], sort: 'desc' })
                            }}
                        />

                    </FilterRow>

                    <SelectedFilters />

                    <ReactiveList
                        componentId="result"
                        react={{ and: ['searchbox', 'viewsSlider', 'ideologyList', 'channelList'] }}
                        render={({ data }) => <VideoSearchResults data={data} query={searchText} />}
                        renderNoResults={() => <></>}
                        infiniteScroll={true}
                        showResultStats={false}
                        size={30}
                        defaultQuery={(d) => {
                            if (searchText?.length > 2) {
                                let query: EsQuery = {
                                    sort: {}
                                }
                                query.sort[sort.field] = { order: sort.sort }
                                return query
                            } else {
                                return {
                                    query: {
                                        match_none: {},
                                    }
                                }
                            }
                        }}
                        // sortOptions={[
                        //     {
                        //         label: 'Relevance',
                        //         dataField: '_score',
                        //         sortBy: 'asc'
                        //     },
                        //     {
                        //         label: 'Views',
                        //         dataField: 'views',
                        //         sortBy: 'desc'
                        //     },
                        //     {
                        //         label: 'Uploaded',
                        //         dataField: 'upload_date',
                        //         sortBy: 'desc'
                        //     }
                        // ]}

                        dataField={sort.field}
                    />

                </MainContainer>
            </ReactiveBase>
        </BasicPageDiv>
    )
}

interface EsQuery {
    query?: {
        match?: { [x: string]: any }
        match_none?: {}
    }
    sort?: {
        [field: string]: { order: 'asc' | 'desc' }
    }
}

interface CaptionSearchResult extends CaptionDocument {
    _doc_count: number
    captions: TimedCaption[]
}

interface TimedCaption {
    offset_seconds: number
    caption: string
}

interface VideoSearchResults {
    data: CaptionSearchResult[]
    query: string
}

const VideoSearchResults = (p: VideoSearchResults) => {
    const byVid = _(p.data).groupBy(c => c.video_id).map(g => {
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

    var words = p.query ? luceneTerms(p.query).map(t => t.term) : []

    return <>{byVid.map(d => <VideoSearchResult caption={d} searchWords={words} key={d.caption_id} />)}</>
}

const ResultsRow = styled(FlexRow)`
    padding: 0.5em;
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

interface VideoLinkProps {
    id: string
    offset: number
}
const VideoA: FunctionComponent<VideoLinkProps> = ({ id, offset, children }) =>
    <a href={`/video/${id}?t=${offset}`} target="_video">{children}</a>

interface VideoSearchResultProps {
    caption: CaptionSearchResult
    searchWords: string[]
}
export const VideoSearchResult = (p: VideoSearchResultProps) => {
    var c = p.caption
    var cd: ChannelTagData = {
        ideology: c.ideology,
        lr: c.lr,
        tags: [],
    }

    var maxCaptions = 4

    return (
        <ResultsRow key={c.caption_id}>
            <VideoA id={c.video_id} offset={c.offset_seconds}><img src={c.thumb_high} width="300px" style={{ verticalAlign: "text-top" }} /></VideoA>
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
                            <VideoA id={c.video_id} offset={t.offset_seconds}>{secondsToHHMMSS(t.offset_seconds)}</VideoA>
                            <Highlighter
                                searchWords={p.searchWords}
                                autoEscape
                                textToHighlight={" " + t.caption}
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