import React, { useState } from "react"
import { RouteComponentProps as CProps } from "@reach/router"
import { ReactiveBase, ReactiveList, DataSearch, MultiList, SelectedFilters, SingleRange, SingleDataList } from '@appbaseio/reactivesearch'
import { TextPage, theme, FlexRow } from "./MainLayout"
import styled from 'styled-components'
import _, { Dictionary } from 'lodash'
import { VideoSearchResults } from './VideoSearchResults'

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

interface SortValue {
    field: string
    sort: 'asc' | 'desc'
}

const sortOptions: Dictionary<string> = {
    'Relevance': '_score',
    'Views': 'views',
    'Uploaded': 'upload_date'
}

export const VideoSearch = (props: CProps<{}>) => {
    const [searchText, setSearchText] = useState<string>(null)
    const [sort, setSort] = useState<SortValue>({ field: '_score', sort: 'desc' })

    return (
        <TextPage>
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
                            onValueChange={(value) => {
                                console.log('onValueChange', value)
                                setSearchText(value)
                            }}
                            onValueSelected={(value) => {
                                console.log('onValueSelected', value)
                                setSearchText(value)
                            }}
                            searchOperators={true}
                            style={{ fontSize: "2em" }}
                            theme={{ fontSize: "2em" }}
                        />
                    </SearchRow>

                    <FilterRow>

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
                            title="Ledwich & Zaitsev Group"
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
                        defaultQuery={() => {
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

                        dataField={sort.field}
                    />
                </MainContainer>
            </ReactiveBase>
        </TextPage>
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