import React, { useState, useEffect, useRef } from "react"
import { RouteComponentProps as CProps } from "@reach/router"
import { ReactiveBase, ReactiveList, DataSearch, MultiList, SelectedFilters, SingleRange, SingleDataList, StateProvider, DateRange } from '@appbaseio/reactivesearch'
import { theme, media, isGatsbyServer, CenterDiv } from "../MainLayout"
import styled from 'styled-components'
import _, { Dictionary } from 'lodash'
import { VideoSearchResults, SearchHelp } from './VideoSearchResults'
import { useMediaQuery } from 'react-responsive'
import { Button } from '../Button'
import { FilterList, Close as IconClose } from '@styled-icons/material'
import { SadTear as IconSad } from '@styled-icons/fa-solid'

const MainContainer = styled.div`
    max-width:1400px;
    margin: auto;
    display:flex;
    justify-content:space-around;
    flex-direction:column;
    height:none;
    @media (${media.width.medium}) {
        justify-content:space-around;
        flex-direction:row;
        height:100vh;
    }
    
`

const ContentPane = styled.div`
    padding:1em;
    @media (${media.width.medium}) {
        padding: 2em;
    }
    display:flex;
    flex-direction:column;
    flex: 100%;
    max-width:1100px;
`


const SearchRow = styled.div`
    display:flex;
    input {
        font-size:1.5rem;
        box-sizing:border-box;
        border-radius:5px;
    }
`

const FiltersPane = styled.div`
    display:flex;
    background-color: ${theme.backColor1};

    justify-content:start;
    flex-direction:column;
    flex-wrap:wrap;
    overflow-y:auto;

    @media (${media.width.small}) {
        max-height:70vh;
    }
    @media (${media.width.medium}) {
        flex-wrap:nowrap;
        width:300px;
        max-height:none;
        height:100%;
    }
    
    padding:0.5em 0.5em;
    
    > * {
        padding:0.5em 1em;
        max-height:300px;
        max-width:250px;
        min-width:200px;
        @media (${media.width.medium}) {
            max-height:200px;
            max-width:none;
            max-height:200px;
        }
        @media (${media.width.medium}) and (${media.height.large}) {
                max-height:25vh;
        }
        @media (${media.width.medium}) and (${media.height.xlarge}) {
                max-height:35vh;
        }
    }

    ul {
        overflow-y:auto;
        padding: 0em;
        max-height:none;
        padding: 0 1em 0 0.2em;
        li {
            min-height:none;
            margin:0em;
            padding: 0px;
            label {
                padding:0px;
            }
        }
        
    }

    h2 {
        text-transform: uppercase;
        color: ${theme.fontColor};
        margin:0.5em 0em;
    }

    .DayPickerInput input {
        height: 2.5em !important;
        padding: 1em .7em !important;
    }
`

const ResultsPane = styled.div`
    position:relative;
    height:100%;
    width:100%;
    overflow-y:auto;
    @media (${media.width.medium}) {
        overflow-y:scroll;
    }

    select {
        background:${theme.backColor};
        outline:1px solid #333;
        color:${theme.fontColor};
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




export const VideoSearch = ({ }: CProps<{}>) => {
    const [sort, setSort] = useState<SortValue>({ field: '_score', sort: 'desc' })
    const [filterVisible, setFilterVisible] = useState<boolean>(false)
    const isMultiColumn = useMediaQuery({ query: `(${media.width.medium})` })
    if (isGatsbyServer()) return <div></div>

    var filtersStyle: React.CSSProperties = { display: 'block' }
    if (!isMultiColumn) {
        filtersStyle = filterVisible ?
            {
                display: 'block',
                position: 'absolute',
                top: '70px',
                left: '1vh',
                width: '95vw',
                zIndex: 2
            } : {
                display: 'none',
                position: 'static'
            }
    }

    return (
        <div>
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
                    <div style={filtersStyle}>
                        {!isMultiColumn && (
                            <div style={{ position: 'absolute', top: '5px', right: '5px' }}>
                                <Button icon={<IconClose />} onclick={_ => setFilterVisible(!filterVisible)} />
                            </div>
                        )}
                        <FiltersPaneComponent setSort={setSort} sort={sort} />

                    </div>
                    <ContentPane>
                        <SearchBar />

                        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                            <SelectedFilters style={{ margin: '0.5em 0px' }} />
                            {!isMultiColumn && <div style={{ verticalAlign: 'top' }} >
                                <Button label="Filter" icon={<FilterList />} onclick={_ => setFilterVisible(!filterVisible)} />
                            </div>}
                        </div>

                        <ResultsPane id="results">
                            <StateProvider strict={false} >
                                {({ searchState }) => {
                                    const query = searchState?.q?.value
                                    return query ? <ReactiveList
                                        componentId="result"
                                        react={{ and: ['q', 'views', 'sort', 'ideology', 'channel', 'upload'] }}
                                        render={({ data, error, loading }) => <VideoSearchResults data={data} query={query} error={error} loading={loading} />}
                                        showResultStats={false}
                                        infiniteScroll
                                        scrollTarget={isMultiColumn ? "results" : null}
                                        size={20}
                                        dataField={sort.field}
                                        sortBy={sort.sort}
                                        showLoader={false}
                                        renderNoResults={() => <CenterDiv>
                                            <span style={{ color: theme.fontColorSubtler, fontSize: '1.5em' }}>
                                                <IconSad color={theme.backColor2} height='2em' style={{ position: 'relative', top: '0.5em', left: '-1em' }} />
                                            Nothing found</span>
                                        </CenterDiv>}
                                    /> : SearchHelp
                                }}
                            </StateProvider>
                        </ResultsPane>

                    </ContentPane>
                </MainContainer>
            </ReactiveBase >
        </div>
    )
}

const SearchBar: React.FunctionComponent = () => {
    const [query, setQuery] = useState<string>("")
    const [timer, setTimer] = useState<number>()

    useEffect(() => () => {
        // When the component unmounts, remove the timer.
        clearTimeout(timer)
    }, [])

    const handleChange = (value: string, triggerQuery: Function) => {
        setQuery(value)
        // Set a timer for debouncing, if it's passed, call triggerQuery.
        //setTimer(setTimeout(triggerQuery, 2000))
    }

    const handleKey = (e: KeyboardEvent, triggerQuery: Function) => {
        if (e.key === "Enter") {
            triggerQuery()
            // Reset the timer for debouncing.
            clearTimeout(timer)
        }
    }

    return (
        <SearchRow>
            <DataSearch
                componentId="q"
                filterLabel="Search"
                dataField={["caption"]}
                placeholder="Search video captions"
                autosuggest={false}
                showIcon={false}
                searchOperators
                style={{ fontSize: "2em", flex: '100%' }}
                URLParams
                onKeyPress={handleKey}
                onChange={handleChange}
                value={query}
                autoFocus={true}
            />

        </SearchRow >

    )
}

const FiltersPaneComponent = ({ setSort, sort }: { setSort: React.Dispatch<React.SetStateAction<SortValue>>, sort: SortValue }) => <FiltersPane>

    <div style={{ display: 'flex', flexDirection: 'row', justifyContent: 'space-between' }}>
        <SingleDataList
            componentId='sort'
            title='Sort'
            filterLabel='Sort'
            dataField={sort.field}
            data={_(sortOptions).keys().map(k => ({ label: k })).value()}
            showRadio={false}
            showCount={false}
            showSearch={false}
            onValueChange={(label: string) => {
                setSort({ field: sortOptions[label], sort: 'desc' })
            }}
        />



        <SingleRange
            componentId="views"
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
            URLParams
        />
    </div>

    <DateRange
        title="Uploaded"
        componentId="upload"
        dataField="upload_date"
        URLParams
    />

    <MultiList
        className="multi-list"
        componentId="ideology"
        filterLabel="Group"
        dataField="ideology.keyword"
        title="Ledwich & Zaitsev Group"
        showCheckbox
        showCount
        showMissing
        showSearch={false}
        react={{ and: ['q', 'views', 'upload'] }}
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
                        order: { "video_count": "desc" }, "missing": "N/A"
                    }
                }
            }
        })}
        transformData={(data: any[]) => {
            const res = data.map(d => ({ key: d.key as string, doc_count: +d.video_count.value }))
            return res
        }}
        URLParams
    />

    <MultiList
        className="multi-list"
        componentId="channel"
        filterLabel="Channel"
        dataField="channel_title.keyword"
        title="Channel"
        showCheckbox
        showCount
        showSearch={true}
        react={{ and: ['q', 'views', 'ideology', 'upload'] }}
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
                        size: 100,
                        order: { "video_count": "desc" }, "missing": "N/A"
                    }
                }
            }
        })}
        transformData={(data: any[]) => {
            const res = data.map(d => ({ key: d.key as string, doc_count: +d.video_count.value }))
            return res
        }}
        URLParams
    />
</FiltersPane>

