import React, { useState, useEffect, useRef, CSSProperties } from "react"
import { RouteComponentProps as CProps } from "@reach/router"
import { ReactiveBase, ReactiveList, DataSearch, MultiList, SelectedFilters, SingleRange, SingleDataList, StateProvider, DateRange } from '@appbaseio/reactivesearch'
import { theme, media, isGatsbyServer, CenterDiv } from "../MainLayout"
import styled from 'styled-components'
import _, { Dictionary } from 'lodash'
import { VideoSearchResults, SearchHelp, NoResult } from './VideoSearchResults'
import { useMediaQuery } from 'react-responsive'
import { Button } from '../Button'
import { FilterList as IconFilter, Close as IconClose, List as IconList } from '@styled-icons/material'


const MainContainer = styled.div`
    display:flex; flex-direction:column;
    
    @media (${media.width.small}) {
      height:100vh;
    }

    @media (${media.width.medium}) {
      flex-direction:row;
    }
`

const ContentPane = styled.div`
    padding:1em;
    @media (${media.width.medium}) {
        padding: 2em;
    }
    display:flex; flex-direction:column;
    width:100%;
    max-width:1100px;
    margin:0 auto;
`

const SearchRow = styled.div`
    display:flex;
    width:100%;
    input {
        font-size:1.5rem;
        box-sizing:border-box;
        border-radius:5px;
    }
`

const FiltersPane = styled.div`
    display:flex; flex-flow:column wrap; justify-content:start;
    background-color: ${theme.backColor1};
    min-height:0px; /*needed for column wrap to kick in*/
    padding:0.5em 0.5em;
    
    @media (${media.width.medium}) {
      flex-flow:column;
      width:350px;
    }

    > * {
        padding:0.5em 1em;
        max-width:250px;
        @media (${media.width.medium}) {
            max-width:none;
        }
    }

    > .multi-list {
      flex: 1 1 auto;
      min-height:300px; /*needed for column wrap to kick in*/
    }

    > .multi-list.ideology {
      flex: 4 1 auto
    }

    ul {
        overflow-y:auto;
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
    height:100%; width:100%;
    overflow-y:hidden;
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
  display: "flex", flexDirection: "column",

}

interface SortValue { field: string, sort: 'asc' | 'desc' }

const sortOptions: Dictionary<string> = {
  'Relevance': '_score',
  'Views': 'views',
  'Uploaded': 'upload_date'
}

export const VideoSearch = ({ }: CProps<{}>) => {
  const [sort, setSort] = useState<SortValue>({ field: '_score', sort: 'desc' })
  const [filterOpened, setFilterOpened] = useState<boolean>(false)
  const filterOnRight = useMediaQuery({ query: `(${media.width.medium})` })
  const filterVisible = filterOnRight || filterOpened
  const resultsVisible = filterOnRight || !filterOpened

  if (isGatsbyServer()) return <div></div>

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
          <ContentPane>
            <SearchBar />

            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
              <SelectedFilters style={{ margin: '0.5em 0px' }} />
              {!filterOnRight && <div style={{ verticalAlign: 'top' }} >
                <Button
                  label={filterOpened ? "Results" : "Filter"}
                  icon={filterOpened ? <IconList /> : <IconFilter />} onclick={_ => setFilterOpened(!filterOpened)} />
              </div>}
            </div>

            <ResultsPane id="results" style={{ display: resultsVisible ? 'block' : 'none' }}>
              <StateProvider strict={false} >
                {({ searchState }) => {
                  const query = searchState?.q?.value
                  return query ? <ReactiveList
                    componentId="result"
                    react={{ and: ['q', 'views', 'sort', 'ideology', 'channel', 'upload'] }}
                    render={({ data, error, loading }) => <VideoSearchResults data={data} query={query} error={error} loading={loading} />}
                    infiniteScroll
                    scrollTarget={filterOnRight ? "results" : null}
                    size={20}
                    dataField={sort.field}
                    sortBy={sort.sort}
                    showResultStats={false}
                    showLoader={false}
                    renderNoResults={() => <NoResult />}
                  /> : SearchHelp
                }}
              </StateProvider>
            </ResultsPane>
          </ContentPane>

          <FiltersPaneComponent setSort={setSort} sort={sort} style={{ display: filterVisible ? 'flex' : 'none' }} />

        </MainContainer>
      </ReactiveBase >
    </div>
  )
}

const SearchBar: React.FunctionComponent = () => {
  const [query, setQuery] = useState<string>("")
  return (
    <SearchRow>
      <DataSearch
        componentId="q"
        filterLabel="Search"
        dataField={["caption", "video_title"]}
        placeholder="Search video captions"
        autosuggest={false}
        showIcon={false}
        searchOperators
        queryFormat='and'
        style={{ fontSize: "2em", flex: '100%' }}
        URLParams
        onKeyPress={(e: KeyboardEvent, triggerQuery: Function) => {
          if (e.key === "Enter") triggerQuery()
        }}
        onChange={(value: string, triggerQuery: Function) => setQuery(value)}
        value={query}
        autoFocus={true}
      />
    </SearchRow >
  )
}

const FiltersPaneComponent = ({ setSort, sort, style }: { setSort: React.Dispatch<React.SetStateAction<SortValue>>, sort: SortValue, style: CSSProperties }) =>
  <FiltersPane style={style}>
    <div style={{ display: 'flex', flexDirection: 'row', justifyContent: 'space-between' }}>
      <SingleDataList componentId='sort' title='Sort' filterLabel='Sort'
        dataField={sort.field}
        data={_(sortOptions).keys().map(k => ({ label: k })).value()}
        showRadio={false} showCount={false} showSearch={false}
        onValueChange={(label: string) => {
          setSort({ field: sortOptions[label], sort: 'desc' })
        }}
      />

      <SingleRange
        componentId="views"
        title="Views" filterLabel="Views"
        dataField="views"
        data={[
          // { start: null, end: null, label: 'any' },
          { start: 0, end: 1000, label: '1k or less' },
          { start: 1000, end: 10000, label: '1k - 10k' },
          { start: 10000, end: 100000, label: '10k - 100k' },
          { start: 100000, end: 1000000, label: '100k - 1M' },
          { start: 1000000, end: null, label: '1M +' },
        ]}
        showRadio={false}
        URLParams
      />
    </div>

    <DateRange
      componentId="upload"
      title="Uploaded"
      dataField="upload_date"
      URLParams
    />


    <MultiList
      className="multi-list ideology"
      componentId="ideology"
      title="Ledwich & Zaitsev Group" filterLabel="Group"
      dataField="ideology.keyword"
      showCheckbox showCount showMissing
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
      className="multi-list channel"
      componentId="channel"
      filterLabel="Channel"
      dataField="channel_title.keyword"
      title="Channel"
      showCheckbox showCount
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

