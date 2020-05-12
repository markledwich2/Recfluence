import React, { useState, useEffect, useRef, CSSProperties, useContext, KeyboardEvent } from "react"
import { RouteComponentProps as CProps } from "@reach/router"
import { ReactiveBase, ReactiveList, DataSearch, MultiList, SelectedFilters, SingleRange, SingleDataList, StateProvider, DateRange } from '@appbaseio/reactivesearch'
import { theme, media, isGatsbyServer, CenterDiv } from "../MainLayout"
import styled from 'styled-components'
import _, { Dictionary } from 'lodash'
import { VideoSearchResults, SearchHelp, NoResult } from './VideoSearchResults'
import { useMediaQuery } from 'react-responsive'
import { Button } from '../Button'
import { FilterList as IconFilter, Close as IconClose, List as IconList } from '@styled-icons/material'
import { TopSiteBar } from '../SiteMenu'
import { IdToken } from '@auth0/auth0-spa-js'
import { EsCfg } from '../../common/Elastic'
import queryString from 'query-string'
import { saveSearch } from '../../common/YtApi'
import { UserContext } from '../UserContext'

const Page = styled.div`
  display:flex; flex-direction:column;
  height:100vh;
`

const SearchAndFilterPane = styled.div`
    display:flex; flex-direction:column;
    min-height:0;
    flex-direction:row;
`

const ContentPane = styled.div`
    padding:0;
    display:flex; flex-direction:column;
    max-width:1100px;
    margin:0 auto;
    flex: 1 100%;
`

const FiltersPane = styled.div`
    display:none; 
    flex-flow:column;
    overflow-y: auto;
    justify-content:start;
    align-content:start;
    background-color: ${theme.backColorBolder};
    min-height:0px; /*needed for column wrap to kick in*/
    padding:0.5em 0.5em;

    @media (min-width: 580px) {
      flex-flow: column wrap;
    }
    
    @media (${media.width.medium}) {
      flex-flow: column;
      min-width:310px;
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

const SearchTextBoxStyle = styled.div`
    width:100%;
    input {
      width:100%;
      font-size:1.5em;
    }
`

const SearchPane = styled.div`
  padding: 0.5em 1em;
`

const SearchSelectionPane = styled.div`
  display: flex;
  justify-content: space-between;
  padding: 0.5em 0 0;
`

const ResultsPane = styled.div`
    position:relative;
    height:100%; width:100%;
    overflow-y:scroll;
    select {
        background:${theme.backColor};
        outline:1px solid #333;
        color:${theme.fontColor};
    }
`

const FilteredListStyle: React.CSSProperties = {
  display: "flex",
  flexDirection: "column"
}

interface SortValue { field: string, sort: 'asc' | 'desc' }

const sortOptions: Dictionary<string> = {
  'Relevance': '_score',
  'Views': 'views',
  'Uploaded': 'upload_date'
}

export const VideoSearch = ({ esCfg }: CProps<{ esCfg: EsCfg }>) => {
  const [sort, setSort] = useState<SortValue>({ field: '_score', sort: 'desc' })
  const [filterOpened, setFilterOpened] = useState(false)
  const filterOnRight = useMediaQuery({ query: `(${media.width.medium})` })
  const filterVisible = filterOnRight || filterOpened
  const resultsVisible = filterOnRight || !filterOpened

  if (isGatsbyServer()) return <div></div>

  return (
    <ReactiveBase
      app="caption2"
      url={esCfg.url}
      credentials={esCfg.creds}
      themePreset="dark"
      theme={{
        typography: { fontSize: theme.fontSize, fontFamily: theme.fontFamily },
        colors: { textColor: theme.fontColor, primaryColor: theme.themeColor }
      }}
    >
      <Page>
        <TopSiteBar showLogin style={{ flex: '0 0 auto' }} />
        <SearchAndFilterPane style={{ flex: '1 1 auto', flexDirection: filterOpened && !filterOnRight ? 'column' : 'row' }}>
          <ContentPane>
            <SearchPane>
              <SearchTexBox />
              <SearchSelectionPane>
                <SelectedFilters />
                {!filterOnRight && <div style={{ verticalAlign: 'top' }} >
                  <Button
                    label={filterOpened ? "Results" : "Filter"}
                    icon={filterOpened ? <IconList /> : <IconFilter />} onclick={_ => setFilterOpened(!filterOpened)} />
                </div>}
              </SearchSelectionPane>
            </SearchPane>

            <ResultsPane id="results" style={{ display: resultsVisible ? 'block' : 'none' }}>
              <StateProvider strict={false} >
                {({ searchState }) => {
                  const query = searchState?.q?.value
                  if (!query) return SearchHelp

                  return <ReactiveList
                    componentId="result"
                    react={{ and: ['q', 'views', 'sort', 'ideology', 'channel', 'upload'] }}
                    infiniteScroll
                    scrollTarget="results"
                    size={50}
                    dataField={sort.field}
                    sortBy={sort.sort}
                    showResultStats={false}
                    showLoader={false}
                    renderNoResults={() => <NoResult />}
                    onError={e => console.log("search error:", e)}
                  >
                    {(renderState) => <VideoSearchResults renderState={renderState} query={query} />}
                  </ReactiveList>
                }}
              </StateProvider>
            </ResultsPane>
          </ContentPane>
          <FiltersPaneComponent setSort={setSort} sort={sort} style={{ display: filterVisible ? 'flex' : 'none' }} />
        </SearchAndFilterPane>
      </Page>
    </ReactiveBase >
  )
}

const SearchTexBox: React.FunctionComponent = () => {
  const [query, setQuery] = useState<string>()
  const [inputValue, setInputValue] = useState<string>()
  const ref = useRef<HTMLInputElement>()

  const userCtx = useContext(UserContext)
  const user = userCtx?.user

  return <SearchTextBoxStyle>

    {/* the default behavior of DataSearch is to show results as you type. We can override thi behavior by using a
       controlled component, but this prevents smooth typing on mobile. 
       
       Out solution is to use an invisible controlled DataSearch component and let users type in a vanilla input.
      */}

    <StateProvider strict={false} >
      {({ searchState }) => <input ref={ref} type='text' autoFocus placeholder="search..."
        value={inputValue} onChange={e => setInputValue(e.currentTarget.value)}
        onKeyPress={(e: KeyboardEvent<HTMLInputElement>) => {
          if (e.key === "Enter") {
            const q = e.currentTarget.value
            setQuery(q)
            saveSearch({
              origin: location.origin,
              email: user.email,
              query: q,
              channels: searchState.channel.value,
              ideologies: searchState.ideology.value,
              updated: new Date()
            })
          }
        }} />
      }
    </StateProvider>

    <DataSearch
      componentId="q"
      filterLabel="Search"
      dataField={["caption"]}
      placeholder="Search video captions"
      autosuggest={false}
      showIcon={false}
      searchOperators
      queryFormat='and'
      style={{ display: 'none' }}
      URLParams
      onChange={(value: string) => {
        setInputValue(value)
        setQuery(value)
      }}
      value={query}
    />


  </SearchTextBoxStyle >
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
      dataField="ideology"
      showCheckbox showCount showMissing
      showSearch={false}
      react={{ and: ['q', 'views', 'upload'] }}
      style={FilteredListStyle}
      defaultQuery={_ => ({
        aggs: {
          "ideology": {
            aggs: {
              video_count: {
                cardinality: {
                  field: "video_id"
                }
              }
            },
            terms: {
              field: "ideology",
              size: 50,
              order: { "video_count": "desc" },
              missing: "N/A"
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
                  field: "video_id"
                }
              }
            },
            terms: {
              field: "channel_title.keyword",
              size: 100,
              order: { "video_count": "desc" },
              missing: "N/A"
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

