import React, { useEffect, useState } from "react"
import { RouteComponentProps, Router } from "@reach/router"
import algoliasearch from "algoliasearch"
import { InstantSearch, SearchBox, Hits, RefinementList } from "react-instantsearch-dom"
import { ReactiveBase, CategorySearch, ReactiveList, ResultCard, DataSearch } from '@appbaseio/reactivesearch'
import { CaptionDb, VideoDb } from "../common/DbModel"

interface Props extends RouteComponentProps { }

export const VideoSearchOld = (p: Props) => {
    const onSubmit = (e: React.FormEvent<HTMLFormElement>) => {
        console.log('hey')
    }


    return (<div style={{ textAlign: "center", marginTop: "1em" }}>
        <form onSubmit={onSubmit}>
            <input type="text" placeholder="search" style={{ width: "80%", maxWidth: "1024px" }}  ></input>
        </form>
    </div>)
}


export const VideoSearchAngola = (p: Props) => {
    const searchClient = algoliasearch(
        'HRBKZQ5S8L',
        '9fc83451122ec78f07d9ccbd436061b3'
    )

    return (
        <InstantSearch searchClient={searchClient} indexName="captions">
            <SearchBox />
            <Hits />
            <RefinementList attribute="channel_title" />
            <RefinementList attribute="ideology" />
        </InstantSearch>
    )
}

interface IndexedCaption extends VideoDb, CaptionDb { }

export const VideoSearch = (p: Props) => {
    return (
        <ReactiveBase
            app="caption"
            // transformRequest={(t) => { t.headers["Content-Length"] = t.body.length }}
            url="https://8999c551b92b4fb09a4df602eca47fbc.westus2.azure.elastic-cloud.com:9243"
            credentials="public:5&54ZPnh!hCg"
            themePreset="dark"
        >
            <CategorySearch
                componentId="searchbox"
                dataField="caption"
                placeholder="Search video captions"
                categoryField="channel_title.keyword"
            />
            <ReactiveList
                componentId="result"
                react={{ and: ['searchbox'] }}
                renderItem={(res) => <div>{res.video_title}</div>}
                dataField="video_title"
            />
        </ReactiveBase>
    )
}