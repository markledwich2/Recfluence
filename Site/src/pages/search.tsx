import * as React from "react"
import { Router, RouteComponentProps as CP } from "@reach/router"
import { Video } from "../components/VideoPage"
import { VideoSearch } from '../components/VideoSearch'
import { MainLayout } from '../components/MainLayout'

const SearchPage = () => (
    <MainLayout path="video">
        <VideoSearch />
    </MainLayout>
)
export default SearchPage


