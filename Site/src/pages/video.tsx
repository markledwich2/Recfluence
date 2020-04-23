import * as React from "react"
import { Router, RouteComponentProps as CP } from "@reach/router"
import { Video } from "../components/VideoPage"
import { VideoSearch } from '../components/VideoSearch'
import { MainLayout } from '../components/MainLayout'

const VideoPage = () => (
    <Router >
        <MainLayout path="video">
            <Video path="watch/:videoId" />
            <VideoSearch path="search" />

            {/* strange behavior on production builds:
            The index page is pre-rendered by gatsby and reloaded with real pages on load.
            This causes layout artifacts and flickering and a blank page is a workaround */}
            <BlankPage path="/" />

            <VideoPageNotFound default />
        </MainLayout>
    </Router>
)
export default VideoPage

const VideoPageNotFound = (props: CP<{}>) => <h1>The requested video page couldn't be found</h1>

const BlankPage = (props: CP<{}>) => <></>
