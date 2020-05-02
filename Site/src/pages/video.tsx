import * as React from "react"
import { Router, RouteComponentProps as CP } from "@reach/router"
import { Video } from "../components/search/VideoPage"
import { VideoSearch } from '../components/search/VideoSearch'
import { MainLayout } from '../components/MainLayout'

const VideoPage = () => (
    <MainLayout>
        <Router >
            <Video path="video/:videoId" />
            <BlankPage path="video" />
            <VideoPageNotFound default />
        </Router>
    </MainLayout>
)
export default VideoPage

const BlankPage = (props: CP<{}>) => <div></div>
const VideoPageNotFound = (props: CP<{}>) => <h1>The requested video page couldn't be found</h1>

