import * as React from "react"
import { Router, RouteComponentProps as CP } from "@reach/router"
import { Video } from "../components/VideoPage"
import { VideoSearch } from '../components/VideoSearch'
import { MainLayout } from '../components/MainLayout'

const VideoPage = () => (
    <Router >
        <MainLayout path="video">
            <Video path=":videoId" />
            <VideoSearch path="/" />
            <VideoPageNotFound default />
        </MainLayout>
    </Router>
)
export default VideoPage

const VideoPageNotFound = (props: CP<{}>) => <h1>The requested video page couldn't be found</h1>

