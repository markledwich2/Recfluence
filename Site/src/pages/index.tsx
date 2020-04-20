import * as React from "react"
import { ChannelRelationsPage } from "../components/ChannelRelationsPage"
import { StaticQuery, graphql } from "gatsby"
import { MainLayout } from "../components/MainLayout"
import { Router } from "@reach/router"
import { Video } from "../components/VideoPage"
import { VideoSearch } from "../components/VideoSearch"

const App = () => (
  <MainLayout>
    <Router>
      <ChannelRelationsPage path="/" dataUrl={process.env.RESULTS_URL} />
      <VideoSearch path="video"></VideoSearch>
      <Video path="video/:videoId"></Video>
    </Router>
  </MainLayout>)

export default App
