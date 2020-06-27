import * as React from "react"
import { ChannelRelationsPage } from "../components/channel_relations/ChannelRelationsPage"
import { MainLayout } from "../components/MainLayout"
import { uri, Uri } from '../common/Uri'
import { resultsUrl } from '../common/YtApi'


const App = () => (
  <MainLayout>
    <ChannelRelationsPage dataUrl={resultsUrl} />
  </MainLayout>
)

export default App