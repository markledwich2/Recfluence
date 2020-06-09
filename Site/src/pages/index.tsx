import * as React from "react"
import { ChannelRelationsPage } from "../components/channel_relations/ChannelRelationsPage"
import { MainLayout } from "../components/MainLayout"
import { uri, Uri } from '../common/Uri'


// On a build+server, or in prod. the server will breifly show the main page before replacing with the correct rout
// This is because when /video/ is requested, the redirects aren't pointing to /index.html.
// i think I should use a static page 
// https://stackoverflow.com/questions/52051090/gatsbyjs-client-only-paths-goes-to-404-page-when-the-url-is-directly-accessed-in

function resultUrl(): Uri {
  const suffix = process.env.BRANCH_ENV ? `-${process.env.BRANCH_ENV}` : ''
  return uri(process.env.RESULTS_HOST).addPath(`${process.env.RESULTS_CONTAINER}${suffix}`, process.env.RESULTS_PATH)
}

const App = () => (
  <MainLayout>
    <ChannelRelationsPage dataUrl={resultUrl()} />
  </MainLayout>
)

export default App