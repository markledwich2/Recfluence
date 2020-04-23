import * as React from "react"
import { ChannelRelationsPage } from "../components/ChannelRelationsPage"
import { MainLayout } from "../components/MainLayout"


// On a build+server, or in prod. the server will breifly show the main page before replacing with the correct rout
// This is because when /video/ is requested, the redirects aren't pointing to /index.html.
// i think I should use a static page 
// https://stackoverflow.com/questions/52051090/gatsbyjs-client-only-paths-goes-to-404-page-when-the-url-is-directly-accessed-in

const App = () => (
  <MainLayout>
    <ChannelRelationsPage dataUrl={process.env.RESULTS_URL} />
  </MainLayout>
)

export default App