import * as React from "react"
import { ChannelRelationsPage } from "../components/ChannelRelationsPage"
import { StaticQuery, graphql } from "gatsby"
import { MainLayout } from "../components/MainLayout"
import { Router } from "@reach/router"
import { Video } from "../components/VideoPage"

// const App = () => {
//   return (
//     <StaticQuery
//       query={graphql`
//         query DataUrlQuery {
//           site {
//             siteMetadata {
//               dataUrl,
//               funcUrl
//             }
//           }
//         }
//       `}
//       render={(data) => (
//         <MainLayout>
//           <ChannelRelationsPage dataUrl={data.site.siteMetadata.dataUrl} />
//         </MainLayout>
//       )}
//     />
//   )
// }

const App = () => (
  <MainLayout>
    <Router>
      <ChannelRelationsPage path="/" dataUrl={process.env.RESULTS_URL} />
      <Video path="video/:videoId"></Video>
    </Router>
  </MainLayout>)

export default App
