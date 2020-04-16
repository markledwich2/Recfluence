import * as React from "react"
import { ChannelRelationsPage } from "../components/ChannelRelationsPage"
import { StaticQuery, graphql } from "gatsby"
import { MainLayout } from "../components/MainLayout"

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
    <ChannelRelationsPage dataUrl={process.env.RESULTS_URL} />
  </MainLayout>)

export default App
