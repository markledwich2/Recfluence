import * as React from "react";
import { ChannelRelationsPage } from "../components/ChannelRelationsPage";
import { StaticQuery, graphql } from "gatsby";
import { MainLayout } from "../components/MainLayout";

const App = () => {
  return (
    <StaticQuery
      query={graphql`
        query DataUrlQuery {
          site {
            siteMetadata {
              dataUrl
            }
          }
        }
      `}
      render={(data) => (
        <MainLayout>
            <ChannelRelationsPage dataUrl={data.site.siteMetadata.dataUrl} />
        </MainLayout>
      )}
    />
  )
}

export default App;
