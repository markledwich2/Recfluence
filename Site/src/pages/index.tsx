import * as React from 'react'
import { ChannelRelationsPage } from '../components/ChannelRelationsPage'
import Helmet from 'react-helmet'
import { StaticQuery, graphql } from 'gatsby'

const App = () => {
  return (
    <StaticQuery query={graphql`
    query SiteTitleQuery {
      site {
        siteMetadata {
          title
          dataUrl
        }
      }
    }
  `} render={
        (data:Query) => (
          <>
            <Helmet>
              <title>{data.site.siteMetadata.title}</title>
            </Helmet>
            <ChannelRelationsPage dataUrl={data.site.siteMetadata.dataUrl} />
          </>
        )
      } />)
}

interface Query {
  site: Site
}

interface Site {
  siteMetadata:SiteMetadata
}

interface SiteMetadata {
  title:string
  dataUrl:string
}

export default App