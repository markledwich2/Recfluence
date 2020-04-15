import { StaticQuery, graphql } from "gatsby"
import React from "react"
import Helmet from "react-helmet"


export class MainLayout extends React.Component<{}, {}> {
  render() {
    const { children } = this.props

    return (
      <StaticQuery
        query={graphql`
          query SiteTitleQuery {
            site {
              siteMetadata {
                title
              }
            }
          }
        `}
        render={data => (
          <>
            <Helmet>
              <title>{data.site.siteMetadata.title}</title>
            </Helmet>
            {children}
          </>
        )}
      />
    )
  }
}
