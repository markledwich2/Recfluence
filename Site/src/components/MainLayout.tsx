import { StaticQuery, graphql } from "gatsby"
import React from "react"
import Helmet from "react-helmet"
import '../styles/main.css'
import styled from 'styled-components'
import { rgb, color as dcolor } from "d3"

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


const themeIntent: ThemeIntent = {
  fontFamily: 'Segoe UI, Tahoma',
  themeColor: '#249e98',
  dark: true
}

export const theme = makeTheme(themeIntent)

export const BasicPageDiv = styled.div`
  max-width:1024px;
  margin: 0 auto;
  mark {
    background-color: ${theme.themeColorSubtler};
    color: ${theme.fontColor};
  }
`

interface ThemeIntent {
  fontFamily: string
  themeColor: string
  dark: boolean
}

interface Theme {
  fontFamily: string,
  fontColor: string,
  fontColorBolder: string,
  fontSize: string,
  themeColor: string,
  themeColorSubtler: string,
}

function makeTheme(intent: ThemeIntent): Theme {
  const { dark } = intent
  const fontColor = dark ? '#bbb' : '#222'
  const subtler = (color: string, k: number) => dark ? dcolor(color).darker(k).toString() : dcolor(color).brighter(k).toString()
  const bolder = (color: string, k: number) => dark ? dcolor(color).brighter(k).toString() : dcolor(color).darker(k).toString()
  return {
    fontFamily: intent.fontFamily,
    fontColor: fontColor,
    fontColorBolder: bolder(fontColor, 2),
    fontSize: '1em',
    themeColor: intent.themeColor,
    themeColorSubtler: subtler(intent.themeColor, 2)
  }
}

