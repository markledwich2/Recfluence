import { StaticQuery, graphql } from "gatsby"
import React from "react"
import Helmet from "react-helmet"
import '../styles/main.css'
import styled from 'styled-components'
import { color as dcolor, hsl } from "d3"
import { Router, RouteComponentProps as CP } from "@reach/router"

export class MainLayout extends React.Component<CP<{}>, {}> {
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
            <MainStyleDiv>
              {children}
            </MainStyleDiv>
          </>
        )}
      />
    )
  }
}

export function isGatsbyServer() { return typeof window === 'undefined' }


const themeIntent: ThemeIntent = {
  fontFamily: 'Segoe UI, Tahoma',
  themeColor: '#249e98',
  dark: true
}

export const theme = makeTheme(themeIntent)

export const size = {
  small: 600,
  medium: 800,
  large: 1000,
  xlarge: 1200
}

export const media = {
  width: {
    small: `min-width: ${size.small}px`,
    medium: `min-width: ${size.medium}px`,
    large: `min-width: ${size.large}px`
  },
  height: {
    small: `min-height: ${size.small}px`,
    medium: `min-height: ${size.medium}px`,
    large: `min-height: ${size.large}px`,
    xlarge: `min-height: ${size.xlarge}px`
  }
}

const MainStyleDiv = styled.div`
  mark {
    background-color: ${theme.themeColorSubtler};
    color: ${theme.fontColor};
  }
`

export const TextPage = styled.div`
  max-width:1024px;
  margin: 0 auto;
  padding-top: 1em;
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
  backColor: string,
  backColor1: string,
  backColor2: string
}

function makeTheme(intent: ThemeIntent): Theme {
  const { dark } = intent
  const fontColor = dark ? '#bbb' : '#222'
  const subtler = (color: string, k: number) => dark ? hsl(color).darker(k).toString() : hsl(color).brighter(k).toString()
  const bolder = (color: string, k: number) => dark ? hsl(color).brighter(k).toString() : hsl(color).darker(k).toString()
  const backColor = dark ? '#111' : `#fff`
  return {
    fontFamily: intent.fontFamily,
    fontColor: fontColor,
    fontColorBolder: bolder(fontColor, 2),
    fontSize: '1em',
    themeColor: intent.themeColor,
    themeColorSubtler: subtler(intent.themeColor, 2),
    backColor: backColor,
    backColor1: bolder(backColor, 1),
    backColor2: bolder(backColor, 4)
  }
}

