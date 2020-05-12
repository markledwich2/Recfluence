import { StaticQuery, graphql } from "gatsby"
import React, { FunctionComponent, useEffect } from "react"
import Helmet from "react-helmet"
import '../styles/main.css'
import styled from 'styled-components'
import { hsl, rgb } from "d3"
import { RouteComponentProps } from "@reach/router"
import { UserContextProvider } from './UserContext'

export function isGatsbyServer() { return typeof window === 'undefined' }

export function safeLocation(): Location { return isGatsbyServer() ? null : location }

const themeIntent: ThemeIntent = {
  fontFamily: 'Segoe UI, Tahoma',
  themeColor: '#249e98',
  dark: true
}

export const theme = makeTheme(themeIntent)

export const MainLayout: FunctionComponent<RouteComponentProps> = ({ children }) => {
  useEffect(() => {

    // incase any elements expand beyond the root div, style the nody background
    const s = document.body.style
    s.background = theme.backColor
  })
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
        <UserContextProvider authOptions={{
          domain: process.env.AUTH0_DOMAIN,
          client_id: process.env.AUTH0_CLIENTID,
          responseType: "token id_token",
          scope: "openid profile email",
          cacheLocation: 'localstorage'
        }}>
          <Helmet>
            <title>{data.site.siteMetadata.title}</title>
          </Helmet>
          <MainStyleDiv>
            {children}
          </MainStyleDiv>
        </UserContextProvider>
      )}
    />
  )
}

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

  a {
    color: ${theme.themeColor};
    text-decoration: none;
  }
  a:hover {
      cursor: pointer;
      color: ${theme.themeColorBolder};
      text-shadow: ${theme.fontThemeShadow};
    }

  input {
    border:solid 1px ${theme.backColorBolder2};
    padding: 0.4em 0.6em;
    border-radius:5px;

    :focus {
      border:solid 1px ${theme.themeColorSubtler};
      outline:none;
    }
  }

  background-color: ${theme.backColor};
  color: ${theme.fontColor};

  *::-webkit-scrollbar {
    width: 0.5em;
  }

  *::-webkit-scrollbar-thumb {
    background-color: ${theme.backColorBolder2};
  }

  select option, input {
    background-color: ${theme.backColorBolder};
    color: ${theme.fontColor};
  }

  b, h1, h2, h3 {
    color: ${theme.fontColorBolder};
  }
`

export const CenterDiv = styled.div`
    position:absolute;
    left:50%;
    top:50%;
    transform:translate(-50%, -50%);
    background: none;
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
  fontColorSubtler: string,
  fontSize: string,
  fontThemeShadow: string,
  themeColor: string,
  themeColorSubtler: string,
  themeColorBolder: string,
  backColor: string,
  backColorBolder: string,
  backColorBolder2: string,
  backColorBolder3: string,
  backColorTransparent: string
}

function makeTheme(intent: ThemeIntent): Theme {
  const { dark } = intent
  const fontColor = dark ? '#bbb' : '#222'
  const subtler = (color: string, k: number) => dark ? hsl(color).darker(k).toString() : hsl(color).brighter(k).toString()
  const bolder = (color: string, k: number) => dark ? hsl(color).brighter(k).toString() : hsl(color).darker(k).toString()
  const withOpacity = (color: string, opacity: number) => Object.assign(rgb(color), { opacity: opacity })
  const backColor = dark ? '#111' : `#eee`
  const themeColor = intent.themeColor
  const themeColorBolder = bolder(themeColor, 2)
  return {
    fontFamily: intent.fontFamily,
    fontColor: fontColor,
    fontColorBolder: bolder(fontColor, 1),
    fontColorSubtler: subtler(fontColor, 0.5),
    fontSize: '1em',
    themeColor: themeColor,
    themeColorSubtler: subtler(themeColor, 2),
    themeColorBolder: themeColorBolder,
    backColor: backColor,
    backColorBolder: bolder(backColor, 1.5),
    backColorBolder2: bolder(backColor, 3),
    backColorBolder3: bolder(backColor, 4),
    backColorTransparent: Object.assign(rgb(backColor), { opacity: 0.5 }).toString(),
    fontThemeShadow: `0 0 0.2em ${withOpacity(themeColorBolder, 0.99)}, 0 0 1em ${withOpacity(themeColor, 0.3)}, 0 0 0.4em ${withOpacity(themeColorBolder, 0.7)}`,
  }
}

