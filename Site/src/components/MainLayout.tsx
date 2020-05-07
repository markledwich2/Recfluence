import { StaticQuery, graphql } from "gatsby"
import React from "react"
import Helmet from "react-helmet"
import '../styles/main.css'
import styled from 'styled-components'
import { hsl, rgb } from "d3"
import { RouteComponentProps as CP } from "@reach/router"
import { UserContextProvider } from './UserContext'


const themeIntent: ThemeIntent = {
  fontFamily: 'Segoe UI, Tahoma',
  themeColor: '#249e98',
  dark: true
}

export const theme = makeTheme(themeIntent)

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
              <UserContextProvider authOptions={{
                domain: process.env.AUTH0_DOMAIN,
                client_id: process.env.AUTH0_CLIENTID,
                redirect_uri: process.env.AUTH0_CALLBACK,
                responseType: "token id_token",
                scope: "openid profile email",
              }} children={children} />
            </MainStyleDiv>
          </>
        )}
      />
    )
  }
}

export function isGatsbyServer() { return typeof window === 'undefined' }


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
  const backColor = dark ? '#111' : `#fff`
  const themeColor = intent.themeColor
  const themeColorBolder = bolder(themeColor, 2)
  return {
    fontFamily: intent.fontFamily,
    fontColor: fontColor,
    fontColorBolder: bolder(fontColor, 2),
    fontColorSubtler: subtler(fontColor, 2),
    fontSize: '1em',
    themeColor: themeColor,
    themeColorSubtler: subtler(themeColor, 2),
    themeColorBolder: themeColorBolder,
    backColor: backColor,
    backColorBolder: bolder(backColor, 2),
    backColorBolder2: bolder(backColor, 5),
    backColorBolder3: bolder(backColor, 6),
    backColorTransparent: Object.assign(rgb(backColor), { opacity: 0.5 }).toString(),
    fontThemeShadow: `0 0 0.2em ${withOpacity(themeColorBolder, 0.99)}, 0 0 1em ${withOpacity(themeColor, 0.3)}, 0 0 0.4em ${withOpacity(themeColorBolder, 0.7)}`,
  }
}

