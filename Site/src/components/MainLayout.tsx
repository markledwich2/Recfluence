import { StaticQuery, graphql } from "gatsby"
import React, { FunctionComponent, useEffect } from "react"
import Helmet from "react-helmet"
import '../styles/main.css'
import styled from 'styled-components'
import { hsl, rgb } from "d3"
import { RouteComponentProps } from "@reach/router"
import { UserContextProvider } from './UserContext'
import { Theme as STheme, PropsWithStyles } from 'react-select/lib/types'
import { StylesConfig } from 'react-select/lib/styles'
import { ToastProvider } from 'react-toast-notifications'

export function isGatsbyServer() { return typeof window === 'undefined' }

export function safeLocation(): Location { return isGatsbyServer() ? null : location }

const themeIntent: ThemeIntent = {
  fontFamily: 'Segoe UI, Tahoma',
  themeColor: '#249e98',
  dark: true
}

export const ytTheme = makeTheme(themeIntent)

export const MainLayout: FunctionComponent<RouteComponentProps> = ({ children }) => {
  useEffect(() => {

    // incase any elements expand beyond the root div, style the nody background
    const s = document.body.style
    s.background = ytTheme.backColor
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
          <ToastProvider>
            <Helmet>
              <title>{data.site.siteMetadata.title}</title>
            </Helmet>
            <MainStyleDiv>
              {children}
            </MainStyleDiv>
          </ToastProvider>
        </UserContextProvider>
      )}
    />
  )
}

export const ContentPageDiv = styled.div`
  width:100vw;
  max-width:1024px;
  margin:0 auto;
  position:relative;
  display:flex;
  flex-direction:column;
  min-height:0px; /* to get inner overflow-y:auto to work: intermediate divs must all be display:flex and min-hight:0 */
`

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
    background-color: ${ytTheme.themeColorSubtler};
    color: ${ytTheme.fontColor};
  }

  a {
    color: ${ytTheme.themeColor};
    text-decoration: none;
  }
  a:hover {
      cursor: pointer;
      color: ${ytTheme.themeColorBolder};
      text-shadow: ${ytTheme.fontThemeShadow};
    }


  input[type=number]::-webkit-inner-spin-button {
    -webkit-appearance: none;
  }

  input, textarea {
    border:solid 1px ${ytTheme.backColorBolder2};
    padding: 0.5em 0.6em;
    border-radius:5px;

    :focus {
      border:solid 1px ${ytTheme.themeColorSubtler};
      outline:none;
    }
  }
  

  input[type=submit] {
    text-transform: uppercase;
    font-weight: bolder;
    padding: .5em 1em;

    :hover {
        background-color: ${ytTheme.backColorBolder3};
    }

    :disabled {
      color: ${ytTheme.fontColorSubtler2};
      background-color: ${ytTheme.backColorBolder};
    }
  }

  textarea {
    font-family: ${ytTheme.fontFamily}
  }

  background-color: ${ytTheme.backColor};
  color: ${ytTheme.fontColor};

  *::-webkit-scrollbar {
    width: 0.5em;
  }

  *::-webkit-scrollbar-thumb {
    background-color: ${ytTheme.backColorBolder2};
  }

  select option, input, textarea {
    background-color: ${ytTheme.backColorBolder};
    color: ${ytTheme.fontColor};
  }

  h1, h2, h3 {
    color: ${ytTheme.fontColorBolder};
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
  fontColorSubtler2: string,
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
    fontColorSubtler2: subtler(fontColor, 2),
    fontSize: '1em',
    themeColor: themeColor,
    themeColorSubtler: subtler(themeColor, 2),
    themeColorBolder: themeColorBolder,
    backColor: backColor,
    backColorBolder: bolder(backColor, 1.5),
    backColorBolder2: bolder(backColor, 2),
    backColorBolder3: bolder(backColor, 3),
    backColorTransparent: Object.assign(rgb(backColor), { opacity: 0.5 }).toString(),
    fontThemeShadow: `0 0 0.2em ${withOpacity(themeColorBolder, 0.99)}, 0 0 1em ${withOpacity(themeColor, 0.3)}, 0 0 0.4em ${withOpacity(themeColorBolder, 0.7)}`,
  }
}

export const selectStyle: StylesConfig = {
  option: (p, s) => ({ ...p, color: ytTheme.fontColor }),
  container: (p) => ({ ...p, padding: 0, margin: 0, width: '100%' }),
  menu: (p) => ({ ...p, padding: 0, margin: 0 }),
  menuList: (p) => ({ ...p, maxHeight: '800px' }),
}

export const selectTheme = (selectTheme: STheme): STheme => ({
  ...selectTheme,
  spacing: {
    ...selectTheme.spacing,

  },
  colors: {
    /*
   * multiValue(remove)/color:hover
   */
    danger: ytTheme.themeColorSubtler,
    /*
    * multiValue(remove)/backgroundColor(focused)
    * multiValue(remove)/backgroundColor:hover
    */
    //dangerLight: ytTheme.fontColorSubtler,

    /*
     * control/backgroundColor
     * menu/backgroundColor
     * option/color(selected)
     */
    neutral0: ytTheme.backColor,

    /*
      * control/backgroundColor(disabled)
     */
    neutral5: ytTheme.backColor,

    /*
     * control/borderColor(disabled)
     * multiValue/backgroundColor
     * indicators(separator)/backgroundColor(disabled)
     */
    neutral10: ytTheme.backColorBolder,

    /*
     * control/borderColor
     * option/color(disabled)
     * indicators/color
     * indicators(separator)/backgroundColor
     * indicators(loading)/color
     */
    neutral20: ytTheme.backColorBolder3,

    /*
     * control/borderColor(focused)
     * control/borderColor:hover
     */
    neutral30: ytTheme.themeColorSubtler,

    /*
     * menu(notice)/color
     * singleValue/color(disabled)
     * indicators/color:hover
     */
    neutral40: ytTheme.themeColorSubtler,

    /*
     * placeholder/color
     */
    neutral50: ytTheme.fontColorSubtler2,

    /*
     * indicators/color(focused)
     * indicators(loading)/color(focused)
     */
    neutral60: ytTheme.themeColorSubtler,

    //neutral70: 'var(--neutral-70)',

    /*
     * input/color
     * multiValue(label)/color
      * singleValue/color
     * indicators/color(focused)
     * indicators/color:hover(focused)
     */
    neutral80: ytTheme.fontColor,

    neutral90: ytTheme.fontColor,

    /*
     * control/boxShadow(focused)
     * control/borderColor(focused)
     * control/borderColor:hover(focused)
     * option/backgroundColor(selected)
     * option/backgroundColor:active(selected)
     */
    primary: ytTheme.backColorBolder2, // 'var(--primary)',

    /*
     * option/backgroundColor(focused)
     */
    primary25: ytTheme.themeColorSubtler,

    /*
     * option/backgroundColor:active
     */
    primary50: ytTheme.themeColorSubtler,

    //primary75: 'var(--primary-75)',
  }
})
