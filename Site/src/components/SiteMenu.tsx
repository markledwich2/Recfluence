import React, { useContext, FunctionComponent } from 'react'
import styled from 'styled-components'
import logo from '../images/recfluence_word.svg'
import { ytTheme, safeLocation } from './MainLayout'
import { Link } from 'gatsby'
import { Person as IconUser, ExitToApp as IconLogout } from '@styled-icons/material'
import { UserContext } from './UserContext'
import { UserMenu } from './UserMenu'

const HeaderBar = styled.div`
  padding:6px 5px 3px 10px;
  display:flex;
  width:100%;
  background-color:${ytTheme.backColorBolder};
`

const NavStyle = styled.nav`
  display:flex;
  justify-content:space-between;
  align-items:center; /* vertical alignment of child items. I'm crap a googling, or this is a secret */
  width:100%;

  .menu-item {
    padding-left: 1em;
  }
  
  .text-links a {
    text-transform: uppercase;
  }

  .text-links a.active {
    color: ${ytTheme.themeColorBolder};
    text-shadow: ${ytTheme.fontThemeShadow};
  }

  /* .icon, .text-icon {
    height: 1.7em;
    position:relative;
    top:0.2em;
  } */
`

const LogoStyle = styled.img`
  height:30px;
  margin: auto 0px 0px 2px;
`

interface ShowLoginProps {
  showLogin?: boolean
  //onLogin?: (user: IdToken) => void
}

interface HeaderBarProps extends ShowLoginProps {
  style?: React.CSSProperties
}

export const HomeLogo = () => <Link to='/'><LogoStyle src={logo} title='Recfluence' /></Link>

export const TopSiteBar = ({ showLogin, style }: HeaderBarProps) => <HeaderBar style={style}>
  <HomeLogo />
  <SiteLinks showLogin={showLogin} />
</HeaderBar>

export const LinkA: FunctionComponent<{ to: string, className?: string }> = ({ to, children, className }) => {
  const active = safeLocation()?.pathname == to
  const classes = [className + ' menu-item', active ? 'active' : null].filter(c => c).join(" ")

  // we use A instead of Link because Flows pages is super heavy and navigating causes large javascript slowdowns
  return <a href={to} className={classes}>{children}</a>
}

export const SiteLinks = ({ showLogin }: ShowLoginProps) => {
  const userCtx = useContext(UserContext)
  const user = userCtx?.user

  return <NavStyle>
    <div className="text-links">
      <LinkA to='/'>Flows</LinkA>
      <LinkA to='/search/'>Search</LinkA>
    </div>
    {showLogin && <UserMenu />}
  </NavStyle>
}