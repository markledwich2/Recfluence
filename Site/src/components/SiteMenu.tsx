import React, { useContext, FunctionComponent } from 'react'
import styled from 'styled-components'
import logo from '../images/recfluence_word.svg'
import { theme, safeLocation } from './MainLayout'
import { Link } from 'gatsby'
import { Person as IconUser, ExitToApp as IconLogout } from '@styled-icons/material'
import { UserContext } from './UserContext'

const HeaderBar = styled.div`
  padding:6px 5px 3px 10px;
  display:flex;
  width:100%;
  background-color:${theme.backColorBolder};
`

const NavStyle = styled.nav`
  display:flex;
  justify-content:space-between;
  align-items:flex-end;
  
  width:100%;
  margin: 0;

  > div {
    margin-bottom: 6px;
  }

  a, .menu-item {
    vertical-align: bottom;
  }

  .menu-item {
    padding: 0 1em;
  }
  
  a {
    text-transform: uppercase;
  }
  a.active {
    color: ${theme.themeColorBolder};
    text-shadow: ${theme.fontThemeShadow};
  }
  .icon, .text-icon {
    height: 1.7em;
    position:relative;
    top:0.2em;
  }
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

  return <>
    <NavStyle>
      <div>
        <LinkA to='/'>Flows</LinkA>
        <LinkA to='/search/'>Search</LinkA>
      </div>
      {showLogin &&
        <div>
          {user ?
            <>
              <span className='menu-item'> {user?.name ?? '(no name)'}
                <a onClick={() => userCtx?.logOut()}> <IconLogout className='icon' title='Log out' /></a>
              </span>
            </>
            : <a className='menu-item' onClick={() => {
              if (!userCtx) return
              userCtx.logIn()
            }}>Log&nbsp;In</a>}
        </div>
      }
    </NavStyle>
  </>
}