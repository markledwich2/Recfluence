import React, { useContext } from "react"
import styled from 'styled-components'
import logo from '../images/recfluence_word.svg'
import { theme } from './MainLayout'
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

  a, .menu-text {
    padding: 0 0.5em;
    vertical-align: bottom;
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
  style: React.CSSProperties
}

export const HomeLogo = () => <Link to='/'><LogoStyle src={logo} title="Recfluence" /></Link>

export const TopSiteBar = ({ showLogin, style }: HeaderBarProps) => <HeaderBar style={style}>
  <HomeLogo />
  <SiteLinks showLogin={showLogin} />
</HeaderBar>

export const SiteLinks = ({ showLogin }: ShowLoginProps) => {
  const userCtx = useContext(UserContext)
  const user = userCtx?.user

  return <>
    <NavStyle>
      <div>
        <Link to='/' activeClassName="active">Flows</Link>
        <Link to='/search' activeClassName="active">Search</Link>
      </div>
      {showLogin &&
        <div>
          {user ?
            <>
              <IconUser className="text-icon" /> <span className="menu-text">{user?.name ?? '(no name)'}</span>
              <a onClick={() => userCtx?.logOut()}><IconLogout className="icon" title="Log out" /></a>
            </>
            : <a onClick={async () => {
              if (!userCtx) return
              await userCtx.logIn()
            }}>Log&nbsp;In</a>}
        </div>
      }
    </NavStyle>
  </>
}