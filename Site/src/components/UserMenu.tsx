import React, { useContext, FunctionComponent, useState, useRef } from 'react'
import styled from 'styled-components'
import { ExitToApp as IconLogout } from '@styled-icons/material'
import { UserContext } from './UserContext'
import { theme } from './MainLayout'
import useOutsideClick from './OutsideClick'


const ProfileIcon = styled.img`
  border-radius:50%;
  height:25px;
  :hover {
    cursor: pointer;
  }
`

const MainDiv = styled.div`
  position:relative;
`

const UserMenuDiv = styled.div`
  position:absolute;
  
  top: 27px;
  right:0px;
  z-index:1;
  background-color:${theme.backColorBolder};
  box-shadow: 0 0 10px rgba(0, 0, 0, 0.8);

  width:200px;
  max-width:90vw;

  div.profile {
    text-align:center;
    padding:1em;
  }

  li {
    list-style: none;

    padding:0.5em;

    a {
      color: ${theme.fontColor};
    }

    .icon {
      height: 1.2em;
      position: relative;
      top: -0.05em;
      padding-right: 0.5em;
    }

    :hover {
      background-color:${theme.backColorBolder2};
      cursor: pointer;
    }
  }
`

export const UserMenu = () => {
  const ctx = useContext(UserContext)
  const user = ctx?.user
  const [isMenuOpen, setIsMenuOpen] = useState(false)
  const ref = useRef()

  useOutsideClick(ref, () => setIsMenuOpen(false))

  if (!user) return <MainDiv><a className='menu-item' onClick={() => ctx?.logIn()}>Sign&nbsp;In</a></MainDiv>

  return <MainDiv ref={ref} >
    <ProfileIcon src={user?.picture} onClick={() => {
      setIsMenuOpen(!isMenuOpen)
    }} />
    <UserMenuDiv style={{ display: isMenuOpen ? 'block' : 'none' }}>
      <div className="profile">
        <ProfileIcon src={user?.picture} style={{ height: '60px' }} /><br />
        {user?.name}<br />
        <small>{user?.email}</small>
      </div>
      <ul>
        <li onClick={() => ctx?.logOut()}><IconLogout className='icon' title='Log out' /> Sign Out</li>
      </ul>
    </UserMenuDiv>
  </MainDiv>
}

