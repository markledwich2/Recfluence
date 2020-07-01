
import createAuth0Client, { Auth0Client, Auth0ClientOptions } from '@auth0/auth0-spa-js'
import React, { useState, useEffect, FunctionComponent, useContext } from 'react'
import { isGatsbyServer, ytTheme } from './MainLayout'
import styled from 'styled-components'


export const UserContext = React.createContext<UserCtx>(null)

/** Subset of IdToken we are interested in for recfluence */
export interface User {
  name?: string
  email?: string
  profile?: string
  picture?: string
  website?: string
}

export interface UserCtx {
  /** the logged in user. If null then consider logged out */
  user?: User
  logIn: () => Promise<User>
  logOut: () => Promise<void>
}

export const UserContextProvider: FunctionComponent<{ authOptions: Auth0ClientOptions }> = ({ children, authOptions }) => {
  const [user, setUser] = useState<User>(null)
  const [client, setClient] = useState<Auth0Client>(null)

  const getOrInitClient = async () => {
    if (client) return client
    const c = await createAuth0Client(authOptions)
    setClient(c)
    return c
  }

  useEffect(() => {
    if (isGatsbyServer()) return null
    async function init() {
      try {
        const c = await getOrInitClient()
        const u: User = await c.getUser()
        setUser(u)
      }
      catch (error) {
        console.log('error in UserContextProvider.init', error)
      }
    }
    init()
  }, []) // only runs on mount

  const logIn = async () => {
    try {
      const c = await getOrInitClient()
      await c.loginWithPopup({ display: 'page', prompt: 'select_account' })
      const u: User = await c.getUser()
      setUser(u)
      return u
    }
    catch (error) {
      console.log('error in UserContextProvider.logIn', error)
    }
  }

  const logOut = async () => {
    const c = await getOrInitClient()
    setUser(null)
    c.logout({ localOnly: false, returnTo: location.origin })
  }

  return <UserContext.Provider value={{ user, logIn, logOut }} children={children} />
}

const LoginOverlayDiv = styled.div`
  position: absolute; 
  top:100px; left:0px;
  width:100%;
  display: flex;
  justify-content: space-around;
  align-items: center;

  > div {
    max-width:500px;
    padding: 1em 2em;
    margin: 5em auto;
  }
`

export const LoginOverlay: FunctionComponent<{ verb: string }> = ({ verb, children }) => {
  const { user, logIn } = useContext(UserContext)
  return <>{!user && <LoginOverlayDiv>
    <div style={{ backgroundColor: ytTheme.backColorTransparent }}>
      <big><a onClick={_ => logIn()}>Sign in</a> to {verb}</big><br /><br />
      <div>{children}</div>
    </div>
  </LoginOverlayDiv>}</>
}