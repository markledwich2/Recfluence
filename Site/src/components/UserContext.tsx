
import createAuth0Client, { Auth0Client, Auth0ClientOptions } from '@auth0/auth0-spa-js'
import React, { useState, useEffect, FunctionComponent } from 'react'

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
    async function init() {
      const c = await getOrInitClient()
      const u: User = await c.getUser()
      setUser(u)
    }
    init()
  }, []) // only runs on mount

  const logIn = async () => {
    const c = await getOrInitClient()
    await c.loginWithPopup({ display: 'page' })
    const u: User = await c.getUser()
    setUser(u)
    return u
  }

  const logOut = async () => {
    const c = await getOrInitClient()
    await c.logout({ localOnly: true })
    setUser(null)
  }

  return <UserContext.Provider value={{ user, logIn, logOut }} children={children} />
}