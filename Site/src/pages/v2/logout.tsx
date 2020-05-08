import React from 'react'
import { isGatsbyServer } from '../../components/MainLayout'
import queryString from 'query-string'

const Logout = () => {
  if (isGatsbyServer()) return
  var returnTo = queryString.parse(location.search).returnTo
  if (!returnTo) return
  location.replace(returnTo.toString())
}