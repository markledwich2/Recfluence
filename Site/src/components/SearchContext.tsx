import { FunctionComponent } from 'react'
import { EsCfg } from '../common/Elastic'
import React from 'react'

export const EsContext = React.createContext<EsCfg>(null)
export const EsContextProvider: FunctionComponent<{ esCfg: EsCfg }> =
  ({ children, esCfg }) => <EsContext.Provider value={esCfg} children={children} />