import * as React from "react"
import { useContext, useEffect, useState } from 'react'
import { UserContext } from '../UserContext'
import { channelsForReview, ChannelForReview } from '../../common/YtApi'
import { Spinner } from '../Spinner'

export const ChannelReview = () => {
  const userCtx = useContext(UserContext)
  const [forReview, setForReview] = useState<ChannelForReview[]>(null)

  useEffect(() => {
    const go = async () => {
      if (!userCtx?.user) {
        console.log('no user yet')
        return
      }
      console.log('loading channels')
      var channels = await channelsForReview(userCtx?.user.email)
      setForReview(channels)
    }
    go()
  }, [userCtx])

  if (!userCtx?.user) return <span>Please log in to review channels</span>
  if (forReview == null) return <><Spinner size='50px' /></>

  // fetch some channels for review & list existing
  return <div>{forReview.map(c => <div key={c.channelId}>{c.channelTitle}</div>)}</div>
}