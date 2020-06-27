import React from 'react'

export const channelUrl = (channelId: string) => `https://www.youtube.com/channel/${channelId}`

export const ChannelLogo = ({ thumb, channelId, style }: { thumb: string, channelId: string, style?: React.CSSProperties }) => <>
  <a href={channelUrl(channelId)} target="blank">
    <img src={thumb} style={{ objectFit: 'contain', clipPath: 'circle()', ...style }} />
  </a></>
