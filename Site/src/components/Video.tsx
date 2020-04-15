import { ChannelData, YtModel } from "../common/YtModel"
import React from "react"
import { compactInteger } from "humanize-plus"
import dateFormat from "dateformat"
import { ColEx } from "../common/Dim"
import { color } from 'd3'
import styled from 'styled-components'
import * as dateformat from 'dateformat'

interface VideoChannelTitleProps {
  Channel: ChannelData
}

export const VideoChannelTitle = (props: VideoChannelTitleProps) => {
  const c = props.Channel
  const dim = new YtModel().channels
  let tags = c.tags.length == 0 ? ['None'] : c.tags.map(t => YtModel.tagAlias[t] ?? t).filter(t => t != '_')
  let lrCol = dim.col('lr')
  let labelFunc = ColEx.labelFunc(lrCol)
  let colorFunc = ColEx.colorFunc(lrCol)
  let lrColor = (v: string) => color(colorFunc(v)).darker(2).hex()

  const Card = styled.div`
    margin: 1em;
    display: flex;
  `

  let dateFormat = (d: Date) => d ? dateformat(d, 'd mmm yyyy') : d

  return (<Card>
    <a href={`https://www.youtube.com/channel/${c.channelId}`} target="blank">
      <img src={c.thumbnail} style={{ height: '7em', marginRight: '1em', clipPath: 'circle()' }} />
    </a>
    <div>
      <div><b>{c.title}</b></div>
      <div>
        <><b>{compactInteger(c.dailyViews)}</b> daily views <i>{dateFormat(c.publishedFrom)}</i> to <i>{dateFormat(c.publishedTo)}</i></>
      </div>
      <div>
        <b>{compactInteger(c.subCount)}</b> subscribers
          </div>
      <div><span key={c.lr} className={'tag'} style={{ backgroundColor: lrColor(c.lr) }}>{labelFunc(c.lr)}</span>{tags.map(t => (<span key={t} className={'tag'}>{t}</span>))}</div>
    </div>
  </Card>)
}