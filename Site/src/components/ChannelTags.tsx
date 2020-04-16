import { ColEx } from "../common/Dim"
import { ChannelData, YtModel } from "../common/YtModel"
import { color } from "d3"
import React from "react"

export interface ChannelComponentProps {
    Channel: ChannelData
}

export const ChannelTags = (props: ChannelComponentProps) => {
    const c = props.Channel
    const dim = new YtModel().channels
    let tags = c.tags.length == 0 ? ['None'] : c.tags.map(t => YtModel.tagAlias[t] ?? t).filter(t => t != '_')
    let lrCol = dim.col('lr')
    let labelFunc = ColEx.labelFunc(lrCol)
    let colorFunc = ColEx.colorFunc(lrCol)
    let lrColor = (v: string) => color(colorFunc(v)).darker(2).hex()
    return <div>
        <span key={c.lr} className={'tag'} style={{ backgroundColor: lrColor(c.lr) }}>{labelFunc(c.lr)}</span>
        {tags.map(t => (<span key={t} className={'tag'}>{t}</span>))}
    </div>
}
