import { ColEx } from "../common/Dim"
import { ChannelData, YtModel } from "../common/YtModel"
import { color } from "d3"
import React from "react"



export interface ChannelTagData extends ChannelTagCols {
    tags: string[],
}

export interface ChannelTagCols {
    lr: string,
    ideology: string
}


export interface ChannelComponentProps {
    channel: ChannelTagData
}

const dim = new YtModel().channels

export const ChannelTags = (props: ChannelComponentProps) => {
    const c = props.channel
    let tags = c.tags.map(t => YtModel.tagAlias[t] ?? t).filter(t => t != '_')

    return <div>
        <ColTag colName="ideology" channel={c} />
        <ColTag colName="lr" channel={c} />
        {tags.map(t => (<span key={t} className={'tag'}>{t}</span>))}
    </div>
}

interface ColTagProps { colName: keyof ChannelTagCols, channel: ChannelTagData }
const ColTag = (p: ColTagProps) => {
    const c = p.channel
    const col = dim.col(p.colName)

    const labelFunc = ColEx.labelFunc(col)
    const colorFunc = ColEx.colorFunc(col)
    const darkerColor = (v: string) => color(colorFunc(v))?.darker(2).hex()
    const val = c[p.colName]

    return <span key={c.lr} className={'tag'} style={{ backgroundColor: darkerColor(val) }}>{labelFunc(val)}</span>
}