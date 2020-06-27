import { ColEx, Col } from "../../common/Dim"
import { ChannelData, YtModel } from "../../common/YtModel"
import { color } from "d3"
import React from "react"
import { Tag } from '../Tag'

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

    return <>
        <ColTag colName="ideology" channel={c} />{' '}<ColTag colName="lr" channel={c} />
    </>
}

interface ColTagProps { colName: keyof ChannelTagCols, channel: ChannelTagData }
const ColTag = (p: ColTagProps) => {
    const c = p.channel
    const col = dim.col(p.colName)
    const labelFunc = ColEx.labelFunc(col)
    const colorFunc = ColEx.colorFunc(col)
    const val = c[p.colName]
    return <Tag key={c.lr} label={labelFunc(val)} color={tagColor(colorFunc(val))} />
}

export const tagColor = (chartColor: string) => color(chartColor)?.darker(2).hex()