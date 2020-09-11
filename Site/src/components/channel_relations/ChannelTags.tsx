import { ColEx, Col } from "../../common/Dim"
import { ChannelData, YtModel } from "../../common/YtModel"
import { color } from "d3"
import React, { FunctionComponent, CSSProperties } from "react"
import { Tag } from '../Tag'
import styled from 'styled-components'

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

const TagDiv = styled.div`
    color: #fff;
    > * {
        margin-right:0.3em;
        margin-bottom:0.2em;
    }
`

export const ChannelTags = (props: ChannelComponentProps) => {
    const c = props.channel
    const tagCol = YtModel.channelDimStatic.col('tags')
    const tagLabel = ColEx.labelFunc(tagCol)
    const colorFunc = ColEx.colorFunc(tagCol)
    return <TagDiv>
        {/* <ColTag colName="lr" channel={c} style={{ marginRight: '0.8em' }} /> */}
        {c.tags.map(t => <Tag key={t} label={tagLabel(t)} color={tagColor(colorFunc(t))} />)}
    </TagDiv>
}

interface ColTagProps { colName: keyof ChannelTagCols, channel: ChannelTagData, style: CSSProperties }
const ColTag: FunctionComponent<ColTagProps> = p => {
    const c = p.channel
    const col = dim.col(p.colName)
    const labelFunc = ColEx.labelFunc(col)
    const colorFunc = ColEx.colorFunc(col)
    const val = c[p.colName]
    return <Tag key={val} label={labelFunc(val)} color={tagColor(colorFunc(val))} style={p.style} />
}

export const tagColor = (chartColor: string) => color(chartColor)?.darker(1).hex()