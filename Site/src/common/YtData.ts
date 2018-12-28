import * as d3 from 'd3'
import { DSVParsedArray } from 'd3'
import { DataSelections, SelectionType, DataSelection } from './Charts'
import * as _ from 'lodash'

export interface Graph<N, L> {
  nodes: N
  links: L
}

export interface YtData {
  channels: _.Dictionary<ChannelData>
  relations: RelationData[]
}

export interface ChannelData {
  ChannelId: string
  Title: string
  Type: string
  SubCount: number
  ViewCount: number
  ChannelVideoViews: number
  Thumbnail: string
  LR: string
  PublishedFrom: string
  PublishedTo: string
}

export interface RelationData {
  FromChannelTitle: string
  ChannelTitle: string
  FromChannelId: string
  ChannelId: string
  FromVideoViews: number
  RecommendsViewFlow: number
  RecommendsPercent: number
  RecommendsFlowPercent: number
  UpdatedAt: string
}

export class YtNetworks {
  static ChannelIdPath: string = 'Channels.channelId'


  static createChannelHighlight(channelId:string):DataSelection {
    return { path:this.ChannelIdPath, type:SelectionType.Highlight, values:[channelId]}
  }

  static createChannelFilter(channelId:string):DataSelection {
    return { path:this.ChannelIdPath, type:SelectionType.Filter, values:[channelId]}
  }

  static async dataSet(path: string): Promise<YtData> {
    let channelsCsvTask = d3.csv(path + 'VisChannels.csv')
    let relationsCsvTask = d3.csv(path + 'VisRelations.csv')
    let channels = _(await channelsCsvTask).map((c: any) => c as ChannelData).keyBy(c => c.ChannelId).value()
    let relations = (await relationsCsvTask).map((c: any) => c as RelationData)

    return { channels, relations }
  }

  static lrMap = new Map([
      ['L', {color:'#3c4455', text:'Left'}],
      ['C', {color:'#7b4d5e', text:'Center/Heterodox'}],
      ['R', {color:'#d43a3d', text:'Right'}],
      ['', {color:'#666666', text:'Unknown'}]
    ])

  static lrColor(key:string) {
    let c = this.lrMap.get(key)
    return c ? c.color : '#666666'
  }

  static lrText(key:string) {
    let c = this.lrMap.get(key)
    return c ? c.text : 'Unknown'
  }
}

interface LrItem {
  color:string
  text:string
}