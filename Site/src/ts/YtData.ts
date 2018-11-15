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
  ChannelVideoViews: number
  Thumbnail: string
  LR: string
}

export interface RelationData {
  FromChannelTitle: string
  ChannelTitle: string
  FromChannelId: string
  ChannelId: string
  FromVideoViews: number
  RecommendsViewFlow: number
  RecommendsPercent: number
}

export class YtNetworks {
  static ChannelIdPath: string = 'Channels.channelId'

  static async dataSet(path: string): Promise<YtData> {
    let channelsCsvTask = d3.csv(path + 'VisChannels.csv')
    let relationsCsvTask = d3.csv(path + 'VisRelations.csv')
    let channels = _(await channelsCsvTask).map((c: any) => c as ChannelData).keyBy(c => c.ChannelId).value()
    let relations = (await relationsCsvTask).map((c: any) => c as RelationData)

    return { channels, relations }
  }

  static lrItems = new Map([
      ['L', {color:'#3D5467', text:'Left'}],
      ['C', {color:'#BFBCCB', text:'Center/Heterodox'}],
      ['PL', {color:'#A26769', text:'Allways Punching Left'}],
      ['R', {color:'#A4243B', text:'Right'}],
      ['', {color:'#D8973C', text:'Unknown'}]
    ])

  static lrColor(key:string) {
    return this.lrItems.get(key).color
  }
}

interface LrItem {
  color:string
  text:string
}