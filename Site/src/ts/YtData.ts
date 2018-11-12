import * as d3 from 'd3'
import * as d3sk from 'd3-sankey'
import { DSVParsedArray } from 'd3';
import { DataSelections, SelectionType, DataSelection } from './Charts';

export interface RelationSimLink extends d3.SimulationLinkDatum<ChannelSimNode> {
  source: ChannelSimNode | string | number
  target: ChannelSimNode | string | number
  strength: number
}

export interface ChannelSimNode extends ChannelSkExtra, d3.SimulationNodeDatum {}

export interface ChannelSkExtra {
  shapeId: string
  channelId: string
  title: string
  size: number
  type: string
  lr: string
}

export interface RelationSkExtra {
  id:string
  source: string
  target: string
  value: number
}

export type ChannelSkNode = d3sk.SankeyNode<ChannelSkExtra, RelationSkExtra>
export type RelationSkLink = d3sk.SankeyLink<ChannelSkExtra, RelationSkExtra>

export interface SGraph {
  nodes: ChannelSkNode[]
  links: RelationSkLink[]
}

export interface Graph<N, L> {
  nodes: N
  links: L
}

export type SimGraph = Graph<ChannelSimNode[],RelationSimLink[]>
export type SkGraph = Graph<ChannelSkNode[], RelationSkLink[]>

export interface YtData {
  channels: ChannelData[]
  relations: RelationData[]
}

interface ChannelData {
  Id: string
  Title: string
  Status: string
  SubCount: number
  ChannelVideoViews: number
  Type: string
  LR: string
}

interface RelationData {
  FromChannelTitle: string
  ChannelTitle: string
  FromChannelId: string
  ChannelId: string
  FromVideoViews: number
  RecommendsPerVideo: number
}

export class YtNetworks {
  static ChannelIdPath: string = 'Channels.channelId'

  static async dataSet(path: string): Promise<YtData> {
    let channelsCsvTask = d3.csv(path + 'VisChannels.csv')
    let relationsCsvTask = d3.csv(path + 'VisRelations.csv')
    let channels = (await channelsCsvTask).map((c:any) => c as ChannelData)
    let relations = (await relationsCsvTask).map((c:any) => c as RelationData)

    return {channels,relations}
  }

  static simData(data: YtData):SimGraph {
    let nodes = data.channels
      .map(
        c =>
          <ChannelSimNode>{
            channelId: c.Id,
            title: c.Title,
            size: c.ChannelVideoViews,
            type: c.Type,
            lr: c.LR
          }
      )

    let links = data.relations
      .map(
        l =>
          <RelationSimLink>{
            source: l.FromChannelId,
            target: l.ChannelId,
            strength: l.RecommendsPerVideo
          }
      )
      .filter(l => l.strength > 0.1)

    return { nodes, links }
  }

  static sankeyData(data: YtData):SkGraph {
    let nodes = data.channels
      .map(
        n =>
          <ChannelSkNode>{
            channelId: n.Id,
            title: n.Title,
            size: n.ChannelVideoViews,
            type: n.Type,
            lr: n.LR
          }
      )

    let links = data.relations
      .map(
        n =>
          <RelationSkLink>{
            id: `${n.FromChannelId}.${n.ChannelId}`,
            source: n.FromChannelId,
            target: n.ChannelId,
            value: n.FromVideoViews
          }
      )

    return { nodes, links }
  }

  static lrPallet(): { [lr: string]: string } {
    return { L: '#3D5467', C: '#BFBCCB', PL: '#A26769', R: '#A4243B', '': '#D8973C' }
  }
}
