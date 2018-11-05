import { string, any } from 'prop-types'
import * as d3 from 'd3'
import * as d3sk from 'd3-sankey'

export interface ChartProps {
  dataPath: string
  width: number
  height: number
}

export interface RecommendedSimDatum extends d3.SimulationLinkDatum<ChannelSimDatum> {
  source: ChannelSimDatum | string | number
  target: ChannelSimDatum | string | number
  strength: number
}

export interface ChannelSimDatum extends ChannelNodeExtra, d3.SimulationNodeDatum {}

export interface ChannelNodeExtra  {
  channelId: string
  title: string
  size: number
  type: string
  lr: string
}

export interface RecommendedLinkExtra {
  source: string
  target: string
  value: number
}

export type SChannelNode = d3sk.SankeyNode<ChannelNodeExtra, RecommendedLinkExtra>;
export type SRecommendedLink = d3sk.SankeyLink<ChannelNodeExtra, RecommendedLinkExtra>;

export interface SGraph {
  nodes: SChannelNode[];
  links: SRecommendedLink[];
}

export interface Graph<N, L> {
  nodes: N[]
  links: L[]
}

export class YtNetworks {
  static async rowData(path: string): Promise<Graph<any, any>> {
    let channelsTask = d3.csv(path + 'Channels.csv')
    let recommendsTask = d3.csv(path + 'ChannelRelations.csv')
    return {
      nodes: await channelsTask,
      links: await recommendsTask
    }
  }

  static async simData(path: string): Promise<Graph<ChannelSimDatum, RecommendedSimDatum>> {
    let data = await this.rowData(path)

    return {
      nodes: data.nodes.map(
        n =>
          <ChannelSimDatum>{
            channelId: n.Id,
            title: n.Title,
            size: n.ChannelVideoViews,
            type: n.Type,
            lr: n.LR
          }
      ),
      links: data.links.map(
        n =>
          <RecommendedSimDatum>{
            source: n.FromChannelId,
            target: n.ChannelId,
            strength: n.RecommendsPerVideo
          }
      )
    }
  }

  static async sankeyData(path: string): Promise<SGraph> {
    let data = await this.rowData(path)

    return {
      nodes: data.nodes.map(
        n =>
          <SChannelNode>{
            channelId: n.Id,
            title: n.Title,
            size: n.ChannelVideoViews,
            type: n.Type,
            lr: n.LR
          }
      ),
      links: data.links.map(
        n =>
          <SRecommendedLink>{
            source: n.FromChannelId,
            target: n.ChannelId,
            value: n.RecommendedViews
          }
      )
    }
  }


  static lrPallet():{ [lr: string]: string } {
    return { L: '#3D5467', C: '#BFBCCB', PL: '#A26769', R: '#A4243B', '': '#D8973C' };
  }
}
