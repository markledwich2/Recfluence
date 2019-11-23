import * as d3 from 'd3'
import * as _ from 'lodash'
import { Dim, Col } from './Dim'
import { SelectionState } from './Chart'

export interface Graph<N, L> {
  nodes: N
  links: L
}

export type Dic<T> = _.Dictionary<T>

export interface ChannelData {
  channelId: string
  title: string
  tags: string[]
  subCount: number
  channelVideoViews: number
  thumbnail: string
  lr: string
  publishedFrom: string
  publishedTo: string
  dailyViews: number
  relevantDailyViews: number
  relevance: number
  views: number
  ideology: string
  media: string,
  manoel: string,
  ain: string
}

export interface RecData {
  fromChannelId: string
  channelId: string
  fromVideoViews: number
  relevantImpressions: number
  recommendsViewChannelPercent: number
}

export class YtModel {

  channels: ChannelData[]
  recs: RecData[]
  channelDim: Dim<ChannelData>
  selectionState: SelectionState

  static async dataSet(path: string): Promise<YtModel> {
    const channelsCsvTask = d3.csv(path + 'channel_stats.csv')
    const relationsCsvTask = d3.csv(path + 'channel_recs.csv')
    const channels = (await channelsCsvTask).map((c: any) => {
      return <ChannelData>{
        channelId: c.CHANNEL_ID,
        title: c.CHANNEL_TITLE,
        tags: JSON.parse(c.TAGS),
        subCount: c.SUBS,
        channelVideoViews: +c.VIEWS,
        thumbnail: c.LOGO_URL,
        lr: c.LR,
        publishedFrom: c.FROM_DATE,
        publishedTo: c.TO_DATE,
        dailyViews: +c.DAILY_VIEWS,
        relevantDailyViews: +c.RELEVANT_DAILY_VIEWS,
        views: +c.CHANNEL_VIEWS,
        ideology: c.IDEOLOGY,
        media: c.MEDIA,
        relevance: c.RELEVANCE,
        manoel: c.MANOEL,
        ain: c.AIN
      }
    })

    const channelDic = _(channels).keyBy(c => c.channelId).value()

    let relations = (await relationsCsvTask).map((c: any) => {
      return <RecData>{
        fromChannelId: c.FROM_CHANNEL_ID,
        channelId: c.TO_CHANNEL_ID,
        relevantImpressions: +c.RELEVANT_IMPRESSIONS,
        recommendsViewChannelPercent: +c.REC_VIEW_CHANNEL_PERCENT
      }
    }).filter(r => channelDic[r.fromChannelId] && channelDic[r.channelId])


    return {
      channels: channels, 
      recs: relations,
      channelDim: new Dim(this.channelDimStatic.meta, channels),
      selectionState: { selected: [], parameters: { colorBy: 'ideology' } },
    }
  }

  static channelDimStatic = new Dim<ChannelData>({
    name: 'channel',
    cols: [
      {
        name: 'channelId',
        props: ['lr', 'ideology', 'media', 'manoel', 'ain'],
        valueLabel: 'title'
      },
      {
        name: 'lr',
        label: 'Left/Center/Right',
        values: [
          { value: 'L', label: 'Left', color: '#3887be' },
          { value: 'C', label: 'Center', color: '#50667f' },
          { value: 'R', label: 'Right', color: '#e0393e' },
          { value: '', label: 'Unknown', color: '#555' }
        ]
      },
      {
        name: 'ideology',
        label: 'Idiology - Ledwich &  Zaitsev',
        values: [
          { value: 'Anti-SJW', color: '#50667f' },
          { value: 'Partisan Right', color: '#e0393e' },
          { value: 'Alt-light', color: '#8a8acb' },
          { value: 'Alt-right', color: '#e55e5e' },
          { value: 'MRA', color: '#ed6498' },
          { value: 'Social Justice', color: '#56b881' },
          { value: 'Socialist', color: '#6ec9e0' },
          { value: 'Partisan Left', color: '#3887be' },
          { value: 'Anti-theist', color: '#96cbb3' },
          { value: 'Libertarian', color: '#b7b7b7' },
          { value: 'Religious Conservative', color: '#41afa5' },
          { value: 'Conspiracy', color: '#ffc168' },
          { value: '', label: 'Other', color: '#333' },
        ]
      },
      {
        name: 'media',
        label: 'Media Type',
        values: [
          { value: '', label: 'Other', color: '#333' },
          { value: 'Mainstream Media',  color: '#3887be' },
          { value: 'YouTube',  color: '#e55e5e' },
          { value: 'Missing Link Media',  color: '#41afa5' },
        ]
      },
      {
        name: 'manoel',
        label: 'Radicalization Pathways - Ribeiro et al.',
        values: [
          { value: 'Alt-light', color: '#8a8acb' },
          { value: 'IDW', color: '#50667f' },
          { value: 'Alt-right', color: '#e55e5e' },
          { value: 'Control', color: '#D8A47F' },
          { value: '', label: 'Other', color: '#333' },
        ]
      },
      {
        name: 'ain',
        label: 'Alternative Influence Network',
        values: [
          { value: '', label: 'Other', color: '#333' },
          { value: 'AIN', color: '#41afa5'}
        ]
      }
    ]
  })
}






