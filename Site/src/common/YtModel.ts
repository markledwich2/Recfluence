import * as d3 from 'd3'
import * as _ from 'lodash'
import { Dim, Col, DimMeta, ColType } from './Dim'
import { SelectionStateHelper, SelectionState } from './Chart'

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
  relevance: number
  views: number
  ideology: string
  media: string
}

export interface RelationData {
  fromChannelId: string
  channelId: string
  fromVideoViews: number
  recommendsViewFlow: number
  recommendsViewChannelPercent: number
}

export class YtModel {

  channels: ChannelData[]
  relations: RelationData[]
  channelDim: Dim<ChannelData>
  selectionState: SelectionState

  static async dataSet(path: string): Promise<YtModel> {
    const channelsCsvTask = d3.csv(path + 'channel_stats.csv')
    const relationsCsvTask = d3.csv(path + 'channel_recs.csv')
    const channels = (await channelsCsvTask).map((c: any) => {
      return <ChannelData>{
        channelId: c.CHANNEL_ID,
        title: c.CHANNEL_TITLE,
        tags: c.TAGS,
        subCount: c.SUBS,
        channelVideoViews: +c.VIEWS,
        thumbnail: c.LOGO_URL,
        lr: c.LR,
        publishedFrom: c.FROM_DATE,
        publishedTo: c.TO_DATE,
        dailyViews: +c.DAILY_VIEWS,
        views: +c.CHANNEL_VIEWS,
        ideology: c.IDEOLOGY,
        media: c.MEDIA,
        relevance: c.RELEVANCE
      }
    })

    const channelDic = _(channels).keyBy(c => c.channelId).value()

    let relations = (await relationsCsvTask).map((c: any) => {
      return <RelationData>{
        fromChannelId: c.FROM_CHANNEL_ID,
        channelId: c.TO_CHANNEL_ID,
        recommendsViewFlow: +c.REC_VIEW_PORTION,
        recommendsViewChannelPercent: +c.REC_VIEW_CHANNEL_PERCENT
      }
    }).filter(r => channelDic[r.fromChannelId] && channelDic[r.channelId])


    return {
      channels: channels, 
      relations,
      channelDim: new Dim(this.channelDimStatic.meta, channels),
      selectionState: { selected: [] }
    }
  }

  static channelDimStatic = new Dim<ChannelData>({
    name: 'channel',
    cols: [
      {
        name: 'channelId',
        props: ['lr', 'ideology', 'media']
      },
      {
        name: 'lr',
        values: [
          { value: 'L', label: 'Left', color: '#3c4455' },
          { value: 'C', label: 'Center', color: '#7b4d5e' },
          { value: 'R', label: 'Right', color: '#d43a3d' },
          { value: '', label: 'Unknown', color: '#666666' }
        ]
      },
      {
        name: 'ideology',
        values: [
          { value: 'IDW', color: '#E42C6A' },
          { value: 'Partisan Right', color: '#E40C2B' },
          { value: 'Alt-light', color: '#A03137' },
          { value: 'White Identitarian', color: '#A51C30' },
          { value: 'MRA', color: '#EF8354' },

          { value: 'Social Justice', color: '#265D6D' },
          { value: 'Socialist', color: '#2A598C' },
          { value: 'Revolutionary Socialist', color: '#5DB7DE' },
          { value: 'Partisan Left', color: '#468BA8' },

          { value: 'Anti-White', color: '#7A5C61' },

          { value: 'Anti-theist', color: '#EDB183' },
          { value: 'Libertarian', color: '#D8A47F' },
          { value: 'Religious Conservative', color: '#6B4E71' },
          { value: 'Conspiracy', color: '#F3A712' },
          { value: '', label: 'Other', color: '#8E8384' },
        ]
      },
      {
        name: 'dailyViews'
      }
    ]
  })
}

  //   import * as rp from 'request-promise'
  // import { Gunzip  } from 'zlibt2'
  // import * as Papa from 'papaparse'
  // static async LoadCsvGz(path: string) : Promise<any[]> {
  //   let buffer = await d3.buffer(path)


  //   //let response = await rp(path)
  //   //let rawZip = new Uint8Array(response)
  //   let rawUnzip = new Gunzip(buffer.).decompress();
  //   let text = String.fromCharCode.apply(null, new Uint16Array(rawUnzip))
  //   let data = Papa.parse(text).data
  //   return data
  // }
  //  let channelsData = await this.LoadCsvGz(path + 'channel_recs.csv.gz')





