import * as d3 from 'd3'
import * as _ from 'lodash'
import { capitalCase, merge, toRecord, delay } from '../common/Utils'
import { SelectionState } from './Chart'
import { Col, Dim } from './Dim'
import { DbModel } from './DbModel'
import { Uri } from './Uri'

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
  publishedFrom: Date
  publishedTo: Date
  dailyViews: number
  relevantDailyViews: number
  relevance: number
  relevantImpressionsDaily: number
  relevantImpressionsDailyIn: number
  views: number
  ideology: string
  media: string,
  manoel: string,
  ain: string,
  /** this only exists when from the the azure function */
  lifetimeDailyViews?: number
}

export class ChannelEx {
  static impressionAdvantagePercent = (c: ChannelData) => c.relevantImpressionsDailyIn / c.relevantImpressionsDaily - 1
}

export interface RecData {
  fromChannelId?: string
  toChannelId?: string
  fromTitle?: string,
  toTitle?: string,
  fromIdeology?: string
  toIdeology?: string
  fromLr?: string
  toLr?: string
  fromMedia?: string
  toMedia?: string
  fromManoel?: string
  toManoel?: string
  fromAin?: string
  toAin?: string
  relevantImpressionsDaily: number
  recommendsViewChannelPercent?: number
}

export type RecDir = 'from' | 'to'

export class RecEx {
  static recCol = (dir: RecDir, col: keyof ChannelData): keyof RecData =>
    `${dir}${_.upperFirst(col)}` as keyof RecData

  static channelCol = (col: keyof RecData | string): keyof ChannelData => {
    if (col.startsWith('from')) return _.lowerFirst(col.substr(4)) as keyof ChannelData
    if (col.startsWith('to')) return _.lowerFirst(col.substr(2)) as keyof ChannelData
    return null
  }
}

interface VIS_CHANNEL_REC {
  RELEVANT_IMPRESSIONS_DAILY: string,
  PERCENT_OF_CHANNEL_RECS: string,
  FROM_CHANNEL_ID: string,
  TO_CHANNEL_ID: string
}

export class YtModel {
  recs: Dim<RecData>
  recCats: Dim<RecData>
  channels: Dim<ChannelData>
  selectionState: SelectionState

  static version = 'v2.4'

  constructor() {
    this.recs = new Dim<RecData>(YtModel.recDimStatic.meta)
    this.recCats = new Dim<RecData>(YtModel.recCatDimStatic.meta)
    this.channels = new Dim<ChannelData>(YtModel.channelDimStatic.meta)
    this.selectionState = { selected: [], parameters: { record: { colorBy: 'ideology' } } }
  }

  static async dataSet(path: Uri): Promise<YtModel> {
    const channelsTask = d3.csv(path.addPath('vis_channel_stats.csv.gz').url)
    const recTask = d3.csv(path.addPath('vis_channel_recs.csv.gz').url)
    const recCatTask = d3.csv(path.addPath('vis_category_recs.csv.gz').url)
    await delay(1)
    const channels = (await channelsTask).map((c: any) => DbModel.ChannelData(c))
    const channelDic = _(channels).keyBy(c => c.channelId).value()
    await delay(1)

    let recCol = (dir: RecDir, c: keyof ChannelData) => ({ recCol: RecEx.recCol(dir, c), channelCol: c, dir })


    const createRec = (r: VIS_CHANNEL_REC): RecData => {
      const recCols = _(YtModel.categoryCols.concat('channelId', 'title'))
        .flatMap(c => [recCol('from', c), recCol('to', c)]).value()

      let rec = merge<RecData>({
        recommendsViewChannelPercent: +r.PERCENT_OF_CHANNEL_RECS,
        relevantImpressionsDaily: +r.RELEVANT_IMPRESSIONS_DAILY
      },
        toRecord(recCols,
          c => c.recCol,
          c => {
            const channel = channelDic[r[`${c.dir.toUpperCase()}_CHANNEL_ID` as keyof VIS_CHANNEL_REC]]
            return channel ? channel[c.channelCol] : null
          }))
      return rec
    }

    let recs = (await recTask).map((r: any) => r as VIS_CHANNEL_REC).map(r => createRec(r))
    await delay(1)

    let recCats = (await recCatTask).map(r => {
      let recCols = _(YtModel.categoryCols)
        .flatMap(c => [RecEx.recCol('from', c), RecEx.recCol('to', c)]).value()

      let rec = merge<RecData>({
        relevantImpressionsDaily: +r.RELEVANT_IMPRESSIONS_DAILY
      }, toRecord(recCols, c => c, c => r[capitalCase(c)]?.toString()))
      return rec
    })
    await delay(1)

    const m = new YtModel()
    m.channels = new Dim(this.channelDimStatic.meta, channels)
    m.recs = new Dim(this.recDimStatic.meta, recs)
    m.recCats = new Dim(this.recCatDimStatic.meta, recCats)
    return m
  }

  static categoryCols: (keyof ChannelData)[] = ['ideology', 'lr', 'media', 'manoel', 'ain']


  static channelDimStatic = new Dim<ChannelData>({
    name: 'channel',
    cols: [
      {
        name: 'channelId',
        props: ['lr', 'ideology', 'media', 'manoel', 'ain'],
        labelCol: 'title'
      },
      {
        name: 'title',
        values: [
          { value: '', label: 'Non-Political' },
        ]
      },
      {
        name: 'lr',
        label: 'Left/Center/Right',
        values: [
          { value: 'L', label: 'Left', color: '#3887be' },
          { value: 'C', label: 'Center', color: '#8a8acb' },
          { value: 'R', label: 'Right', color: '#e0393e' },
          { value: '', label: 'Unclassified', color: '#555' }
        ]
      },
      {
        name: 'ideology',
        label: 'Ledwich & Zaitsev Group',
        pallet: ['#333'],
        values: [
          { value: 'Anti-SJW', color: '#8a8acb' },
          { value: 'Partisan Right', color: '#e0393e' },
          { value: 'Provocative Anti-SJW', color: '#e55e5e' },
          { value: 'White Identitarian', color: '#c68143' },
          { value: 'MRA', color: '#ed6498' },
          { value: 'Social Justice', color: '#56b881' },
          { value: 'Socialist', color: '#6ec9e0' },
          { value: 'Partisan Left', color: '#3887be' },
          { value: 'Anti-theist', color: '#96cbb3' },
          { value: 'Libertarian', color: '#b7b7b7' },
          { value: 'Religious Conservative', color: '#41afa5' },
          { value: 'Conspiracy', color: '#ffc168' },
          { value: 'Center/Left MSM', color: '#aa557f' },
          { value: 'Unclassified', label: 'No Group' },
          { value: '', label: 'Non-Political' },
        ]
      },
      {
        name: 'media',
        label: 'Media Type',
        values: [
          { value: '', label: 'Other', color: '#333' },
          { value: 'Mainstream Media', color: '#3887be' },
          { value: 'YouTube', label: 'YouTube Creator', color: '#e55e5e' },
          { value: 'Missing Link Media', color: '#41afa5' },
        ]
      },
      {
        name: 'manoel',
        label: 'Radicalization Pathways - Ribeiro et al.',
        values: [
          { value: 'Alt-light', color: '#e55e5e' },
          { value: 'IDW', color: '#8a8acb' },
          { value: 'Alt-right', color: '#c68143' },
          { value: 'Control', color: '#b7b7b7' },
          { value: '', label: 'Unclassified', color: '#333' },
        ]
      },
      {
        name: 'ain',
        label: 'Alternative Influence Network',
        values: [
          { value: '', label: 'Unclassified', color: '#333' },
          { value: 'AIN', color: '#41afa5' }
        ]
      }
    ]
  })

  static tagAlias: Record<string, string> = {
    ManoelAltLite: 'Ribeiro - Alt-light',
    ManoelAltRight: 'Ribeiro - Alt-right',
    ManoelIDW: 'Ribeiro - IDW',
    ManoelControl: 'Ribeiro - Control',
    AntiSJW: 'Anti-SJW',
    SocialJusticeL: 'Social Justice',
    WhiteIdentitarian: 'White Identitarian',
    PartisanLeft: 'Partisan Left',
    PartisanRight: 'Partisan Right',
    AntiTheist: 'Anti-theist',
    ReligiousConservative: 'Religious Conservative',
    MissingLinkMedia: 'Missing Link Media',
    StateFunded: 'State Funded',
    AntiWhiteness: 'Anti-whiteness',
    'Mainstream News': 'MSM'
  }

  private static recCol(dir: RecDir, name: keyof ChannelData) {
    const col = YtModel.channelDimStatic.col(name)
    return merge(col as any as Col<RecData>, {
      name: RecEx.recCol(dir, col.name),
      labelCol: col.labelCol ? RecEx.recCol(dir, col.labelCol) : null,
    })
  }

  static recDimStatic = new Dim<RecData>({
    name: 'Recommendations',
    cols: _(['channelId', 'title'] as (keyof ChannelData)[])
      .concat(YtModel.categoryCols)
      .flatMap(c => ([YtModel.recCol('from', c), YtModel.recCol('to', c)])).value()
  })

  static recCatDimStatic = new Dim<RecData>({
    name: 'Category Recommendations',
    cols: _(YtModel.categoryCols)
      .flatMap(c => ([YtModel.recCol('from', c), YtModel.recCol('to', c)])).value()
  })
}







