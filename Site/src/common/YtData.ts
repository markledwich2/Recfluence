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
  PublishedTo: string,
  Topic: string
  Words: Word[]
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

export interface Word {
  Word: string
  TfIdf: number
  Count: number
}

export interface ChannelWordsData {
  ChannelId: string
  Title: string
  Word: string
  TfIdf: number
  Count: number
}

export interface ChannelTopicData {
  ChannelId: string
  Topic: string
}

export class YtNetworks {
  static ChannelIdPath: string = 'Channels.channelId'


  static createChannelHighlight(channelId: string): DataSelection {
    return { path: this.ChannelIdPath, type: SelectionType.Highlight, values: [channelId] }
  }

  static createChannelFilter(channelId: string): DataSelection {
    return { path: this.ChannelIdPath, type: SelectionType.Filter, values: [channelId] }
  }

  static async dataSet(path: string): Promise<YtData> {
    let channelsCsvTask = d3.csv(`${path}VisChannels.csv`)
    let relationsCsvTask = d3.csv(`${path}VisRelations.csv`)
    let channelTopicsCsvTask = d3.csv(`${path}ChannelTopics.csv`)
    let channelWordsTask = d3.csv(`${path}ChannelWords.csv`)

    let channels = _(await channelsCsvTask).map((c: any) => c as ChannelData).keyBy(c => c.ChannelId).value()
    let relations = (await relationsCsvTask).map((c: any) => c as RelationData)
    let topics = _(await channelTopicsCsvTask).map((c: any) => c as ChannelTopicData).keyBy(t => t.ChannelId).value()
    let channelWords = _(await channelWordsTask).map((c: any) => c as ChannelWordsData).groupBy(c => c.ChannelId).value()

    _(channels).forEach(c => {
      let topic = topics[c.ChannelId]
      let words = channelWords[c.ChannelId]
      if(topic)
        c.Topic = topic.Topic
      if(words)
        c.Words = words.map(w => <Word> { Word:w.Word, TfIdf:+w.TfIdf, Count:+w.Count })
    })

    return { channels, relations }
  }

  static lrMap = new Map([
    ['L', { color: '#3c4455', text: 'Left' }],
    ['C', { color: '#7b4d5e', text: 'Center/Heterodox' }],
    ['R', { color: '#d43a3d', text: 'Right' }],
    ['', { color: '#666666', text: 'Unknown' }]
  ])

  static topicMap = new Map([
    ['1', { color: '#6469CB', text: '?' }],
    ['2', { color: '#3CA4B3', text: '?' }],
    ['3', { color: '#BF50C5', text: '?' }],
    ['4', { color: '#B93E69', text: '?' }],
    ['5', { color: '#ABCB63', text: '?' }],
    ['6', { color: '#4DC4A2', text: '?' }],
    ['7', { color: '#319453', text: '?' }],
    ['8', { color: '#C69754', text: '?' }]
  ])

  static lrColor(key: string) {
    let c = this.lrMap.get(key)
    return c ? c.color : '#666666'
  }

  static lrText(key: string) {
    let c = this.lrMap.get(key)
    return c ? c.text : 'Unknown'
  }

  static topicColor(key: string) {
    let c = this.topicMap.get(key)
    return c ? c.color : '#666666'
  }
}

interface LrItem {
  color: string
  text: string
}