import * as d3 from 'd3'
import { DSVParsedArray, color } from 'd3'
import { DataSelections, SelectionType, DataSelection } from './Charts'
import _ from 'lodash'
import { ChannelWords } from '../components/ChannelWords';
import { allResolved } from 'q';
import { TopicsPage } from '../components/TopicsPage';

export interface Graph<N, L> {
  nodes: N
  links: L
}

export interface RelationsData {
  channels: _.Dictionary<ChannelData>
  relations: RelationData[],
  topics: _.Dictionary<Topic>
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
  Topic: ChannelTopic
  Topics: ChannelTopic[]
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

export interface VideoData {
  VideoId:string,
  Title:string,
  Views:number,
  Thumbnai:string,
  Topic:string
}

export interface Word {
  Word: string
  TfIdf: number
  Count: number
}

// export interface TopicData {
//   topics: Topic[]
//   channels: ChannelTopicData[]
//   videos: VideoData[]
// }

export interface Topic {
  label:string
  color:string
  words:Word[]
}

export interface TopicWord {
  word:string
  beta:string
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
  Topic: string,
  Videos: number,
  OnTopic: number
}

export class ChannelTopic {
  topic: Topic
  videos: number
  onTopic: number

  constructor(init?:Partial<ChannelTopic>) {
    Object.assign(this, init);
  }

  color() { return this.topic.color }
  label() { return this.topic.label }
}

export class YtNetworks {
  static ChannelIdPath: string = 'Channels.channelId'


  static createChannelHighlight(channelId: string): DataSelection {
    return { path: this.ChannelIdPath, type: SelectionType.Highlight, values: [channelId] }
  }

  static createChannelFilter(channelId: string): DataSelection {
    return { path: this.ChannelIdPath, type: SelectionType.Filter, values: [channelId] }
  }

  static async loadRelationsData(path: string): Promise<RelationsData> {
    let channelsCsvTask = d3.csv(`${path}VisChannels.csv`)
    let relationsCsvTask = d3.csv(`${path}VisRelations.csv`)
    let channelTopicsCsvTask = d3.csv(`${path}ChannelTopics.csv`)
    let channelWordsTask = d3.csv(`${path}ChannelWords.csv`)

    let channels = _(await channelsCsvTask).map((c:any) => c as ChannelData).keyBy(c => c.ChannelId).value()
    let relations = (await relationsCsvTask).map((c: any) => c as RelationData)
    let allChannelTopics = _(await channelTopicsCsvTask).map((c: any) => c as ChannelTopicData)
    let channelTopics = allChannelTopics.groupBy(t => t.ChannelId).value()
    let channelWords = _(await channelWordsTask).groupBy(c => c.ChannelId).value()


    

    var topicNames = allChannelTopics.map(t => t.Topic).uniq().value()

    var allTopics = _(topicNames.map((t, i) => {
      let c = d3.color(d3.interpolateRainbow(i/(YtNetworks.legendPallet.length-1))).darker(0.2)
      return <Topic>{label:t, color:c.toString()}
    })).keyBy(t => t.label).value()

    _(channels).forEach(c => {
      let cTopics = channelTopics[c.ChannelId]
      let words = channelWords[c.ChannelId]

      let toTopic = (t:ChannelTopicData) => new ChannelTopic({topic:allTopics[t.Topic], videos:t.Videos, onTopic:t.OnTopic})
      if(cTopics) {
        c.Topics = cTopics.map(t => toTopic(t))
        c.Topic = _(c.Topics).orderBy(t => t.onTopic).reverse().first()
      }
       
      if(words)
        c.Words = Array.from( words.map(w => <Word> { Word:w.Word, TfIdf:+w.TfIdf, Count:+w.Count }).values())
    })

    return { channels, relations, topics:allTopics }
  }

  static lrMap = new Map([
    ['L', { color: '#3c4455', text: 'Left' }],
    ['C', { color: '#7b4d5e', text: 'Center/Heterodox' }],
    ['R', { color: '#d43a3d', text: 'Right' }],
    ['', { color: '#666666', text: 'Unknown' }]
  ])

  static legendPallet = ['#f44336', '#E91E63', '#9C27B0', '#673AB7', '#3F51B5', '#2196F3', '#03A9F4',
  '#00BCD4', '#009688', '#4CAF50', '#8BC34A', '#CDDC39', '#FFEB3B',
  '#FFC107', '#FF9800', '#FF5722', '#795548', '#9E9E9E', '#607D8B', '#8D6E63']

  

  static lrColor(key: string) {
    let c = this.lrMap.get(key)
    return c ? c.color : '#666666'
  }

  static lrText(key: string) {
    let c = this.lrMap.get(key)
    return c ? c.text : 'Unknown'
  }
}

interface LrItem {
  color: string
  text: string
}