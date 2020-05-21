import { ChannelData } from "./YtModel"
import { getJson } from "./Utils"
import _ from 'lodash'
import { DbModel } from './DbModel'
import { EsCfg } from './Elastic'

const apiUrl = process.env.FUNC_URL

export async function getVideo(cfg: EsCfg, videoId: string): Promise<{ video: EsVideo, channel: ChannelData }> {
  var res = await getJson<EsVideoResponse>(`${apiUrl}/${cfg.indexes.video}/${videoId}`)
  return { video: res.video, channel: DbModel.ChannelData(res.channel) }
}

export async function getCaptions(cfg: EsCfg, videoId: string): Promise<EsCaption[]> {
  var res = await getJson<EsCaption[]>(`${apiUrl}/${cfg.indexes.caption}/${videoId}`)
  return res
}

export async function saveSearch(search: UserSearch): Promise<void> {
  var res = await fetch(`${apiUrl}/search`, { method: 'PUT', body: JSON.stringify(search) })
}

export interface UserSearch {
  origin: string
  email: string
  query: string
  ideologies: string[]
  channels: string[]
  updated: Date
}

export interface VideoData {
  video: EsVideo,
  channel: ChannelData
}

interface EsVideoResponse {
  video: EsVideo,
  channel: any
}

interface EsCaptionVideoCommon {
  video_id: string
  channel_id: string
  video_title: string
  channel_title: string
  thumb_high: string
  keywords: string
  upload_date: Date
  updated: string
  pcd_ads: number
  views: number
  ideology: string
  lr: string
  media: string
  country: string
}

export interface EsCaption extends EsCaptionVideoCommon {
  caption_id: string
  video_id: string
  channel_id: string
  caption: string
  offset_seconds: number
  part: CaptionPart
}

export interface EsVideo extends EsCaptionVideoCommon {
  description: string
  likes: number
  dislikes: number
}

export type CaptionPart = 'Caption' | 'Title' | 'Description' | 'Keywords'