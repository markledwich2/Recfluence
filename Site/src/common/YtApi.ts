import { ChannelData } from "./YtModel"
import { getJson, putJson, getJsonl } from "./Utils"
import _ from 'lodash'
import { DbModel } from './DbModel'
import { EsCfg } from './Elastic'
import { uri } from './Uri'

export const apiUrl = process.env.FUNC_URL

// On a build+server, or in prod. the server will breifly show the main page before replacing with the correct rout
// This is because when /video/ is requested, the redirects aren't pointing to /index.html.
// i think I should use a static page 
// https://stackoverflow.com/questions/52051090/gatsbyjs-client-only-paths-goes-to-404-page-when-the-url-is-directly-accessed-in

export const resultsUrl = uri(process.env.RESULTS_HOST)
  .addPath(`${process.env.RESULTS_CONTAINER}${process.env.BRANCH_ENV ? `-${process.env.BRANCH_ENV}` : ''}`, process.env.RESULTS_PATH)

export async function getVideo(cfg: EsCfg, videoId: string): Promise<{ video: EsVideo, channel: ChannelData }> {
  var res = await getJson<EsVideoResponse>(`${apiUrl}/video/${videoId}`)
  return { video: res.video, channel: DbModel.ChannelData(res.channel) }
}

export async function getCaptions(cfg: EsCfg, videoId: string): Promise<EsCaption[]> {
  var res = await getJson<EsCaption[]>(`${apiUrl}/captions/${videoId}`)
  return res
}

export async function saveSearch(search: UserSearch): Promise<void> {
  await putJson(`${apiUrl}/search`, search)
}

export async function getChannels(): Promise<RawChannelData[]> {
  return await getJsonl<RawChannelData>(resultsUrl.addPath('class_channels_raw.jsonl.gz').url)
}

export async function saveReview(review: ChannelReview): Promise<Response> {
  return await putJson(uri(apiUrl).addPath('channel_review').url, review)
}

export async function channelsReviewed(email: string): Promise<ChannelReview[]> {
  return await getJson<ChannelReview[]>(uri(apiUrl).addPath('channels_reviewed').addQuery({ email }).url)
}

export interface ChannelReviewAndData extends ChannelReview, RawChannelData {
  ReviewUpdated: string
}

export interface ChannelReview {
  ChannelId: string
  Email?: string
  LR?: string
  Relevance?: number
  SoftTags: string[]
  Notes?: string
  Updated?: string
  MainChannelId?: string
}

export interface RawChannelData {
  ChannelId: string
  ChannelTitle: string
  Description: string
  LogoUrl: string
  ChannelViews: number
  Keywords: string
  ReviewStatus: string
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
  video: EsVideo
  channel: ChannelData
}

interface EsVideoResponse {
  video: EsVideo
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