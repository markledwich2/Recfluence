import { ChannelData } from "./YtModel"
import { getJson, putJson, getJsonl } from "./Utils"
import _, { Dictionary, camelCase } from 'lodash'
import { DbModel } from './DbModel'
import { EsCfg, EsSearchRes, EsDocRes, EsDocsRes } from './Elastic'
import { uri, Uri } from './Uri'
import * as base64 from 'base-64'
import * as camelKeys from 'camelcase-keys'

export const apiUrl = process.env.FUNC_URL

// On a build+server, or in prod. the server will briefly show the main page before replacing with the correct rout
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

export async function channelSearch(cfg: EsCfg, q: string) {
  const sources = await esQuery(cfg, cfg.indexes.channel, q)
  const channels = sources.map(c => camelKeys(c) as BasicChannel)
  return channels
}

export async function getChannel(cfg: EsCfg, channelId: string) {
  const res = await esDoc(cfg, cfg.indexes.channel, channelId)
  return camelKeys(res._source)
}

export async function getChannels(cfg: EsCfg, ids: string[]): Promise<BasicChannel[]> {
  const get = async (index: string, ids: string[]) => {
    const res = await esDocs(cfg, index, ids)
    return res.docs?.filter(d => d.found).map(d => camelKeys(d._source) as BasicChannel) ?? []
  }

  const bChannels = await get(cfg.indexes.channel, ids)
  const tChannels = await get(cfg.indexes.channelTitle, ids.filter(id => !bChannels.find(c => c.channelId == id)))
  return bChannels.concat(tChannels)
}

export const esHeaders = (cfg: EsCfg) => ({
  "Authorization": `Basic ${base64.encode(cfg.creds)}`
})

export async function esDoc(cfg: EsCfg, index: string, id: string) {
  {
    var res = await fetch(
      new Uri(cfg.url).addPath(index, '_doc', id).url, {
      headers: new Headers(esHeaders(cfg))
    })
    var j = await res.json() as EsDocRes<any>
    return j
  }
}

export async function esDocs(cfg: EsCfg, index: string, ids: string[]) {
  {
    var res = await fetch(new Uri(cfg.url).addPath(index, '_mget').url,
      {
        method: 'POST',
        body: JSON.stringify({ ids }),
        headers: new Headers({ ...esHeaders(cfg), 'Content-Type': 'application/json' })
      })
    var j = await res.json() as EsDocsRes<any>
    return j
  }
}

export async function esQuery(cfg: EsCfg, index: string, q: string) {
  {
    let res = await fetch(
      new Uri(cfg.url).addPath(index, '_search').addQuery({ q }).url, {
      headers: new Headers(esHeaders(cfg))
    })
    let j = (await res.json()) as EsSearchRes<any>
    let sources = j.hits.hits.map(h => h._source)
    return sources
  }
}

export async function saveSearch(search: UserSearch): Promise<void> {
  await putJson(`${apiUrl}/search`, search)
}

export async function reviewChannelLists(): Promise<Dictionary<BasicChannel[]>> {
  const list = await getJsonl<BasicChannel & { list: string }>(resultsUrl.addPath('channel_review_lists.jsonl.gz').url, { headers: { cache: "no-store" } })
  return _(list).groupBy(l => l.list).value()
}

export async function saveReview(review: Review): Promise<Response> {
  return await putJson(uri(apiUrl).addPath('channel_review').url, review)
}

export async function channelsReviewed(email: string): Promise<Review[]> {
  return await getJson<Review[]>(uri(apiUrl).addPath('channels_reviewed').addQuery({ email }).url)
}

export function videoThumbHigh(videoId: string) {
  return `https://img.youtube.com/vi/${videoId}/hqdefault.jpg`
}



export interface Review {
  channelId: string
  email?: string
  lr?: string
  relevance?: number
  softTags: string[]
  notes?: string
  updated?: string
  mainChannelId?: string
}

export interface ChannelTitle {
  channelId: string
  channelTitle: string
  description: string
}

export interface ChannelReview {
  review: Review
  channel: BasicChannel
  mainChannel?: BasicChannel
}

export interface BasicChannel extends ChannelTitle {
  logoUrl?: string
  channelViews?: number
  reviewStatus?: string
  reviewsAll?: number
  reviewsAlgo?: number
}

export interface UserSearch {
  origin: string
  email: string
  query: string
  tags: string[]
  channels: string[]
  updated: Date
}

export interface VideoData {
  video: EsVideo
  channel: ChannelData
}

interface EsChannelTitle {
  channel_id: string
  channel_title: string
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
  keywords: string
  upload_date: Date
  updated: string
  views: number
  ideology: string
  lr: string
  country: string
  tags: string[]
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