import { ChannelData } from "./YtModel"
import { parseISO } from "date-fns"
import { getJson } from "./Utils"
import _ from 'lodash'

export class DbModel {
    static ChannelData(dbChannel: any): ChannelData {
        const c = _.mapKeys(dbChannel, (_, k) => k.toLowerCase())

        return {
            channelId: c.channel_id,
            title: c.channel_title,
            tags: JSON.parse(c.tags),
            subCount: c.subs,
            channelVideoViews: +c.channel_views,
            thumbnail: c.logo_url,
            lr: c.lr,
            publishedFrom: parseISO(c.from_date),
            publishedTo: parseISO(c.to_date),
            dailyViews: +c.video_views_daily,
            relevantDailyViews: +c.relevant_video_views_daily,
            views: +c.channel_views,
            relevantImpressionsDaily: c.relevant_impressions_daily,
            relevantImpressionsDailyIn: c.relevant_impressions_in_daily,
            ideology: c.ideology,
            media: c.media,
            relevance: c.relevance,
            manoel: c.manoel,
            ain: c.ain,
            lifetimeDailyViews: c.channel_lifetime_daily_views ?
                +c.channel_lifetime_daily_views : null
        }
    }
}

export class FuncClient {
    static async getVideo(videoId: string): Promise<{ video: EsVideo, channel: ChannelData }> {
        var res = await getJson<EsVideoResponse>(`${process.env.FUNC_URL}/video/${videoId}`)
        return { video: res.video, channel: DbModel.ChannelData(res.channel) }
    }

    static async getCaptions(videoId: string): Promise<EsCaption[]> {
        var res = await getJson<EsCaption[]>(`${process.env.FUNC_URL}/captions/${videoId}`)
        return res
    }
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
