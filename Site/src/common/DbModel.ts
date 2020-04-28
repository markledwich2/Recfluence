import { ChannelData } from "./YtModel"
import { parseISO } from "date-fns"
import { getJson } from "./Utils"

export class DbModel {
    static ChannelData(dbChannel: any): ChannelData {
        return {
            channelId: dbChannel.CHANNEL_ID,
            title: dbChannel.CHANNEL_TITLE,
            tags: JSON.parse(dbChannel.TAGS),
            subCount: dbChannel.SUBS,
            channelVideoViews: +dbChannel.CHANNEL_VIEWS,
            thumbnail: dbChannel.LOGO_URL,
            lr: dbChannel.LR,
            publishedFrom: parseISO(dbChannel.FROM_DATE),
            publishedTo: parseISO(dbChannel.TO_DATE),
            dailyViews: +dbChannel.VIDEO_VIEWS_DAILY,
            relevantDailyViews: +dbChannel.RELEVANT_VIDEO_VIEWS_DAILY,
            views: +dbChannel.CHANNEL_VIEWS,
            relevantImpressionsDaily: dbChannel.RELEVANT_IMPRESSIONS_DAILY,
            relevantImpressionsDailyIn: dbChannel.RELEVANT_IMPRESSIONS_IN_DAILY,
            ideology: dbChannel.IDEOLOGY,
            media: dbChannel.MEDIA,
            relevance: dbChannel.RELEVANCE,
            manoel: dbChannel.MANOEL,
            ain: dbChannel.AIN,
            lifetimeDailyViews: dbChannel.CHANNEL_LIFETIME_DAILY_VIEWS ?
                +dbChannel.CHANNEL_LIFETIME_DAILY_VIEWS : null
        }
    }
}

export class FuncClient {
    static async getVideo(videoId: string): Promise<VideoData> {
        var res = await getJson<VideoResponse>(`${process.env.FUNC_URL}/video/${videoId}`)
        var data = {
            video: res.video,
            channel: DbModel.ChannelData(res.channel)
        } as VideoData
        return data
    }

    static async getCaptions(videoId: string): Promise<CaptionDb[]> {
        var res = await getJson<CaptionDb[]>(`${process.env.FUNC_URL}/captions/${videoId}`)
        return res
    }
}

export interface VideoDb {
    VIDEO_ID: string
    VIDEO_TITLE: string
    UPLOAD_DATE: string
    VIEWS: number
    LIKES: number
    DISLIKES: number
    PCT_ADS: number
    DESCRIPTION: string
}

export interface CaptionDb {
    CAPTION?: string
    CAPTION_ID: string
    OFFSET_SECONDS: number
}


export interface VideoData {
    video: VideoDb
    channel: ChannelData
}

export interface VideoResponse {
    video: any
    captions: any[]
    channel: any
}
