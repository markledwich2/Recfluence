import { ChannelData } from "./YtModel"
import { parseISO } from "date-fns"

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
            ain: dbChannel.AIN
        }
    }
}