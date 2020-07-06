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
            lifetimeDailyViews: c.channel_lifetime_daily_views ?
                +c.channel_lifetime_daily_views : null
        }
    }
}

