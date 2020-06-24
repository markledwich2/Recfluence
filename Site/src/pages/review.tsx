import * as React from "react"
import { MainLayout } from '../components/MainLayout'
import { ChannelReview } from '../components/review/ChannelReview'
import { TopSiteBar } from '../components/SiteMenu'

const ReviewPage = () => (
    <MainLayout>
        <TopSiteBar showLogin />
        <ChannelReview />
    </MainLayout>
)
export default ReviewPage


