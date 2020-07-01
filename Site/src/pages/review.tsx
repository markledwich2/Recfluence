import * as React from "react"
import { MainLayout } from '../components/MainLayout'
import { ChannelReviewDiv } from '../components/review/ChannelReview'
import { TopSiteBar } from '../components/SiteMenu'

const ReviewPage = () => (
    <MainLayout>
        <TopSiteBar showLogin />
        <ChannelReviewDiv />
    </MainLayout>
)
export default ReviewPage


