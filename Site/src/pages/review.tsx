import * as React from "react"
import { MainLayout } from '../components/MainLayout'
import { ChannelReviewForm } from '../components/review/ChannelReview'
import { TopSiteBar } from '../components/SiteMenu'

const ReviewPage = () => (
    <MainLayout>
        <TopSiteBar showLogin />
        <ChannelReviewForm />
    </MainLayout>
)
export default ReviewPage


