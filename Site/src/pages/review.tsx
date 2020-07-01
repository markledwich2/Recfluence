import * as React from "react"
import { MainLayout } from '../components/MainLayout'
import { ReviewControl } from '../components/review/Review'
import { TopSiteBar } from '../components/SiteMenu'

const ReviewPage = () => (
    <MainLayout>
        <TopSiteBar showLogin />
        <ReviewControl />
    </MainLayout>
)
export default ReviewPage


