import * as React from "react"
import { VideoSearch } from '../components/search/VideoSearch'
import { MainLayout } from '../components/MainLayout'
import { TopSiteBar } from '../components/SiteMenu'

const SearchPage = () => (
    <MainLayout>
        <VideoSearch />
    </MainLayout>
)
export default SearchPage


