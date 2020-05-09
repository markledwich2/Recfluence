import * as React from "react"
import { VideoSearch } from '../components/search/VideoSearch'
import { MainLayout } from '../components/MainLayout'
import { TopSiteBar } from '../components/SiteMenu'
import { esCfgFromEnv } from '../common/Elastic'

const SearchPage = () => (
    <MainLayout>
        <VideoSearch esCfg={esCfgFromEnv()} />
    </MainLayout>
)
export default SearchPage


