import { RouteComponentProps, Router } from "@reach/router"
import * as React from "react"
import { useEffect, useState } from "react"
import { DbModel } from "../common/DbModel"
import { getJson, secondsToHHMMSS } from "../common/Utils"
import { ChannelData } from "../common/YtModel"
import { MainLayout } from "../components/MainLayout"
import { VideoChannelTitle } from "../components/Video"
import "../styles/Main.css"
import styled from 'styled-components'
import { YouTube } from "../components/YouTube"

const VideosPage = () => {
  return (
    <MainLayout>
      <Router>
        <Video path="video/:videoId"></Video>
      </Router>
    </MainLayout>
  )
}

interface VideoProps extends RouteComponentProps {
  videoId?: string
}

const Video: React.FC<VideoProps> = (props) => {
  const [data, setData] = useState<VideoData>()

  useEffect(() => {
    async function renderVideo() {
      var res = await getJson<VideoResponse>(`http://localhost:7071/Video/${props.videoId}`)
      var data = {
        video: res.video,
        captions: res.captions,
        channel: DbModel.ChannelData(res.channel)
      } as VideoData
      setData(data)
    }
    renderVideo()
  }, [])

  const PageDiv = styled.div`
    display:flex;
    flex-direction:column;
    max-width:1024px;
    margin: 0 auto; 
    justify-content: stretch;
    flex-basis:20%;
    height:100vh;
    > * {
      margin: 10px;
    }
  `

  const CaptionDiv = styled.div`
    overflow-y: scroll;
    height:100%;
    ::-webkit-scrollbar {
      width: 0.5em;
    }
    ::-webkit-scrollbar-thumb {
      background-color: #333;
    }
  `

  const v = data?.video

  return !data ? <></> :
    <PageDiv>
      <YouTube id={v.VIDEO_ID} />
      <h2>{v.VIDEO_TITLE}</h2>
      <VideoChannelTitle Channel={data.channel} />
      <CaptionDiv>
        {data.captions.map(c => <span key={c.CAPTION_ID}>
          <a href={`https://youtu.be/${v.VIDEO_ID}?t=${c.OFFSET_SECONDS}`}>{secondsToHHMMSS(c.OFFSET_SECONDS)}</a> {c.CAPTION}<br /></span>)}
      </CaptionDiv>
    </PageDiv>
}

interface VideoData {
  video: VideoDb
  channel: ChannelData
  captions: CaptionDb[]
}

interface VideoResponse {
  video: any
  captions: any[]
  channel: any
}

interface VideoDb {
  VIDEO_ID: string
  VIDEO_TITLE: string
}

interface CaptionDb {
  CAPTION?: string
  CAPTION_ID: string
  OFFSET_SECONDS: number
}

export default VideosPage

