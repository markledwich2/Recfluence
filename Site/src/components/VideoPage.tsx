import { RouteComponentProps, Router } from "@reach/router"
import { parseISO } from "date-fns"
import { compactInteger } from "humanize-plus"
import React, { useEffect, useState } from "react"
import YouTube from 'react-youtube'
import styled from 'styled-components'
import { FuncClient, VideoData } from "../common/DbModel"
import '../common/NodeTypings.d.ts'
import { dateFormat, secondsToHHMMSS } from "../common/Utils"
import { MainLayout } from "../components/MainLayout"
import { ChannelComponentProps } from "./ChannelTags"


const PageDiv = styled.div`
  display:flex;
  flex-direction:column;
  max-width:1024px;
  margin: 0 auto; 
  justify-content: stretch;
  flex-basis:15%;
  height:100vh;
  & > * {
    margin: 0.2em;
  }
`

const CaptionDiv = styled.div`
  overflow-y: scroll;
  height:100%;
  font-size: 1.2em;
  &::-webkit-scrollbar {
    width: 0.5em;
  }
  &::-webkit-scrollbar-thumb {
    background-color: #333;
  }
  > div {
    margin-bottom: 0.5em;
  }
`

const DescriptionDiv = styled.div`
  color: #BBB;
`

const YtContainer = styled.div`
    position:relative;
    padding-top:56.25%;
    > div {
      position: absolute;
      top: 0;
      left: 0;
      width:100%;
      height:100%;
    }
`

const VideoStatsDiv = styled.div`
  display: flex;
  justify-content: stretch;
  span {
    padding-right: 1em;
  }
`



interface VideoProps extends RouteComponentProps {
  videoId?: string
}

export const Video: React.FC<VideoProps> = (props) => {
  const [data, setData] = useState<VideoData>()
  //const [offset, setOffset] = useState<number>()
  const [player, setPlayer] = useState<YT.Player>(null)


  useEffect(() => {
    async function renderVideo() {
      const data = await FuncClient.getVideo(props.videoId)
      setData(data)
    }
    renderVideo()
  }, [])


  const v = data?.video

  const onReady = (event: any) => {
    setPlayer(event.target)
  }

  const onCaptionClick = (offset: number) => {
    player?.seekTo(offset, true)
  }

  if (!data) return <></>

  return (<PageDiv>
    <YtContainer><YouTube videoId={v.VIDEO_ID} onReady={e => onReady(e)} opts={{ height: "100%", width: "100%" }} /></YtContainer>
    <div>
      <h2>{v.VIDEO_TITLE}</h2>
    </div>
    <VideoStatsDiv>
      <span><b>{compactInteger(v.VIEWS)}</b> views</span>
      <span>{dateFormat(parseISO(v.UPLOAD_DATE))}</span>
      <span style={{ marginLeft: 'auto' }}>
        <span><b>{compactInteger(v.LIKES)}</b> likes</span>
        <span><b>{compactInteger(v.DISLIKES)}</b> dislikes</span>
      </span>
    </VideoStatsDiv>
    <CaptionDiv>
      <VideoChannelTitle Channel={data.channel} />
      <DescriptionDiv>{v.DESCRIPTION}</DescriptionDiv>
      <div>
        {data.captions.map(c => <span key={c.CAPTION_ID}>
          <a onClick={() => onCaptionClick(c.OFFSET_SECONDS)}>{secondsToHHMMSS(c.OFFSET_SECONDS)}</a> {c.CAPTION}<br /></span>)}
      </div>
    </CaptionDiv>
  </PageDiv >)
}

const Card = styled.div`
  margin: 1em;
  display: flex;
`

const VideoChannelTitle = (props: ChannelComponentProps) => {
  const c = props.Channel
  return (<Card>
    <a href={`{https://www.youtube.com}/channel/${c.channelId}`} target="blank">
      <img src={c.thumbnail} style={{ height: '7em', marginRight: '1em', clipPath: 'circle()' }} />
    </a>
    <div>
      <div><b>{c.title}</b></div>
      <div>
        <b>{compactInteger(c.subCount)}</b> subscribers
      </div>
      <div>
        <b>{compactInteger(c.lifetimeDailyViews)}</b> average daily views since created
      </div>

    </div>
  </Card>)
}




