import { RouteComponentProps } from "@reach/router"
import { parseISO } from "date-fns"
import { compactInteger } from "humanize-plus"
import React, { useEffect, useState } from "react"
import YouTube from 'react-youtube'
import styled from 'styled-components'
import { FuncClient, VideoData, CaptionDb } from "../../common/DbModel"
import '../../types/NodeTypings.d.ts'
import { dateFormat, secondsToHHMMSS } from "../../common/Utils"
import { TextPage } from "../MainLayout"
import { ChannelData } from "../../common/YtModel"
import { useLocation } from '@reach/router'
import queryString from 'query-string'

const VidePageDiv = styled(TextPage)`
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
  const { videoId } = props

  const [video, setVideoData] = useState<VideoData>()
  const [captions, setCaptions] = useState<CaptionDb[]>()
  //const [offset, setOffset] = useState<number>()
  const [player, setPlayer] = useState<YT.Player>(null)

  const v = video?.video
  const location = useLocation()
  const t = +queryString.parse(location.search).t

  useEffect(() => {
    async function renderVideo() {
      const captionTask = FuncClient.getCaptions(videoId)
      const videoTask = await FuncClient.getVideo(videoId)
      setVideoData(await videoTask)
      setCaptions(await captionTask)
    }
    renderVideo()
  }, [])

  useEffect(() => {
    if (!t) return
    const e = document.getElementById(t.toString())
    if (e)
      e.scrollIntoView()
  })

  const onVideoReader = (event: any) => {
    const player: YT.Player = event.target
    setPlayer(player)
    if (t)
      player?.seekTo(t, true)
  }

  const onCaptionClick = (offset: number) => {
    player?.seekTo(offset, true)
  }


  return (
    <div>
      <VidePageDiv>
        <YtContainer><YouTube videoId={videoId} onReady={e => onVideoReader(e)} opts={{ height: "100%", width: "100%" }} /></YtContainer>
        <div>
          <h2>{v?.VIDEO_TITLE}</h2>
        </div>

        {v ? (
          <>
            <VideoStatsDiv>
              <span><b>{compactInteger(v.VIEWS)}</b> views</span>
              <span>{dateFormat(parseISO(v.UPLOAD_DATE))}</span>
              <span style={{ marginLeft: 'auto' }}>
                <span><b>{compactInteger(v.LIKES)}</b> likes</span>
                <span><b>{compactInteger(v.DISLIKES)}</b> dislikes</span>
              </span>
            </VideoStatsDiv>
            <CaptionDiv>
              <VideoChannelTitle Channel={video.channel} />
              <DescriptionDiv>{v.DESCRIPTION}</DescriptionDiv>
              <div>
                {captions?.map(c => {
                  var selected = c.OFFSET_SECONDS == t
                  return (
                    <span key={c.OFFSET_SECONDS} id={c.OFFSET_SECONDS.toString()}>
                      <a onClick={() => onCaptionClick(c.OFFSET_SECONDS)} style={{ fontWeight: selected ? 2 : 1 }} >{secondsToHHMMSS(c.OFFSET_SECONDS)}</a> {c.CAPTION}<br />
                    </span>
                  ) ?? <></>
                })
                }
              </div>
            </CaptionDiv>
          </>
        ) : <></>}

      </VidePageDiv >
    </div>
  )
}

const Card = styled.div`
  margin: 1em;
  display: flex;
`

export interface VideoChannelTitleProps {
  Channel: ChannelData
}

const VideoChannelTitle = (props: VideoChannelTitleProps) => {
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




