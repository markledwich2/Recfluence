import { RouteComponentProps } from '@reach/router'
import { parseISO } from 'date-fns'
import { compactInteger } from 'humanize-plus'
import React, { useEffect, useState } from 'react'
import YouTube from 'react-youtube'
import styled from 'styled-components'
import { FuncClient, VideoData, CaptionDb } from '../../common/DbModel'
import '../../types/NodeTypings.d.ts'
import { dateFormat, secondsToHHMMSS } from '../../common/Utils'
import { TextPage, theme } from '../MainLayout'
import { ChannelData } from '../../common/YtModel'
import { useLocation } from '@reach/router'
import queryString from 'query-string'
import scrollIntoView from 'scroll-into-view-if-needed'

const VidePageDiv = styled(TextPage)`
  display:flex;
  flex-direction:column;
  max-width:1024px;
  margin: auto; 
  justify-content: stretch;
  height:98vh;
  & > * {
    margin: 0.5em;
  }
`

const CaptionDiv = styled.div`
  overflow-y: scroll;
  font-size: 1.1em;
  height:100%;
  > div {
    margin-bottom: 0.5em;
  }

  color: ${theme.fontColor};

  .caption {
    padding-left: 10px;
  }
  .current.caption {
    color: ${theme.fontColorBolder};
    padding-left: 5px;
    border-left: 5px solid ${theme.backColorBolder2};
  }
`

const DescriptionDiv = styled.div`
  color: #BBB;
`

const VideoContainer = styled.div`
  min-height:30vh;
  position:relative;
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
  const urlTime = +queryString.parse(location.search).t ?? 0
  const [time, setTime] = useState<number>(urlTime)

  // get data for video from function
  useEffect(() => {
    async function renderVideo() {
      const captionTask = FuncClient.getCaptions(videoId)
      const videoTask = await FuncClient.getVideo(videoId)
      setVideoData(await videoTask)
      setCaptions(await captionTask)
    }
    renderVideo()
  }, [])

  // scroll to time if there is on in the url
  // useEffect(() => {
  //   if (!urlTime) return
  //   const e = document.getElementById(urlTime.toString())
  //   if (e)
  //     e.scrollIntoView()
  // })

  const onCaptionChange = (player: YT.Player) => {
    if (player.getPlayerState() == YT.PlayerState.PLAYING) {
      setTime(player.getCurrentTime()) // if it is playing, update the state of the control to match
      const captions = document.querySelector('#captions')
      const lastScroll = captions.getAttribute('data-last-scroll')
      const e = document.querySelector('#captions .current.caption')
      if (e && lastScroll != e.id) {
        scrollIntoView(e, { behavior: 'smooth', scrollMode: 'if-needed' })
        captions.setAttribute('data-last-scroll', e.id)
      }
    }
  }

  const onVideoRender = (event: any) => {
    const player: YT.Player = event.target
    setPlayer(player)
    if (urlTime)
      player?.seekTo(urlTime, true)
    window.setInterval(() => onCaptionChange(player), 1000) // update component state with current periodically
  }

  const onCaptionClick = (offset: number) => {
    player?.seekTo(offset, true)
  }

  const fInt = (n?: number) => n ? compactInteger(n) : ''
  const fDate = (d?: string) => d ? dateFormat(parseISO(d)) : ''

  return (
    <div>
      <VidePageDiv>
        <VideoContainer><YouTube videoId={videoId} onReady={e => onVideoRender(e)} opts={{ height: '100%', width: '100%' }} /></VideoContainer>
        <h2>{v?.VIDEO_TITLE}</h2>
        <VideoStatsDiv>
          {v && <>
            <span><b>{fInt(v?.VIEWS)}</b> views</span>
            <span>{fDate(v?.UPLOAD_DATE)}</span>
            <span style={{ marginLeft: 'auto' }}>
              <span><b>{fInt(v?.LIKES)}</b> likes</span>
              <span><b>{fInt(v?.DISLIKES)}</b> dislikes</span>
            </span>
          </>}
        </VideoStatsDiv>
        <CaptionDiv id="captions">
          <ChannelTitle channel={video?.channel} />
          <DescriptionDiv>{v?.DESCRIPTION}</DescriptionDiv>
          <div>
            {captions?.map((c, i) => {
              var cNext = captions[i + 1]
              var playerTime = player?.getCurrentTime() ?? time
              var currentCaption = c.OFFSET_SECONDS <= playerTime && cNext?.OFFSET_SECONDS > playerTime //TODO: look at current time in video
              return (
                <div key={c.OFFSET_SECONDS} id={c.OFFSET_SECONDS.toString()} className={'caption' + (currentCaption ? ' current' : '')}>
                  <a onClick={() => onCaptionClick(c.OFFSET_SECONDS)}>
                    {secondsToHHMMSS(c.OFFSET_SECONDS)}
                  </a>
                  <i> </i>{c.CAPTION}<br />
                </div>
              ) ?? <></>
            })}
          </div>
        </CaptionDiv>
      </VidePageDiv >
    </div>
  )
}

const ChannelTitleStyle = styled.div`
  margin: 1em;
  display: flex;
`

const ChannelTitle = (p: { channel: ChannelData }) => {
  const c = p.channel
  if (c == null) return <ChannelTitleStyle />
  return <ChannelTitleStyle>
    <a href={`{https://www.youtube.com}/channel/${c.channelId}`} target='blank'>
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
  </ChannelTitleStyle>
}




