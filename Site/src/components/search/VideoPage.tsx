import { RouteComponentProps } from '@reach/router'
import { parseISO } from 'date-fns'
import { compactInteger } from 'humanize-plus'
import React, { useEffect, useState } from 'react'
import YouTube from 'react-youtube'
import styled from 'styled-components'
import '../../types/NodeTypings.d.ts'
import { dateFormat, secondsToHHMMSS, delay } from '../../common/Utils'
import { theme } from '../MainLayout'
import { ChannelData } from '../../common/YtModel'
import { useLocation } from '@reach/router'
import queryString from 'query-string'
import scrollIntoView from 'scroll-into-view-if-needed'
import { TopSiteBar } from '../SiteMenu'
import { EsCfg } from '../../common/Elastic'
import { getCaptions, getVideo, VideoData, EsCaption } from '../../common/YtApi'

const MainPageDiv = styled.div`
  height:100vh;
  display:flex;
  flex-direction:column;
  justify-content: stretch;
`

const VidePageDiv = styled.div`
  width:100vw;
  max-width:1024px;
  margin:0 auto;
  position:relative;
  display:flex;
  flex-direction:column;
  min-height:0px; /* to get inner overflow-y:auto to work: intermediate divs must all be display:flex and min-hight:0 */
`

const ContentDiv = styled.div`
  margin:1em;
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

const CaptionDiv = styled(ContentDiv)`
  overflow-y: scroll;
  font-size: 1.1em;
  margin: 0.5em;
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

const VideoTitle = styled.h2`
  margin: 0.5em 0;
`

const VideoStatsDiv = styled.div`
  display: flex;
  justify-content: stretch;
  span {
    padding-right: 1em;
  }
`

interface VideoProps extends RouteComponentProps { videoId?: string, esCfg: EsCfg }

export const Video: React.FC<VideoProps> = ({ videoId, esCfg }) => {
  const [video, setVideoData] = useState<VideoData>()
  const [captions, setCaptions] = useState<EsCaption[]>()
  const [player, setPlayer] = useState<YT.Player>(null)

  const location = useLocation()
  const urlTime = +queryString.parse(location.search).t ?? 0
  const [time, setTime] = useState<number>(urlTime)

  // get data for video from function
  useEffect(() => {
    async function renderVideo() {
      getCaptions(esCfg, videoId)
        .then(c => setCaptions(c))
        .then(() => delay(200))
        .then(() => scrollToCurrentTime())
      getVideo(esCfg, videoId).then(v => setVideoData(v))
    }
    renderVideo()
  }, [])

  const checkVideoTime = (player: YT.Player) => {
    if (player.getPlayerState() == YT.PlayerState.PLAYING && player.getCurrentTime() > 0) {
      setTime(player.getCurrentTime()) // if it is playing, update the state of the control to match
      scrollToCurrentTime()
    }
  }

  const scrollToCurrentTime = () => {
    const captions = document.querySelector('#captions')
    const lastScroll = captions.getAttribute('data-last-scroll')
    const e = document.querySelector('#captions .current.caption')
    if (e && lastScroll != e.id) {
      scrollIntoView(e, { behavior: 'smooth', scrollMode: 'if-needed' })
      captions.setAttribute('data-last-scroll', e.id)
    }
  }

  const onVideoRender = (event: any) => {
    const player: YT.Player = event.target
    setPlayer(player)
    if (urlTime)
      player?.seekTo(urlTime, true)
    window.setInterval(() => checkVideoTime(player), 1000) // update component state with current periodically
  }

  const onCaptionClick = (offset: number) => {
    player?.seekTo(offset, true)
  }

  const fInt = (n?: number) => n ? compactInteger(n) : ''
  const fDate = (d?: string) => d ? dateFormat(parseISO(d)) : ''
  const v = video?.video
  const c = video?.channel

  return (
    <MainPageDiv>
      <TopSiteBar showLogin />
      <VidePageDiv>
        <VideoContainer><YouTube videoId={videoId} onReady={e => onVideoRender(e)} opts={{ height: '100%', width: '100%' }} /></VideoContainer>
        <ContentDiv>
          <VideoTitle>{v?.video_title}</VideoTitle>
          <VideoStatsDiv>
            {video && <>
              <span><b>{fInt(v?.views)}</b> views</span>
              <span>{dateFormat(v?.upload_date)}</span>
              <span style={{ marginLeft: 'auto' }}>
                <span><b>{fInt(v?.likes)}</b> likes</span>
                <span><b>{fInt(v?.dislikes)}</b> dislikes</span>
              </span>
            </>}
          </VideoStatsDiv>
        </ContentDiv>
        <CaptionDiv id="captions">
          <ChannelTitle channel={video?.channel} />
          <DescriptionDiv>{v?.description}</DescriptionDiv>
          <div>
            {captions?.map((c, i) => {
              var cNext = captions[i + 1]
              var currentCaption = c.offset_seconds <= time && (cNext === undefined || cNext?.offset_seconds > time)
              return (
                <div key={c.offset_seconds} id={c.offset_seconds.toString()} className={'caption' + (currentCaption ? ' current' : '')}>
                  <a onClick={() => onCaptionClick(c.offset_seconds)}>
                    {secondsToHHMMSS(c.offset_seconds)}
                  </a>
                  <i> </i>{c.caption}<br />
                </div>
              ) ?? <></>
            })}
          </div>
        </CaptionDiv>
      </VidePageDiv >
    </MainPageDiv>
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




