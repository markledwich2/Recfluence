import * as React from 'react'
import { InteractiveDataProps, InteractiveDataState } from '../common/Chart'
import { YtInteractiveChartHelper } from "../common/YtInteractiveChartHelper"
import { YtModel, ChannelData, ChannelEx } from '../common/YtModel'
import { compactInteger, formatNumber } from 'humanize-plus'
import * as _ from 'lodash'
import { SearchChannels } from '../components/SearchChannels'
import * as dateformat from 'dateformat'
import { Dim, ColEx } from '../common/Dim'
import * as d3 from 'd3'
import { color } from 'd3'

interface State extends InteractiveDataState { }
interface Props extends InteractiveDataProps<YtModel> { }

export class ChannelTitle extends React.Component<Props, State> {
  chart: YtInteractiveChartHelper = new YtInteractiveChartHelper(this, 'search')
  state: Readonly<State> = {
    selections: this.props.model.selectionState
  }

  get dim(): Dim<ChannelData> {
    return this.props.model.channels
  }

  channel() {
    const channelId = this.chart.selections
      .highlightedOrSelectedValue(this.dim.col("channelId"))
    return channelId ? this.props.model.channels.rows.find(c => c.channelId == channelId) : null
  }

  tagAlias: Record<string, string> = {
    ManoelAltLite: 'Ribeiro - Alt-light',
    ManoelAltRight: 'Ribeiro - Alt-right',
    ManoelIDW: 'Ribeiro - IDW',
    ManoelControl: 'Ribeiro - Control',
    AntiSJW: 'Anti-SJW',
    SocialJusticeL: 'Social Justice',
    WhiteIdentitarian: 'White Identitarian',
    PartisanLeft: 'Partisan Left',
    PartisanRight: 'Partisan Right',
    AntiTheist: 'Anti-theist',
    ReligiousConservative: 'Religious Conservative',
    MissingLinkMedia: 'Missing Link Media',
    StateFunded: 'State Funded',
    AntiWhiteness: 'Anti-whiteness',
    LateNightTalkShow: '_',
    Revolutionary: '_'
  }

  render() {
    let channel = this.channel()
    let dateFormat = (d: Date) => d ? dateformat(d, 'd mmm yyyy') : d

    const renderChannel = (c: ChannelData) => {
      let tags = c.tags.length == 0 ? ['None'] : c.tags.map(t => this.tagAlias[t] ?? t).filter(t => t != '_')
      let lrCol = this.props.model.channels.col('lr')
      let labelFunc = ColEx.labelFunc(lrCol)
      let colorFunc = ColEx.colorFunc(lrCol)
      let lrColor = (v: string) => d3.color(colorFunc(v)).darker(2).hex()
      let advantage = ChannelEx.impressionAdvantagePercent(c)
      return (<>
        <a href={`https://www.youtube.com/channel/${c.channelId}`} target="blank">
          <img src={c.thumbnail} style={{ height: '7em', marginRight: '1em', clipPath: 'circle()' }} />
        </a>
        <div className="title-details">
          <div><b>{c.title}</b></div>
          <div>
            {c.relevantDailyViews == c.dailyViews ?
              <><b>{compactInteger(c.relevantDailyViews)}</b> relevant daily views <i>{dateFormat(c.publishedFrom)}</i> to <i>{dateFormat(c.publishedTo)}</i></>
              : <><b>{compactInteger(c.relevantDailyViews)}</b> relevant / <b>{compactInteger(c.dailyViews)}</b> daily views <i>{dateFormat(c.publishedFrom)}</i> to <i>{dateFormat(c.publishedTo)}</i></>}
          </div>
          <div>
            <b>{compactInteger(c.subCount)}</b> subscribers
              <span style={{ paddingLeft: 10 }}>
              {advantage >= 0 ? <span style={{ color: '#56b881' }}>▲</span> : <span style={{ color: '#e0393e' }}>▼</span>}  {formatNumber(advantage * 100, 0)}% impression advantage
              </span>
          </div>

          <div><span key={c.lr} className={'tag'} style={{ backgroundColor: lrColor(c.lr) }}>{labelFunc(c.lr)}</span>{tags.map(t => (<span key={t} className={'tag'}>{t}</span>))}</div>
        </div>
      </>)
    }

    return (
      <div className={'Title'}>
        <div className={'Card'}>
          {channel == null ? (
            <div style={{}}>
              <h2>Recfluence</h2>
              <p>Analysis of YouTube's political influence through recommendations</p>
            </div>
          ) : (
              renderChannel(channel)
            )}
        </div>
        <div className={'Search'} style={{}}>
          <SearchChannels model={this.props.model} onSelection={this.props.onSelection} selections={this.state.selections} />
        </div>
      </div>
    )
  }
}
