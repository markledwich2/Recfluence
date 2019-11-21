import * as React from 'react'
import { InteractiveDataProps, InteractiveDataState } from '../common/Chart'
import { YtInteractiveChartHelper } from "../common/YtInteractiveChartHelper"
import { YtModel, ChannelData } from '../common/YtModel'
import { compactInteger } from 'humanize-plus'
import * as _ from 'lodash'
import { SearchChannels } from '../components/SearchChannels'
import * as dateformat from 'dateformat'
import { Dim } from '../common/Dim'

interface State extends InteractiveDataState { }
interface Props extends InteractiveDataProps<YtModel> { }

export class ChannelTitle extends React.Component<Props, State> {
  chart: YtInteractiveChartHelper = new YtInteractiveChartHelper(this)
  state: Readonly<State> = {
    selections: this.props.dataSet.selectionState
  }

  get dim(): Dim<ChannelData> {
    return this.props.dataSet.channelDim
  }

  channel() {
    const channelId = this.chart.selectionHelper
      .highlitedOrSelectedValue(this.dim.col("channelId"))
    return channelId ? this.props.dataSet.channels.find(c => c.channelId == channelId) : null
  }

  render() {
    let c = this.channel()
    let fdate = (d: string) => dateformat(new Date(d), 'd mmm yyyy')
    return (
      <div className={'Title'}>
        <div className={'Card'}>
          {c == null ? (
            <div style={{}}>
              <h2>Political YouTube</h2>
              <p>select a channel to see more detail</p>
            </div>
          ) : (
              <>
                <img src={c.thumbnail} style={{ height: '7em', marginRight: '1em', clipPath: 'circle()' }} />
                <div>
                  <h2>{c.title}</h2>
                  <b>{compactInteger(c.channelVideoViews)}</b> views for video's published
                <i> {fdate(c.publishedFrom)}</i> - <i>{fdate(c.publishedTo)}</i>
                  <br />
                  <b>{compactInteger(c.subCount)}</b> subscribers
                <br />
                  <a href={`https://www.youtube.com/channel/${c.channelId}`} target="blank">
                    Open in YouTube
                </a>
                </div>
              </>
            )}
        </div>
        <div className={'Search'} style={{}}>
          <SearchChannels dataSet={this.props.dataSet} onSelection={this.props.onSelection} selections={this.state.selections}  />
        </div>
      </div>
    )
  }
}
