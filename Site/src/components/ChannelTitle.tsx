import * as React from 'react'
import { InteractiveDataProps, InteractiveDataState, DataComponentHelper, DataSelections } from '../ts/Charts'
import { YtNetworks, Graph, YtData } from '../ts/YtData'
import { compactInteger } from 'humanize-plus'
import * as _ from 'lodash'
import { SearchChannels } from '../components/SearchChannels'
import * as dateformat from 'dateformat'

interface State extends InteractiveDataState {}
interface Props extends InteractiveDataProps<YtData> {}

export class ChannelTitle extends React.Component<Props, State> {
  chart: DataComponentHelper = new DataComponentHelper(this)
  state: Readonly<State> = {
    selections: this.props.initialSelection
  }

  channel() {
    let channelId = this.chart.highlightedItems(YtNetworks.ChannelIdPath).find(() => true)
    if (!channelId) channelId = this.chart.filteredItems(YtNetworks.ChannelIdPath).find(() => true)
    return channelId ? this.props.dataSet.channels[channelId] : null
  }

  render() {
    let c = this.channel()
    let fdate = (d: string) => dateformat(new Date(d), 'd mmm yyyy')
    return (
      <div className={'Title'}>
        <div className={'Card'}>
          {c == null ? (
            <div style={{}}>
              <h2>YouTube Channel Recommendations</h2>
              <p>select a channel to see more detail</p>
            </div>
          ) : (
            <>
              <img src={c.Thumbnail} style={{ height: '7em', marginRight: '1em', clipPath: 'circle()' }} />
              <div>
                <h3>{c.Title}</h3>
                <p>
                  <b>{compactInteger(c.ChannelVideoViews)}</b> views for video's published
                  <i> {fdate(c.PublishedFrom)}</i> - <i>{fdate(c.PublishedTo)}</i>
                  <br />
                  <b>{compactInteger(c.SubCount)}</b> subscribers
                  <br />
                  <a href={`https://www.youtube.com/channel/${c.ChannelId}`} target="blank">
                    Open in YouTube
                  </a>
                </p>
              </div>
            </>
          )}
        </div>
        <div className={'Search'} style={{}}>
          <SearchChannels dataSet={this.props.dataSet} onSelection={this.props.onSelection} />
        </div>
      </div>
    )
  }
}
