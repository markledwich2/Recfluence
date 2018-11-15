import * as React from 'react'
import { InteractiveDataProps, InteractiveDataState, DataComponentHelper, DataSelections } from '../ts/Charts'
import { YtNetworks, Graph, YtData } from '../ts/YtData'
import { compactInteger } from 'humanize-plus'
import * as _ from 'lodash'

interface State extends InteractiveDataState {}
interface Props extends InteractiveDataProps<YtData> {}

export class ChannelTitle extends React.Component<Props, State> {
  chart: DataComponentHelper = new DataComponentHelper(this)
  state: Readonly<State> = {
    selections: new DataSelections()
  }

  channel() {
    let channelId = this.chart.filteredItems(YtNetworks.ChannelIdPath).find(() => true)
    return channelId ? this.props.dataSet.channels[channelId] : null
  }

  render() {
    let c = this.channel()
    let lrItems = _(YtNetworks.lrItems)
      .entries()
      .filter(lr => lr[0] != '')
    return (
      <div>
        <div style={{ display: 'flex', justifyContent: 'space-between' }}>
          <div>
            {c == null ? (
              <span><h2>YouTube Channel Recommendations</h2>
              <p>select a channel to see more detail</p></span>
            ) : (
              <div>
                <div><img src={c.Thumbnail} width={120} height={120} style={{margin:'10px'}}/></div>
                <h2>{c.Title}</h2>
                <p>
                  <b>{compactInteger(c.ChannelVideoViews)}</b> views in TODO date range
                  <br />
                  <b>{compactInteger(c.SubCount)}</b> subscribers
                  <br />
                  <a href={`https://www.youtube.com/channel/${c.ChannelId}`} target="blank">
                    Open in YouTube
                  </a>
                </p>
              </div>
            )}
          </div>
          <div style={{}}>
            <ul className={'legend'}>
              {lrItems.map(l => (
                <li style={{ color: l[1].color }} key={l[0]}>
                  <span className={'text'}>{l[1].text}</span>
                </li>
              ))}
            </ul>
          </div>
        </div>
      </div>
    )
  }
}
