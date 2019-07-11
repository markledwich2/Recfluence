import * as React from 'react'
import { InteractiveDataState, DataComponentHelper, ChartProps } from '../common/Charts'
import { YtNetworks, RelationsData, ChannelTopic, Topic } from '../common/YtData'
import _ from 'lodash'
import human from 'humanize-plus'

interface State extends InteractiveDataState { }
interface Props extends ChartProps<RelationsData> { }

export class ChannelTopics extends React.Component<Props, State> {
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
    const c = this.channel()
    if (!c) return (<div></div>);
    const topics = _(c.Topics).orderBy(t => t.onTopic).reverse().value()
    const h = '50px'

    let getDim = (t: ChannelTopic) => {
      let pix = t.onTopic * 8000
      let width = 300
      let rows = Math.floor(pix / width)
      let minHeight = 50
      let height = minHeight * rows
      if (height == 0) {
        height = minHeight
        width = pix
      }
      return { width, height }
    }

    let barsData = topics.map(topic => {
      return { topic, dim: getDim(topic) }
    })

    const bars = barsData.map(t => (
      <div key={t.topic.label()}
        style={{
          width: `${t.dim.width}px`, height: `${t.dim.height}px`, overflow: 'hidden', textOverflow: 'elipsis',
          textAlign: 'center', verticalAlign: 'middle', fontSize: '1.2em', fontWeight: 'bold'
        }}>
        <div style={{ background: t.topic.color(), margin: '5px', padding: '5px', height: '100%' }}>
          {human.formatNumber(t.topic.onTopic * 100, 0)}% {t.dim.width > 200 ? t.topic.label() : ""}
        </div>
      </div>
    ))
    return (
      <div style={{ display: 'flex', flexFlow: ' column wrap', maxHeight: '500px' }}>{bars}</div>
    )
  }


}
