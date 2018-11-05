import * as React from 'react'
import * as d3 from 'd3'
import { sankey, sankeyLinkHorizontal, sankeyLeft } from 'd3-sankey'
import '../styles/Main.css'
import { layoutTextLabel, layoutGreedy, layoutLabel, layoutRemoveOverlaps } from 'd3fc-label-layout'
import { YtNetworks, ChannelNodeExtra, RecommendedLinkExtra, SChannelNode, SRecommendedLink, SGraph } from '../ts/YouTubeNetworks'
import { object } from 'prop-types'

interface Props {
  dataPath: string
  width: number
  height: number
}

export class RecommendFlows extends React.Component<Props, {}> {
  ref: SVGSVGElement
  onResize: () => void

  componentDidMount() {
    this.loadChart()
  }

  render() {
    return (
      <svg width={this.props.width} height={this.props.height} className="container" ref={(ref: SVGSVGElement) => (this.ref = ref)}>
        {/* SVG contents */}
      </svg>
    )
  }

  async loadData() {
    let allData = await YtNetworks.sankeyData(this.props.dataPath)

    let fromChannelId = 'UCJdKr0Bgd_5saZYqLCa9mng'// 'UCe02lGcO-ahAURWuxAJnjdA'  //'UCaXkIU1QidjPwiAYu6GcHjg'// //'UCzQUP1qoWDoEbmsQxvdjxgQ'// //rubin report

    // sankey is not bi-directional, so clone channels with a new id to represent input channels.
    let inChannels = allData.nodes.filter(c => allData.links.find(r => r.target == c.channelId)).map(c => {
      return { ...c, channelId: 'in.' + c.channelId }
    })

    let inLinks = allData.links.filter(r => r.target == fromChannelId).map(r => {
      return { ...r, source: 'in.' + r.source }
    })

    let outChannels = allData.nodes.filter(c => allData.links.find(r => r.source == c.channelId)).map(c => {
      return { ...c, channelId: 'out.' + c.channelId }
    })

    let outLinks = allData.links.filter(r => r.source == fromChannelId).map(r => {
      return { ...r, target: 'out.' + r.target }
    })

    let channels = inChannels.concat(outChannels).concat(allData.nodes.find(c => c.channelId == fromChannelId))

    let links = inLinks.concat(outLinks)
      .sort((a,b) => a.value - b.value).filter((c,i) => i < 20)
      .filter(l => channels.find(c => c.channelId == l.target) && channels.find(c => c.channelId == l.source))

    channels = channels.filter(c => links.find(l => l.source == c.channelId || l.target == c.channelId))

    // .sort(c => c.value).reverse()
    // .filter((c, i) => i < 20 || c.channelId == fromChannelId)

    return { nodes: channels, links: links }
  }

  async loadChart() {
    let data = await this.loadData()

    console.log(`data`, data)

    let lrPallet = YtNetworks.lrPallet()
    let width = this.props.width
    let height = this.props.height

    let layout = sankey<SChannelNode, SRecommendedLink>()
      .nodeWidth(36)
      .nodePadding(40)
      .size([width, height])
      .nodeAlign(sankeyLeft)
      .nodeId(d => d.channelId)

    let graph = layout(data)

    //layout.update(data)
    let svg = d3.select(this.ref)

    let linkPath = svg
      .append('g')
      .attr('class', 'links')
      .attr('fill', 'none')
      .attr('stroke', '#000')
      .attr('stroke-opacity', 0.2)
      .selectAll('path')

    let nodeG = svg
      .append('g')
      .attr('class', 'nodes')
      .attr('font-family', 'sans-serif')
      .attr('font-size', 10)
      .selectAll('g')

    let link = linkPath
      .data(graph.links)
      .enter()
      .append('path')
      .attr('d', sankeyLinkHorizontal())
      .attr('stroke-width', d => {
        return Math.max(1, d.width)
      })

    link.append('title').text(d => d.value)

    let node = nodeG
      .data(graph.nodes)
      .enter()
      .append('g')

    node
      .append('rect')
      .attr('x', d => d.x0)
      .attr('y', d => d.y0)
      .attr('height', d => {
        return Math.max(d.y1 - d.y0, 0)
      })
      .attr('width', d => d.x1 - d.x0)
      .attr('fill', d => lrPallet['L'])
      .attr('stroke', '#000')

    node
      .append('text')
      .attr('x', d => d.x0 - 6)
      .attr('y', d => (d.y1 + d.y0) / 2)
      .attr('dy', '0.35em')
      .attr('text-anchor', 'end')
      .text(d => `${d.title}`)
      .filter(d => d.x0 < width / 2)
      .attr('x', d => d.x1 + 6)
      .attr('text-anchor', 'start')

    //node.append('title').text(d => d.title)
  }
}
