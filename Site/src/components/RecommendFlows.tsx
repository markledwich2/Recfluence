import * as React from 'react'
import * as d3 from 'd3'
import { sankey, sankeyLinkHorizontal, sankeyLeft, SankeyNode, SankeyLink } from 'd3-sankey'
import '../styles/Main.css'
import { YtNetworks, YtData, Graph, RelationData } from '../ts/YtData'
import { ChartProps, InteractiveDataState, DataSelections, DataComponentHelper } from '../ts/Charts'
import { jsonEquals, jsonClone } from '../ts/Utils'
import { compactInteger } from 'humanize-plus'
import * as _ from 'lodash'

interface State extends InteractiveDataState {}
interface Props extends ChartProps<YtData> {}
interface ChannelNodeExtra extends LrNodeExtra {
  channelId: string
  size: number
  type: string
}
interface RecommendFlowExtra {
  id: string
}
interface LrNodeExtra {
  shapeId: string
  lr: string
  title: string
}

type ChannelNode = SankeyNode<ChannelNodeExtra, RecommendFlowExtra>
type ChannelLink = SankeyLink<ChannelNodeExtra, RecommendFlowExtra>
type LrNode = SankeyNode<LrNodeExtra, RecommendFlowExtra>
type LrLink = SankeyLink<LrNodeExtra, RecommendFlowExtra>


export class RecommendFlows extends React.Component<Props, State> {
  ref: SVGSVGElement

  chart: DataComponentHelper = new DataComponentHelper(this)
  state: Readonly<State> = {
    selections: new DataSelections()
  }

  dataRender: () => void

  componentDidMount() {
    this.createChart()
  }

  componentDidUpdate(prevProps: ChartProps<YtData>, prevState: State) {
    this.dataRender()
  }

  render() {
    return <svg ref={(ref: SVGSVGElement) => (this.ref = ref)}>{/* SVG contents */}</svg>
  }

  renderedSelections: DataSelections

  layoutForAll(): Graph<LrNode[], LrLink[]> {
    let channels = this.props.dataSet.channels
    let byType = _(channels).groupBy(c => c.LR)

    let fromNodes = byType.map(
      (g, t) =>
        ({
          shapeId: `from.${t}`,
          lr: t,
          title: YtNetworks.lrItems.get(t).text
        } as LrNode)
    ).value()

    let toNodes = byType.map(
      (g, t) =>
        ({
          shapeId: `to.${t}`,
          lr: t,
          title: YtNetworks.lrItems.get(t).text
        } as LrNode)
    ).value()

    let flows = _(this.props.dataSet.relations)
      .groupBy<RelationData>(r => `${channels[r.FromChannelId].LR}.${channels[r.ChannelId].LR}`)
      .map(
        (g, t) =>
          ({
            id: t,
            source: `from.${t.split('.')[0]}`,
            target: `to.${t.split('.')[1]}`,
            value: _(g).sumBy(r => +r.RecommendsViewFlow)
          } as LrLink)
      ).filter(f => f.source != f.target).value()

    return { nodes: fromNodes.concat(toNodes), links: flows }
  }

  LayoutForChannel(channelId: string): Graph<ChannelNode[], ChannelLink[]> {
    let nodes = _(this.props.dataSet.channels)
      .map(
        n =>
          ({
            channelId: n.ChannelId,
            title: n.Title,
            size: n.ChannelVideoViews,
            lr: n.LR
          } as ChannelNode)
      )
      .keyBy(c => c.channelId)

    let links = this.props.dataSet.relations.map(n => {
      return {
        id: `${n.FromChannelId}.${n.ChannelId}`,
        source: n.FromChannelId,
        target: n.ChannelId,
        value: n.RecommendsViewFlow
      } as ChannelLink
    })

    var maxNodes = 20

    // sankey is not bi-directional, so clone channels with a new id to represent input channels.
    let inChannels = nodes
      .filter(c => links.some(r => r.target == c.channelId))
      .map(c => ({ ...c, shapeId: 'in.' + c.channelId } as ChannelNode)).value()
    let inLinks = links.filter(r => r.target == channelId).map(r => ({ ...r, source: 'in.' + r.source } as ChannelLink))
    let outChannels = nodes
      .filter(c => links.some(r => r.source == c.channelId))
      .map(c => ({ ...c, shapeId: 'out.' + c.channelId } as ChannelNode)).value()
    let outLinks = links.filter(r => r.source == channelId).map(r => ({ ...r, target: 'out.' + r.target } as ChannelLink))
    let mainChannel = nodes.find(c => c.channelId == channelId)
    let channels = inChannels.concat(outChannels).concat([{ ...mainChannel, shapeId: mainChannel.channelId } as ChannelNode])
    let finalLinks = _(inLinks.concat(outLinks))
      .orderBy(l => l.value, 'desc')
      .filter(l => channels.some(c => c.shapeId == l.target) && channels.some(c => c.shapeId == l.source))
      .slice(0, maxNodes).value()
    channels = channels.filter(c => finalLinks.some(l => l.source == c.shapeId || l.target == c.shapeId))

    return { nodes: channels, links: finalLinks }
  }

  async createChart() {
    let svg = d3.select(this.ref)
    let container = this.chart.createContainer(svg)
    let linkG = container.append('g').attr('class', 'links')
    let nodeG = container.append('g').attr('class', 'nodes')

    this.dataRender = () => {
      var w = this.props.width
      var h = this.props.height

      svg.attr('width', w)
      svg.attr('height', h)

      //if (this.renderedSelections != null && jsonEquals(this.renderedSelections.filters, this.state.selections.filters))
      //  return
      this.renderedSelections = jsonClone(this.state.selections)
      let selectedChannels = this.chart.filteredItems(YtNetworks.ChannelIdPath)

      let channelId = this.chart.filteredItems(YtNetworks.ChannelIdPath).find(() => true)
      let { nodes, links } = channelId ? this.LayoutForChannel(channelId) : this.layoutForAll()

      let layout = sankey<LrNodeExtra, RecommendFlowExtra>()
        .nodeWidth(36)
        .nodePadding(40)
        .size([w, h])
        .nodeAlign(sankeyLeft)
        .nodeId(d => d.shapeId)

      let graph = layout({ nodes, links })
      let updateLink = linkG.selectAll('path').data(graph.links, (l: ChannelLink) => l.id)
      let enterLink = updateLink
        .enter()
        .append('path')
        .attr('class', 'link')
        .attr('stroke', d => YtNetworks.lrColor((d.source as LrNode).lr))

      updateLink
        .merge(enterLink)
        .attr('d', sankeyLinkHorizontal())
        .attr('stroke-width', d => d.width)

      updateLink.exit().remove()

      let updateNode = nodeG.selectAll('g').data(graph.nodes, (n: ChannelNode) => n.shapeId)
      let enterNode = updateNode.enter().append('g')
      // .attr('x', d => d.x0)
      // .attr('y', d => d.y0)

      enterNode
        .append('rect')
        .attr('class', 'selectable')
        .attr('width', d => d.x1 - d.x0)
        .attr('height', d => Math.max(d.y1 - d.y0, 1))

      let nodeText = enterNode.append('text').attr('class', 'label')

      let recommendTextFunc = (d: LrNode): string => {
        let incomming = d3.sum(d.sourceLinks.map(l => l.value))
        let outgoing = d3.sum(d.targetLinks.map(l => l.value))
        let tokens = new Array<string>()
        if (d.targetLinks.length > 0) tokens.push(`${compactInteger(outgoing, 1)} received`)
        if (d.sourceLinks.length > 0) tokens.push(`${compactInteger(incomming, 1)} recomendations`)
        return tokens.length > 0 ? tokens.join(', ') : 'no recommendations'
      }

      nodeText.append('tspan').text(d => `${d.title}`)
      nodeText
        .append('tspan')
        .attr('class', 'subtitle')
        .attr('dy', '1.2em')
        .attr('class', 'subtitle')

      if (channelId) this.chart.addDataShapeEvents(enterNode.select('rect'), (n: ChannelNode) => n.channelId, YtNetworks.ChannelIdPath)

      let mergeNode = updateNode.merge(enterNode)
      mergeNode
        .select('rect')
        .attr('x', d => d.x0)
        .attr('y', d => d.y0)
        .attr('height', d => Math.max(d.y1 - d.y0, 1))
        .attr('width', d => d.x1 - d.x0)
        .attr('fill', d => YtNetworks.lrColor(d.lr))

      if (channelId) mergeNode.style('stroke', (d: ChannelNode) => (selectedChannels.some(id => id == d.channelId) ? '#ddd' : null))

      let isLeft = (d: LrNode) => d.x0 < w / 2
      let textX = (d: LrNode) => (isLeft(d) ? d.x1 + 10 : d.x0 - 10)
      mergeNode
        .select('text')
        .attr('x', textX)
        .attr('y', d => (d.y1 + d.y0) / 2)
        .attr('text-anchor', d => (isLeft(d) ? 'start' : 'end'))
        .select('tspan.subtitle')
        .attr('x', textX)
        .text(d => recommendTextFunc(d))
      updateNode
        .exit()
        .transition()
        .duration(500)
        .style('opacity', 0)
        .remove()
    }

    this.dataRender()
  }
}

