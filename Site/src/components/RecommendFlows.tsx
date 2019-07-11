import React from 'react'
import { renderToString } from 'react-dom/server'
import * as d3 from 'd3'
import { sankey, sankeyLinkHorizontal, sankeyLeft, SankeyNode, SankeyLink } from 'd3-sankey'
import '../styles/Main.css'
import { YtNetworks, RelationsData, Graph, RelationData, Topic } from '../common/YtData'
import { ChartProps, InteractiveDataState, DataSelections, DataComponentHelper } from '../common/Charts'
import { jsonEquals, jsonClone } from '../common/Utils'
import { compactInteger } from 'humanize-plus'
import _ from 'lodash'
import { oc } from 'ts-optchain'

interface State extends InteractiveDataState { }
interface Props extends ChartProps<RelationsData> { }
interface ChannelNodeExtra extends NodeExtra {
  channelId: string
  size: number
  type: string
}
interface RecommendFlowExtra {
  id: string
}
interface NodeExtra {
  shapeId: string
  lr: string
  title: string
  mode: NodeMode
  incomming: number
  outgoing: number
  topic:Topic
  color:string
}

type ChannelNode = SankeyNode<ChannelNodeExtra, RecommendFlowExtra>
type ChannelLink = SankeyLink<ChannelNodeExtra, RecommendFlowExtra>
type Node = SankeyNode<NodeExtra, RecommendFlowExtra>
type Link = SankeyLink<NodeExtra, RecommendFlowExtra>

enum NodeMode {
  Default = 'Default',
  From = 'From',
  To = 'To',
  Main = 'Main'
}

export class RecommendFlows extends React.Component<Props, State> {
  ref: SVGSVGElement

  chart: DataComponentHelper = new DataComponentHelper(this)
  state: Readonly<State> = {
    selections: this.props.initialSelection
  }

  dataRender: () => void

  componentDidMount() {
    this.createChart()
  }

  componentDidUpdate(prevProps: ChartProps<RelationsData>, prevState: State) {
    this.dataRender()
  }

  render() {
    return <svg ref={(ref: SVGSVGElement) => (this.ref = ref)}>{/* SVG contents */}</svg>
  }

  renderedSelections: DataSelections

  layoutForAll(): Graph<Node[], Link[]> {
    let channels = this.props.dataSet.channels
    let byType = _(channels).groupBy(c => c.Topic)
    let topics = this.props.dataSet.topics

    let fromNodes = byType
      .map(
        (g, t) =>
          ({
            shapeId: `from.${t}`,
            topic: topics[t],
            title: t, // YtNetworks.lrText(t),
            mode: NodeMode.From,
            color: oc(topics[t]).color()
          } as Node)
      )
      .value()

    let toNodes = byType
      .map(
        (g, t) =>
          ({
            shapeId: `to.${t}`,
            topic: topics[t],
            title: t, //YtNetworks.lrText(t),
            mode: NodeMode.To
          } as Node)
      )
      .value()

    let flows = _(this.props.dataSet.relations)
      .groupBy(r => `${channels[r.FromChannelId].Topic}.${channels[r.ChannelId].Topic}`)
      .map(
        (g, t) =>
          ({
            id: t,
            source: `from.${t.split('.')[0]}`,
            target: `to.${t.split('.')[1]}`,
            value: _(g).sumBy(r => +r.RecommendsViewFlow)
          } as Link)
      )
      .filter(f => f.source != f.target)
      .value()

    return { nodes: fromNodes.concat(toNodes), links: flows }
  }

  LayoutForChannel(channelId: string): Graph<ChannelNode[], ChannelLink[]> {
    let nodes = _(this.props.dataSet.channels)
      .map(
        n =>
          ({
            channelId: n.ChannelId,
            title: n.Title,
            size: +n.ChannelVideoViews,
            lr: n.LR,
            topic: oc(n.Topic).topic()
          } as ChannelNode)
      )
      .keyBy(c => c.channelId)

    let links = this.props.dataSet.relations.map(n => {
      return {
        id: `${n.FromChannelId}.${n.ChannelId}`,
        source: n.FromChannelId,
        target: n.ChannelId,
        value: +n.RecommendsViewFlow
      } as ChannelLink
    })

    var maxNodes = 10

    let inLinks = links.filter(r => r.target == channelId).map(r => ({ ...r, source: 'in.' + r.source } as ChannelLink))
    let outLinks = links.filter(r => r.source == channelId).map(r => ({ ...r, target: 'out.' + r.target } as ChannelLink))
    let finalLinks = _(inLinks)
      .orderBy(l => l.value, 'desc')
      .slice(0, maxNodes)
      .concat(
        _(outLinks)
          .orderBy(l => l.value, 'desc')
          .slice(0, maxNodes)
          .value()
      )
      .value()

    // sankey is not bi-directional, so clone channels with a new id to represent input channels.
    let inChannels = nodes
      .map(c => ({ ...c, shapeId: 'in.' + c.channelId, mode: NodeMode.From } as ChannelNode))
      .filter(c => finalLinks.some(r => r.source == c.shapeId))
      .value()

    let outChannels = nodes
      .map(c => ({ ...c, shapeId: 'out.' + c.channelId, mode: NodeMode.To } as ChannelNode))
      .filter(c => finalLinks.some(r => r.target == c.shapeId))
      .value()

    let mainChannel: ChannelNode = {
      ...nodes.find(c => c.channelId == channelId),
      mode: NodeMode.Main,
      incomming: _.sum(inLinks.map(l => l.value)),
      outgoing: _.sum(outLinks.map(l => l.value))
    } // calculate value now as to ignore the filter
    let channels = inChannels.concat(outChannels).concat([{ ...mainChannel, shapeId: mainChannel.channelId } as ChannelNode])

    //channels = channels.filter(c => finalLinks.some(l => l.source == c.shapeId || l.target == c.shapeId))

    return { nodes: channels, links: finalLinks }
  }

  async createChart() {
    let svg = d3.select(this.ref)
    let container = this.chart.createContainer(svg)
    let dH = 60
    container.attr('transform', `translate(0, ${dH})`) // move down 10 px to allow space for title
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
      let highlightedChannels = this.chart.filteredOrHighlightedItems(YtNetworks.ChannelIdPath)

      let channelId = this.chart.filteredItems(YtNetworks.ChannelIdPath).find(() => true)
      let { nodes, links } = channelId ? this.LayoutForChannel(channelId) : this.layoutForAll()

      let layout = sankey<NodeExtra, RecommendFlowExtra>()
        .nodeWidth(36)
        .nodePadding(40)
        .size([w, h - dH - 20])
        .nodeAlign(sankeyLeft)
        .nodeId(d => d.shapeId)

      let graph = layout({ nodes, links })
      let updateLink = linkG.selectAll('path').data(graph.links, (l: ChannelLink) => l.id)
      let enterLink = updateLink
        .enter()
        .append('path')
        .attr('class', 'link')

      updateLink
        .merge(enterLink)
        .attr('d', sankeyLinkHorizontal())
        .attr('stroke-width', d => d.width)
        .attr('stroke', d => {
          let s = d.source as Node
          let t = d.target as Node
          return s.mode == NodeMode.From ? s.color : t.color
        })

      updateLink.exit().remove()

      let updateNode = nodeG.selectAll('g.node').data(graph.nodes, (n: ChannelNode) => n.shapeId)
      let enterNode = updateNode
        .enter()
        .append('g')
        .attr('class', 'node')
      enterNode
        .append('rect')
        .attr('class', 'selectable')
        .attr('width', d => d.x1 - d.x0)
        .attr('height', d => Math.max(d.y1 - d.y0, 1))

      enterNode.append('g').attr('class', 'label')

      if (channelId) this.chart.addDataShapeEvents(enterNode.select('rect'), (n: ChannelNode) => n.channelId, YtNetworks.ChannelIdPath)

      // update the nodes rectangle properties
      let mergeNode = updateNode.merge(enterNode)
      mergeNode
        .style('display', 'inherit')
        .style('opacity', 1)
        .select('rect')
        .attr('x', d => d.x0)
        .attr('y', d => d.y0)
        .attr('height', d => Math.max(d.y1 - d.y0, 1))
        .attr('width', d => d.x1 - d.x0)
        .attr('fill', d => d.color)

      //highlight shape when selected
      if (channelId)
        mergeNode.select('rect').style('stroke', (d: ChannelNode) => (highlightedChannels.some(id => id == d.channelId) ? '#ddd' : null))

      let txtMode = new Map([
        [
          NodeMode.Main,
          {
            anchor: 'start',
            getX: (d: Node) => d.x0,
            getY: (d: Node) => d.y0 - 45
          }
        ],
        [
          NodeMode.From,
          {
            anchor: 'start',
            getX: (d: Node) => d.x1 + 10,
            getY: (d: Node) => (d.y1 + d.y0) / 2
          }
        ],
        [
          NodeMode.To,
          {
            anchor: 'end',
            getX: (d: Node) => d.x0 - 10,
            getY: (d: Node) => (d.y1 + d.y0) / 2
          }
        ]
      ])

      let labelText = (d: Node) => {
        // sourceLInks are objects with the source set to this object (i.e. outgoing)
        let incomming = d.incomming > 0 ? d.incomming : _.sum(d.targetLinks.map(l => l.value))
        let outgoing = d.outgoing > 0 ? d.outgoing : _.sum(d.sourceLinks.map(l => l.value))
        return (
          <text className={'label'} textAnchor={txtMode.get(d.mode).anchor}>
            {d.title}
            {incomming > 0 && (
              <>
                <tspan className={'subtitle-bold'} dy={'1.3em'} x={0}>
                  {compactInteger(incomming, 1)}
                </tspan>
                <tspan className={'subtitle'}> received</tspan>
              </>
            )}
            {outgoing > 0 && (
              <>
                <tspan className={'subtitle-bold'} dy={'1.3em'} x={0}>
                  {compactInteger(outgoing, 1)}
                </tspan>
                <tspan className={'subtitle'}> viewed recommendations</tspan>
              </>
            )}
          </text>
        )
      }

      mergeNode
        .select('g.label')
        .attr('transform', d => `translate(${txtMode.get(d.mode).getX(d)}, ${txtMode.get(d.mode).getY(d)})`) //translate makes g coodinates relative
        .html(d => renderToString(labelText(d)))

      let exitNode = updateNode.exit()

      exitNode
        .transition()
        .duration(300)
        .style('opacity', 0)


      exitNode.transition().delay(300).remove()
    }

    this.dataRender()
  }
}
