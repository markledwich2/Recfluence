import * as React from 'react'
import { renderToString } from 'react-dom/server'
import * as d3 from 'd3'
import { sankey, sankeyLinkHorizontal, sankeyLeft, SankeyNode, SankeyLink } from 'd3-sankey'
import '../styles/Main.css'
import { YtModel, Graph, ChannelData } from '../common/YtModel'
import { YtInteractiveChartHelper } from "../common/YtInteractiveChartHelper"
import { compactInteger } from 'humanize-plus'
import * as _ from 'lodash'
import { ChannelChartState } from '../common/YtChart'
import { ChartProps, InteractiveDataState, SelectionStateHelper } from '../common/Chart'
import { SelectableCell, cellValue } from '../common/Dim'

interface State extends InteractiveDataState, ChannelChartState { }
interface Props extends ChartProps<YtModel> { }
interface ChannelNodeExtra extends NodeExtra {
  channelId: string
  size: number
}

interface RecommendFlowExtra {
  id: string
}

interface NodeExtra extends SelectableCell<ChannelData> {
  shapeId: string
  mode?: NodeMode
  incomming?: number
  outgoing?: number
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

  chart: YtInteractiveChartHelper = new YtInteractiveChartHelper(this)
  state: Readonly<State> = {
    selections: this.props.dataSet.selectionState,
    colorCol: "ideology"
  }

  dataRender: () => void

  componentDidMount() {
    this.createChart()
  }

  componentDidUpdate(prevProps: ChartProps<YtModel>, prevState: State) {
    this.dataRender()
  }

  render() {
    return <svg ref={(ref: SVGSVGElement) => (this.ref = ref)}>{/* SVG contents */}</svg>
  }

  renderedSelections: SelectionStateHelper

  layoutForAll(): Graph<Node[], Link[]> {
    const channels = this.props.dataSet.channels
    const channelDic = _(channels).keyBy(c => c.channelId).value()
    const colorBy = this.state.colorCol
    const cells = this.props.dataSet.channelDim.cells(channels, { group: [colorBy], order:{col:'dailyViews',order:'desc'} })

    let fromNodes: Node[] = cells
      .map(c => ({
        shapeId: `from.${cellValue(c, colorBy)}`,
        mode: NodeMode.From,
        ...c
      }))

    let toNodes = cells
      .map(c => ({
        shapeId: `to.${cellValue(c, colorBy)}`,
        mode: NodeMode.To,
        ...c
      }))

    let flows = _(this.props.dataSet.relations)
      .groupBy(r => `${channelDic[r.fromChannelId][colorBy]}.${channelDic[r.channelId][colorBy]}`)
      .map(
        (g, t) =>
          ({
            id: t,
            source: `from.${t.split('.')[0]}`,
            target: `to.${t.split('.')[1]}`,
            value: _(g).sumBy(r => +r.recommendsViewFlow)
          } as Link)
      )
      //.filter(f => f.source != f.target)
      .value()
    return { nodes: fromNodes.concat(toNodes), links: flows }
  }

  get dim() { return this.props.dataSet.channelDim }

  LayoutForChannel(channelId: string): Graph<ChannelNode[], ChannelLink[]> {
    let colorBy = this.state.colorCol
    const cells = this.dim.rowCells(this.props.dataSet.channels, { group: ['channelId'], colorBy, label: 'title'})

    let nodes: ChannelNodeExtra[] = cells
      .map(
        c =>
          ({
            shapeId: c.row.channelId,
            channelId: c.row.channelId,
            size: +c.row.channelVideoViews,
            ...c.cell
          })
      )

    let links = this.props.dataSet.relations
      //.filter(c => c.channelId != c.fromChannelId)
      .map(n => {
        return {
          id: `${n.fromChannelId}.${n.channelId}`,
          source: n.fromChannelId,
          target: n.channelId,
          value: +n.recommendsViewFlow * 10 // impressions
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

    let outChannels = nodes
      .map(c => ({ ...c, shapeId: 'out.' + c.channelId, mode: NodeMode.To } as ChannelNode))
      .filter(c => finalLinks.some(r => r.target == c.shapeId))

    let mainChannel: ChannelNode = {
      ...nodes.find(c => c.channelId == channelId),
      mode: NodeMode.Main,
      incomming: _.sum(inLinks.map(l => l.value)),
      outgoing: _.sum(outLinks.map(l => l.value))
    } // calculate value now as to ignore the filter
    let channels = inChannels.concat(outChannels).concat([{ ...mainChannel, shapeId: mainChannel.channelId } as ChannelNode])
    this.chart.selectionHelper.updateSelectableCells(channels)

    return { nodes: channels, links: finalLinks }
  }

  async createChart() {
    let svg = d3.select(this.ref)
    let container = this.chart.createContainer(svg, 'flows')
    let dH = 60
    container.attr('transform', `translate(0, ${dH})`) // move down 10 px to allow space for title
    let linkG = container.append('g').attr('class', 'links')
    let nodeG = container.append('g').attr('class', 'nodes')

    this.dataRender = () => {
      var w = this.props.width
      var h = this.props.height

      svg.attr('width', w)
      svg.attr('height', h)

      const dim = this.props.dataSet.channelDim
      const idAttribute = dim.col("channelId")

      let channelId = this.chart.selectionHelper.selectedSingleValue(idAttribute)

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

      let updateNode = nodeG.selectAll('g.node')
        .data<Node>(graph.nodes, (n: ChannelNode) => n.shapeId)

      let enterNode = updateNode
        .enter()
        .append<SVGGElement>('g')
        .attr('class', 'node')
      let enterNodeRec = enterNode
        .append('rect')
        .attr('width', d => d.x1 - d.x0)
        .attr('height', d => Math.max(d.y1 - d.y0, 1))

      // add events for both l/r and channel nodes
      this.chart.addShapeEvents(enterNodeRec, true)

      enterNode.append('g').attr('class', 'label')

      // update the nodes rectangle properties
      let mergeNode = updateNode.merge(enterNode)
      let mergedRec = mergeNode
        .style('display', 'inherit')
        .style('opacity', 1)
        .select('rect')
        .attr('x', d => d.x0)
        .attr('y', d => d.y0)
        .attr('height', d => Math.max(d.y1 - d.y0, 1))
        .attr('width', d => d.x1 - d.x0)
        .attr('fill', d => d.color)

      this.chart.addShapeClasses(mergedRec)

      // //highlight shape when selected
      // if (channelId)
      //   mergeNode.select('rect')
      //     .classed('highlight', d => d.highlighted)
      //.style('stroke', d => (highlightedChannels.some(id => id == d.channelId) ? '#ddd' : null))

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
            {d.label}
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
                <tspan className={'subtitle'}> impressions</tspan>
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
