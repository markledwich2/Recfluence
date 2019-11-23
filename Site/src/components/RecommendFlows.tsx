import * as React from 'react'
import { renderToString } from 'react-dom/server'
import * as d3 from 'd3'
import { sankey, sankeyLinkHorizontal, sankeyLeft, SankeyNode, SankeyLink } from 'd3-sankey'
import '../styles/Main.css'
import { YtModel, Graph, ChannelData } from '../common/YtModel'
import { YtInteractiveChartHelper } from "../common/YtInteractiveChartHelper"
import { compactInteger } from 'humanize-plus'
import * as _ from 'lodash'
import { ChartProps, InteractiveDataState } from '../common/Chart'
import { SelectableCell, ColEx, Cell, CellEx } from '../common/Dim'
import { typedKeys } from '../common/Utils'

interface State extends InteractiveDataState { }
interface Props extends ChartProps<YtModel> { }

interface RecommendFlowExtra {
  id: string
}

interface NodeExtra extends SelectableCell<ChannelData> {
  shapeId: string
  mode?: NodeMode
  incomming?: number
  outgoing?: number
}

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
    selections: this.props.model.selectionState
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

  layoutForAll(): Graph<Node[], Link[]> {
    const channels = this.props.model.channels
    const channelDic = _(channels).keyBy(c => c.channelId).value()
    const colorBy = this.chart.selections.params().colorBy
    const cells = this.props.model.channelDim.cells(channels, { group: [colorBy], order: { col: 'dailyViews', order: 'desc' } })

    let fromNodes: Node[] = cells
      .map(c => ({
        shapeId: `from.${CellEx.cellValue(c, colorBy)}`,
        mode: NodeMode.From,
        ...c
      }))

    let toNodes = cells
      .map(c => ({
        shapeId: `to.${CellEx.cellValue(c, colorBy)}`,
        mode: NodeMode.To,
        ...c
      }))

    let flows = _(this.props.model.recs)
      .groupBy(r => `${channelDic[r.fromChannelId][colorBy]}.${channelDic[r.channelId][colorBy]}`)
      .map(
        (g, t) =>
          ({
            id: t,
            source: `from.${t.split('.')[0]}`,
            target: `to.${t.split('.')[1]}`,
            value: _(g).sumBy(r => +r.relevantImpressions)
          } as Link)
      )
      //.filter(f => f.source != f.target)
      .value()
    return { nodes: fromNodes.concat(toNodes), links: flows }
  }

  get dim() { return this.props.model.channelDim }

  LayoutForSelection(selection: Record<keyof ChannelData, string>): Graph<Node[], Link[]> {
    const colorBy = this.chart.selections.params().colorBy
    const cDic = _.keyBy(this.props.model.channels, c => c.channelId)
    const groupId = (c: Cell<ChannelData> | ChannelData) =>
      CellEx.isCell(c) ?
        ColEx.valueString(_.values(c.keys))
        : ColEx.valueStringCol(c, typedKeys(selection))

    const nodes = _(this.dim.cells(this.props.model.channels, { group: typedKeys(selection), colorBy }))
      .map(n => ({ shapeId: groupId(n), ...n }))
      .keyBy(n => n.shapeId).value()

    let links = _(this.props.model.recs)
      .map(r => {
        let from = cDic[r.fromChannelId]
        let to = cDic[r.channelId]
        return ({ ...r, from, to, groupId: `${groupId(from)}.${groupId(to)}` })
      })
      .groupBy(r => r.groupId)
      .map((g, k) => {
        return {
          id: k,
          source: groupId(g[0].from),
          target: groupId(g[0].to),
          value: _.sumBy(g, r => r.relevantImpressions)
        } as Link
      }).value()

    var maxNodes = 10
    let selectedKey = ColEx.valueString(_.values(selection))

    let inLinks = links.filter(r => r.target == selectedKey)
      .map(r => ({ ...r, source: 'in.' + r.source } as Link))
    let outLinks = links.filter(r => r.source == selectedKey)
      .map(r => ({ ...r, target: 'out.' + r.target } as Link))
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
    let inChannels = _(nodes)
      .map(c => ({ ...c, shapeId: 'in.' + groupId(c), mode: NodeMode.From } as Node))
      .filter(c => finalLinks.some(r => r.source == c.shapeId)).value()

    let outChannels = _(nodes)
      .map(c => ({ ...c, shapeId: 'out.' + groupId(c), mode: NodeMode.To } as Node))
      .filter(c => finalLinks.some(r => r.target == c.shapeId)).value()

    let mainChannel: Node = {
      ..._(nodes).find(c => groupId(c) == selectedKey),
      mode: NodeMode.Main,
      incomming: _.sum(inLinks.map(l => l.value)),
      outgoing: _.sum(outLinks.map(l => l.value))
    } // calculate value now as to ignore the filter
    let channels = inChannels.concat(outChannels).concat([{ ...mainChannel, shapeId: mainChannel.shapeId } as Node])
    this.chart.selections.updateSelectableCells(channels)

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

      let highlightedOrSelected = this.chart.selections.highlightedOrSelected()

      this.chart.selections.highlightedOrSelected()

      let { nodes, links } = highlightedOrSelected ? this.LayoutForSelection(highlightedOrSelected) : this.layoutForAll()

      let layout = sankey<NodeExtra, RecommendFlowExtra>()
        .nodeWidth(36)
        .nodePadding(40)
        .size([w, h - dH - 20])
        .nodeAlign(sankeyLeft)
        .nodeId(d => d.shapeId)

      let graph = layout({ nodes, links })
      let updateLink = linkG.selectAll('path').data(graph.links, (l: Link) => l.id)
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
        .data<Node>(graph.nodes, (n: Node) => n.shapeId)

      let enterNode = updateNode
        .enter()
        .append<SVGGElement>('g')
        .attr('class', 'node')
      let enterNodeRec = enterNode
        .append('rect')
        .attr('width', d => d.x1 - d.x0)
        .attr('height', d => Math.max(d.y1 - d.y0, 1))

      // add events for both l/r and channel nodes
      this.chart.addShapeEvents(enterNodeRec, true, false)

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

      updateNode.exit().remove()

      this.chart.addShapeClasses(mergedRec)

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
    }

    this.dataRender()
  }
}
