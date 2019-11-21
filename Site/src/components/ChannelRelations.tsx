import * as React from 'react'
import * as d3 from 'd3'
import '../styles/Main.css'
import { layoutTextLabel, layoutGreedy, layoutLabel } from 'd3fc-label-layout'
import { YtModel, ChannelData } from '../common/YtModel'
import { Col, Cell, SelectableCell, Dim } from '../common/Dim'
import { YtInteractiveChartHelper } from "../common/YtInteractiveChartHelper"
import * as _ from 'lodash'
import { ChannelChartState, YtChart } from '../common/YtChart'
import { ChartProps, InteractiveDataState } from '../common/Chart'

interface State extends InteractiveDataState, ChannelChartState { }
interface Props extends ChartProps<YtModel> { }
interface Link extends d3.SimulationLinkDatum<Node> {
  strength: number
  color: string
}

interface Node extends d3.SimulationNodeDatum, SelectableCell<Node> {
  channelId: string
  size: number
}

export class ChannelRelations extends React.Component<Props, State> {
  ref: SVGSVGElement

  chart: YtInteractiveChartHelper = new YtInteractiveChartHelper(this)

  state: Readonly<State> = {
    selections: this.props.dataSet.selectionState,
    colorCol: "ideology"
  }

  componentDidMount() {
    this.loadChart()
  }

  componentDidUpdate(prevProps: Props, prevState: State) {
    this.stateRender(prevProps, prevState)
  }

  render() {
    return (<svg ref={ref => (this.ref = ref)} />)
  }

  // a cheap render. Set by loadChat() so that it can use a buch of context
  stateRender: (prevProps: Props, prevState: State) => void

  getData() {
    const channelCells = _.keyBy(this.dim.rowCells(this.props.dataSet.channels,
      {
        group: ['channelId'], colorBy: this.state.colorCol,
        label: 'title'
      }), c => c.row.channelId)

    let nodes: Node[] = _(channelCells)
      .filter(c => c.row.channelVideoViews > 0)
      .map(
        c =>
          ({
            channelId: c.row.channelId,
            size: c.row.channelVideoViews,
            ...c.cell
          } as Node)
      ).value()

    let links = _(this.props.dataSet.relations)
      .filter(l => l.channelId != l.fromChannelId) //l.recommendsViewChannelPercent > 0.005 && 
      .map(
        l =>
          ({
            source: l.fromChannelId,
            target: l.channelId,
            strength: l.recommendsViewChannelPercent,
            color: channelCells[l.fromChannelId].cell.color
          } as Link)
      )
      .filter(
        l => (nodes.some(c => c.channelId == (l.source as string)) && nodes.some(c => c.channelId == (l.target as string)))
      )
      .value()

    let keyedNodes = nodes.filter(n => links.some(l => n.channelId == (l.source as string) || n.channelId == (l.target as string)))

    let adjlist = new Map()
    links.forEach(d => {
      adjlist.set(d.source + '-' + d.target, true)
      adjlist.set(d.target + '-' + d.source, true)
    })

    let isConnected = (a: string, b: string) => a == b || adjlist.get(a + '-' + b)

    return { nodes: keyedNodes, links: links, isConnected }
  }

  private get dim() {
    return this.props.dataSet.channelDim
  }

  getLayout(nodes: Node[], links: Link[]) {
    let simSize = 1024
    let maxStrength = d3.max(links, l => l.strength)
    let maxSize = d3.max(nodes, n => n.size)
    let getNodeRadius = (d: Node) => Math.sqrt(d.size > 0 ? d.size / maxSize : 1) * 40
    let getLineWidth = (d: Link) => (d.strength / maxStrength) * 100
    let centerForce = d3.forceCenter(simSize / 2, simSize / 2)
    let force = d3
      .forceSimulation<Node, Link>(nodes)
      .force('charge', d3.forceManyBody().strength(-40))
      .force('center', centerForce)
      .force(
        'link',
        d3
          .forceLink<Node, Link>(links)
          .distance(1)
          .id(d => d.channelId)
          .strength(d => (d.strength / maxStrength) * 0.7)
      )
      .force('collide', d3.forceCollide<Node>(getNodeRadius))

    return { force, getLineWidth, getNodeRadius, simSize }
  }

  loadChart() {
    const { nodes, links, isConnected } = this.getData()
    const lay = this.getLayout(nodes, links)

    let svg = d3.select(this.ref)
    let container = this.chart.createContainer(svg, 'relations')

    let linkEnter = container
      .append('g')
      .attr('class', 'links')
      .selectAll('line')
      .data(links)
      .enter()
      .append<SVGLineElement>('line')
      .attr('class', 'link')
      .attr('stroke-width', lay.getLineWidth)
      .attr('stroke', (l: Link) => l.color)

    let nodesContainer = container.append<SVGGElement>('g').attr('class', 'node')

    let nodesEnter = nodesContainer
      .selectAll('g')
      .data<Node>(nodes, (n: Node) => n.channelId)
      .enter()
      .append('g')

    let nodesCircle = nodesEnter
      .append<SVGCircleElement>('circle')
      .attr('r', lay.getNodeRadius)
      .attr('fill', d => d.color)

    this.chart.addShapeEvents(nodesCircle, true)

    let labelsGroup = container
      .append<SVGGElement>('g')
      .attr('class', 'labels')
      .datum(nodes)
      .attr('pointer-events', 'none')

    let textRatio: [number, number] = null

    let labelPadding = 2
    let layoutLabels = layoutLabel<Node[]>(layoutGreedy())
      .size((n: any, i, g) => {
        const node = n as Node
        if (!textRatio) {

          // render a label to know the size to calclate with
          let e = g[i] as Element
          let text = e.getElementsByTagName('text')[0]
          text.style.display = 'inherit'
          let bbox = text.getBBox()
          textRatio = [bbox.width / node.label.length, bbox.height]
          text.style.display = null
        }

        return [node.label.length * textRatio[0], textRatio[1]]
      })
      .component(
        layoutTextLabel()
          .padding(labelPadding)
          .value((d: Node) => d.label)
      )

    function updateLabels(fast: boolean) {
      if (fast) {
        labelsGroup.selectAll<SVGGElement, Node>('g.label')
          .attr('transform', d => `translate(${d.x}, ${d.y})`)
      }
      else {
        labelsGroup.call(layoutLabels)
      }
    }

    this.renderLegend(svg)

    let updateVisibility = () => {

      this.chart.selectionHelper.updateSelectableCells(nodes)
      const zoomTrans = d3.zoomTransform(svg.node())
      let focused = nodes.filter(n => n.highlighted || n.selected)

      this.chart.addShapeClasses(nodesCircle)

      const labelText = labelsGroup.selectAll<SVGTextElement, Node>('text')
      this.chart.addShapeClasses(labelText)
      const related = (n: Node): boolean => focused.length <= 2 && focused.some(c => isConnected(n.channelId, c.channelId))
      nodesCircle.classed('related', related)
      labelText.classed('related', d => this.relatedLabelsVisible(zoomTrans) && related(d))
      linkEnter.classed('related', d => {
        let s = d.source as Node
        var t = d.target as Node
        return focused.some(n => s.channelId == n.channelId || t.channelId == n.channelId)
      })
    }

    function updatePositions() {
      nodesCircle.attr('cx', d => d.x).attr('cy', d => d.y) // faster than attr('transform', d => `translate(${d.x}, ${d.y})`)

      let fixna = (x?: number) => (x != null && isFinite(x) ? x : 0)
      linkEnter
        .attr('x1', d => fixna((d.source as Node).x))
        .attr('y1', d => fixna((d.source as Node).y))
        .attr('x2', d => fixna((d.target as Node).x))
        .attr('y2', d => fixna((d.target as Node).y))
    }


    // see here for good docs on d3 zooming https://www.datamake.io/blog/d3-zoom
    var zoomHandler = d3.zoom()
      .scaleExtent([0.2, 5])
      .on("zoom", () => {
        const t = d3.zoomTransform(svg.node())
        container.attr('transform', () => t.toString())

        const labelsTrans = `scale(${1 / t.k})`
        const existingTrans = labelsGroup.select('text').attr('transform')
        if (labelsTrans != existingTrans) {
          labelsGroup.selectAll<SVGTextElement, Node>('text')
            .attr('transform', () => labelsTrans) // undo the zoom on labels
          updateVisibility()
          updateLabels(false)
        }
      })

    let zoomToExpectedScale = (width: number, height: number) =>
      zoom(width, height, new DOMRect(0, -200, 1300, 1300), 0)

    let zoom = (width: number, height: number, bounds: DOMRect, duration: number) => {
      let midX = bounds.x + bounds.width / 2
      let midY = bounds.y + bounds.height / 2
      var scale = 1 / Math.max(bounds.width / width, bounds.height / height)
      var t = { x: width / 2 - scale * midX, y: height / 2 - scale * midY, scale: scale }

      let trans = d3.zoomIdentity.translate(t.x, t.y).scale(t.scale)
      let s = svg.transition().duration(duration)
      s.call(zoomHandler.transform, trans)
    }

    zoomHandler(svg)
    zoomToExpectedScale(this.props.width, this.props.height)


    this.stateRender = (prevProps: Props) => {
      let svg = d3.select(this.ref)

      if (prevProps == null || prevProps.width != this.props.width || prevProps.height != this.props.height) {
        svg.attr('width', this.props.width)
        svg.attr('height', this.props.height)

        zoomToExpectedScale(this.props.width, this.props.height)
      }

      updateVisibility()
      updatePositions()
    }

    for (var i = 0; i < 200; i++) lay.force.tick()
    this.stateRender(null, null)

    updateLabels(false)
  }

  private relatedLabelsVisible(zoomTrans: d3.ZoomTransform) {
    return zoomTrans.k > 1
  }

  private renderLegend(svg: d3.Selection<SVGSVGElement, unknown, null, undefined>) {
    let legendNodes = this.props.dataSet.channelDim.cells(this.props.dataSet.channels, {
      group: [this.state.colorCol], // if the group has a color, it should be colored
      order: { col: 'dailyViews', order: 'desc' }
    })

    this.chart.selectionHelper.updateSelectableCells(legendNodes)

    let legendGroup = svg.append<SVGGElement>('g')
      .attr('transform', () => `translate(5, 5)`)
      .attr('class', 'legend')
      .selectAll('g')
      .data<Cell<ChannelData>>(legendNodes)
      .enter()
      .append<SVGGElement>('g')
      .attr('transform', (d, i) => `translate(5, ${10 + (i * 21)})`)
    legendGroup.append<SVGCircleElement>("circle")
      .attr('r', 8)
      .attr('fill', d => d.color)
    legendGroup
      .append<SVGTextElement>('text')
      .text(d => d.label)
      .attr('x', 14).attr('y', 5)

    this.chart.addShapeEvents(legendGroup, false)
  }
}
