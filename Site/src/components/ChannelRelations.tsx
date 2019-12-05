import * as React from 'react'
import * as d3 from 'd3'
import '../styles/Main.css'
import { layoutTextLabel, layoutGreedy, layoutLabel } from 'd3fc-label-layout'
import { YtModel, ChannelData } from '../common/YtModel'
import { SelectableCell, DimQuery, Dim } from '../common/Dim'
import { YtInteractiveChartHelper } from "../common/YtInteractiveChartHelper"
import * as _ from 'lodash'
import { ChartProps, InteractiveDataState } from '../common/Chart'
import { YtTheme } from '../common/YtTheme'
import Select from 'react-select'
import { renderToString } from 'react-dom/server'
import { classNames, delay } from '../common/Utils'
import { range } from 'd3'

interface State extends InteractiveDataState { }
interface Props extends ChartProps<YtModel> { }
interface Link extends d3.SimulationLinkDatum<Node> {
  strength: number
  color: string
  impressions: number
}

interface Node extends d3.SimulationNodeDatum, SelectableCell<Node> {
  channelId: string
  size: number
  row: ChannelData
}

// interface ChannelData extends ChannelData {
//   fromChannelId?:string
//   toChannelId?:string
// }

export class ChannelRelations extends React.Component<Props, State> {
  svg: SVGSVGElement
  legendDiv: HTMLDivElement

  static source = 'relations'

  chart: YtInteractiveChartHelper = new YtInteractiveChartHelper(this, ChannelRelations.source)

  state: Readonly<State> = {
    selections: this.props.model.selectionState
  }

  componentDidMount() {
    this.loadChart()
  }

  componentDidUpdate(prevProps: Props, prevState: State) {
    this.stateRender(prevProps, prevState)
  }

  render() {
    return (
      <>
        <svg ref={ref => (this.svg = ref)} />
        {this.renderLegendHtml()}
      </>)
  }

  channelQuery(withColor: boolean): DimQuery<ChannelData> {
    return {
      group: ['channelId'],
      colorBy: withColor ? this.chart.selections.params().colorBy : null,
      labelBy: 'title'
    }
  }

  onColorBySelected = (option: { value: string }) => {
    this.chart.selections.setParam({ colorBy: option.value })
  }

  private renderLegendHtml(): JSX.Element {
    let colorBy = this.chart.selections.params().colorBy
    let legendNodes = this.props.model.channels.cells({
      group: [colorBy], // if the group has a color, it should be colored
      order: { col: 'dailyViews', order: 'desc' }
    }) as SelectableCell<ChannelData>[]

    let selections = this.chart.selections

    selections.updateSelectableCells(legendNodes)

    let dim = this.props.model.channels
    let options = YtModel.categoryCols.map(c => dim.col(c)).map(c => ({ value: c.name, label: c.label ?? c.name }))
    let selected = options.find(o => o.value == colorBy)
    return (
      <div className={'legend'}>
        <Select
          options={options}
          value={selected}
          styles={YtTheme.selectStyle}
          theme={YtTheme.selectTheme}
          onChange={this.onColorBySelected}
        ></Select>
        <svg height={legendNodes.length * 21} onClick={() => selections.clearAll()}>
          <g className={'chart legend'}>
            {legendNodes.map((d, i) => {
              const className = classNames({
                selected: d.selected,
                highlighted: d.highlighted,
                dimmed: d.dimmed
              })
              const events = ({
                click: (e: React.MouseEvent<Element, MouseEvent>) => {
                  e.stopPropagation()
                  return selections.select(d.keys)
                },
                in: (e: React.MouseEvent<Element, MouseEvent>) => selections.highlight(d.keys),
                out: (e: React.MouseEvent<Element, MouseEvent>) => selections.clearHighlight()
              })

              return (<g key={d.color} transform={`translate(10, ${10 + (i * 21)})`}
                onClick={events.click} onMouseEnter={events.in} onMouseLeave={events.out}>
                <circle r={8} fill={d.color} className={'node ' + className} ></circle>
                <text x={14} y={5} className={'label ' + className}>{d.label}</text>
              </g>)
            })}
          </g>
        </svg>
      </div>
    )
  }

  // a cheap render. Set by loadChat() so that it can use a bunch of context
  stateRender: (prevProps: Props, prevState: State) => void

  getData() {
    const channelCells = this.dim.rowCells(this.channelQuery(false))

    // channelCells.forEach(c => {
    //   c.cell.props.fromChannelId = c.row.channelId,
    //   c.cell.props.toChannelId = c.row.channelId
    // })

    let nodes: Node[] = _(channelCells)
      .filter(c => c.row.channelVideoViews > 0)
      .map(
        c =>
          ({
            channelId: c.row.channelId,
            size: c.row.channelVideoViews,
            row: c.row,
            ...c.cell
          } as Node)
      ).value()

    let links = _(this.props.model.recs.rows)
      .filter(l => l.toChannelId != l.fromChannelId && l.recommendsViewChannelPercent > 0.01)
      .map(
        l =>
          ({
            source: l.fromChannelId,
            target: l.toChannelId,
            strength: l.recommendsViewChannelPercent,
            impressions: l.relevantImpressionsDaily
          } as Link)
      )
      .filter(
        l => (nodes.some(c => c.channelId == (l.source as string)) && nodes.some(c => c.channelId == (l.target as string)))
      )
      .value()

    let keyedNodes = nodes.filter(n => links.some(l => n.channelId == (l.source as string) || n.channelId == (l.target as string)))

    let recMap = new Map()
    links.forEach(d => {
      recMap.set(d.source + '-' + d.target, true)
      recMap.set(d.target + '-' + d.source, true)
    })

    let isConnected = (a: string, b: string) => a == b || recMap.get(a + '-' + b)

    return { nodes: keyedNodes, links: links, isConnected }
  }

  private get dim() {
    return this.props.model.channels as unknown as Dim<ChannelData>
  }

  getLayout(nodes: Node[], links: Link[]) {
    let simSize = 1024
    let maxStrength = d3.max(links, l => l.strength)
    let maxImpressions = d3.max(links, l => l.impressions)
    let maxSize = d3.max(nodes, n => n.size)
    let getNodeRadius = (d: Node) => Math.sqrt(d.size > 0 ? d.size / maxSize : 1) * 40
    let getLineWidth = (d: Link) => Math.sqrt(d.impressions / maxImpressions) * 200
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
          .strength(d => (d.strength / maxStrength) * 0.3)
      )
      .force('collide', d3.forceCollide<Node>(getNodeRadius))

    return { force, getLineWidth, getNodeRadius, simSize, maxSize }
  }

  async loadChart() {
    const { nodes, links, isConnected } = this.getData()
    const lay = this.getLayout(nodes, links)

    let svg = d3.select(this.svg)
    let container = this.chart.createContainer(svg, ChannelRelations.source)

    let linkEnter = container
      .append('g')
      .attr('class', 'links')
      .selectAll('line')
      .data(links)
      .enter()
      .append<SVGLineElement>('line')
      .attr('class', 'link')
      .attr('stroke-width', lay.getLineWidth)

    let nodesContainer = container.append<SVGGElement>('g').attr('class', 'node')

    let nodesEnter = nodesContainer
      .selectAll('g')
      .data<Node>(nodes, (n: Node) => n.channelId)
      .enter()
      .append('g')

    let nodesCircle = nodesEnter
      .append<SVGCircleElement>('circle')
      .attr('r', lay.getNodeRadius)

    this.chart.addShapeEvents(nodesCircle)

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

          // render a label to know the size to calculate with
          let e = g[i] as Element
          let text = e.getElementsByTagName('text')[0]
          text.style.display = 'inherit'
          let box = text.getBBox()
          textRatio = [box.width / node.label.length, box.height]
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
          .attr('transform', d => `translate(${d.x + lay.getNodeRadius(d) + 3}, ${d.y - 10})`)
      }
      else {
        labelsGroup.call(layoutLabels)
      }
    }

    let updateColor = () => {
      let queryContext = this.dim.queryContext(this.channelQuery(true))
      nodes.forEach(c => c.color = queryContext.getColor(c.row))

      const nodeById = _.keyBy(nodes, n => n.channelId)
      links.forEach(l => l.color = nodeById[(l.source as Node).channelId].color)

      linkEnter.attr('stroke', (l: Link) => l.color)
    }

    let updateVisibility = () => {
      this.chart.selections.updateSelectableCells(nodes)

      let selectedOrHighlighted = nodes.filter(n => n.highlighted || n.selected)
      let selected = nodes.filter(n => n.selected)
      let highlighted = nodes.filter(n => n.highlighted)

      const related = (n: Node): boolean => 
        selectedOrHighlighted.length <= 2 && selectedOrHighlighted.some(c => isConnected(n.channelId, c.channelId))

      nodesCircle.classed('related', related)

      const labelText = labelsGroup.selectAll<SVGTextElement, Node>('text')
      labelText
        .style('display', d => {
          let zoomLabel = d3.zoomTransform(svg.node()).k > 2
          let display = selectedOrHighlighted.length > 2 ?
            (d.selected && (zoomLabel || selected.length < 2)) || (d.highlighted && (zoomLabel || highlighted.length < 2)) // many selected -  only show labels when zoomed
            : (d.selected || d.highlighted) || related(d) && zoomLabel// few selected - show labels of selected/highlighted, and also related when zoomed

          return display ? 'inherit' : 'none'
        })

      linkEnter.classed('related', d => {
        let s = d.source as Node
        var t = d.target as Node
        return selectedOrHighlighted.some(n => s.channelId == n.channelId || t.channelId == n.channelId)
      })

      linkEnter.style('opacity', selectedOrHighlighted.length > 2 ? 0.1 : 0.3)
    }

    const chart = this.chart;
    const updateEffects = () => {
      let zoom = d3.zoomTransform(svg.node())
      return chart.updateShapeEffects(nodesCircle, { unselectedGlow: d => d.size  > lay.maxSize * 0.1 * zoom.k})
    }

    function updatePositions() {
      nodesCircle.attr('cx', d => d.x).attr('cy', d => d.y) // faster than attr('transform', d => `translate(${d.x}, ${d.y})`)

      let safeLoc = (x?: number) => (x != null && isFinite(x) ? x : 0)
      linkEnter
        .attr('x1', d => safeLoc((d.source as Node).x))
        .attr('y1', d => safeLoc((d.source as Node).y))
        .attr('x2', d => safeLoc((d.target as Node).x))
        .attr('y2', d => safeLoc((d.target as Node).y))
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
          updateLabels(true)
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
      console.log("zoomed")
    }

    zoomHandler(svg)
    zoomToExpectedScale(this.props.width, this.props.height)

    this.stateRender = (prevProps: Props, prevState: State) => {
      let svg = d3.select(this.svg)

      if (prevProps == null || prevProps.width != this.props.width || prevProps.height != this.props.height) {
        svg.attr('width', this.props.width)
        svg.attr('height', this.props.height)

        zoomToExpectedScale(this.props.width, this.props.height)
        console.log("state rendered - size change")
      }

      updateVisibility()
      updateColor()
      updateEffects()
      console.log("state rendered")
    }

    this.stateRender(null, null)
    for (var i = 0; i < 150; i++) {
      lay.force.tick()
      updatePositions()
      await delay(5)
    }
    lay.force.stop()

    updatePositions()
    this.stateRender(null, null)
    updateLabels(false)
  }
}
