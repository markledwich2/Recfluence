import * as React from 'react'
import * as d3 from 'd3'
import '../styles/Main.css'
import { layoutTextLabel, layoutGreedy, layoutLabel } from 'd3fc-label-layout'
import { Graph, YtNetworks, YtData } from '../common/YtData'
import { delay } from '../common/Utils'
import { ChartProps, DataSelections, DataComponentHelper, InteractiveDataState } from '../common/Charts'
import * as _ from 'lodash'
import { lab } from 'd3'
import { Properties } from 'csstype'

interface State extends InteractiveDataState {}
interface Props extends ChartProps<YtData> {}
interface Link extends d3.SimulationLinkDatum<Node> {
  strength: number
}

interface Node extends d3.SimulationNodeDatum {
  channelId: string
  size: number
  type: string
  shapeId: string
  lr: string
  title: string
}

export class ChannelRelations extends React.Component<Props, State> {
  ref: SVGSVGElement

  chart: DataComponentHelper = new DataComponentHelper(this)

  state: Readonly<State> = {
    selections: this.props.initialSelection
  }

  componentDidMount() {
    this.loadChart()
  }

  componentDidUpdate(prevProps: Props, prevState: State) {
    this.stateRender(prevProps)
  }

  render() {
    let lrItems = _(Array.from(YtNetworks.lrMap.entries()))
      .filter(lr => lr[0] != '')
      .value()
    return (
      <>
        <div style={{ position: 'absolute' }}>
          <ul className={'legend'}>
            {lrItems.map(l => (
              <li style={{ color: l[1].color }} key={l[0]}>
                <span className={'text'}>{l[1].text}</span>
              </li>
            ))}
          </ul>
        </div>
        <svg ref={ref => (this.ref = ref)} />
      </>
    )
  }

  getData() {
    let nodes = _(this.props.dataSet.channels)
      .filter(c => c.ChannelVideoViews > 0)
      .map(
        c =>
          ({
            channelId: c.ChannelId,
            title: c.Title,
            size: +c.ChannelVideoViews,
            type: c.Type,
            lr: c.LR
          } as Node)
      )
      .value()

    let links = _(this.props.dataSet.relations)
      .map(
        l =>
          ({
            source: l.FromChannelId,
            target: l.ChannelId,
            strength: +l.RecommendsFlowPercent
          } as Link)
      )
      .filter(
        l =>
          l.strength > 0.01 &&
          (nodes.some(c => c.channelId == (l.source as string)) && nodes.some(c => c.channelId == (l.target as string)))
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

  getLayout(nodes: Node[], links: Link[]) {
    let simSize = 1024
    let maxStrength = d3.max(links, l => l.strength)
    let maxSize = d3.max(nodes, n => n.size)
    let getNodeRadius = (d: Node) => Math.sqrt(d.size > 0 ? d.size / maxSize : 1) * 30
    let getLineWidth = (d: Link) => (d.strength / maxStrength) * 40
    let centerForce = d3.forceCenter(simSize / 2, simSize / 2)
    let force = d3
      .forceSimulation<Node, Link>(nodes)
      .force('charge', d3.forceManyBody().strength(-100))
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

    return { force, getLineWidth, getNodeRadius, simSize }
  }

  async loadChart() {
    const { nodes, links, isConnected } = await this.getData()
    const lay = this.getLayout(nodes, links)

    let svg = d3.select(this.ref)
    let container = this.chart.createContainer(svg)

    let linkEnter = container
      .append('g')
      .attr('class', 'links')
      .selectAll('line')
      .data(links)
      .enter()
      .append<SVGLineElement>('line')
      .attr('class', 'link')
      .attr('stroke-width', lay.getLineWidth)

    let nodesContainer = container.append<SVGGElement>('g').attr('class', 'nodes')

    let nodesEnter = nodesContainer
      .selectAll('g')
      .data(nodes, (n: Node) => n.channelId)
      .enter()
      .append('g')

    let nodesCircle = nodesEnter
      .append<SVGCircleElement>('circle')
      .attr('class', 'shape')
      .attr('r', lay.getNodeRadius)
      .attr('fill', d => YtNetworks.lrColor(d.lr))

    this.chart.addDataShapeEvents(nodesCircle, d => d.channelId, YtNetworks.ChannelIdPath)

    let labelPadding = 2
    let layoutLabels = layoutLabel<Node[]>(layoutGreedy())
      .size((_, i, g) => {
        let e = g[i] as Element
        let textSize = e.getElementsByTagName('text')[0].getBBox()
        return [textSize.width + labelPadding * 2, textSize.height + labelPadding * 2]
      })
      .component(
        layoutTextLabel()
          .padding(labelPadding)
          .value(d => d.title)
      )

    let labelsGroup = container
      .append('g')
      .attr('class', 'labels')
      .datum(nodes)
      .call(layoutLabels)

    function updateLabels(fast:boolean) {
      if(fast) {
        labelsGroup.selectAll<SVGGElement, Node>("g.label")
          .attr('transform', d => `translate(${d.x}, ${d.y})`)
      }
      else
        labelsGroup.call(layoutLabels)
    }

    let labels = labelsGroup.selectAll<SVGTextElement, Node>('text')
    labels.attr('pointer-events', 'none')

    let updateVisibility = () => {
      let lighted = this.chart.highlightedItems(YtNetworks.ChannelIdPath)
      let filtered = this.chart.filteredItems(YtNetworks.ChannelIdPath)
      let lightedFiltered = lighted.concat(filtered)

      let nodeFiltered = (c:Node) => filtered.some(id => id == c.channelId)
      let nodeLightedFiltered = (c: Node) => lightedFiltered.some(id => id == c.channelId)
      let nodeRelated = (c: Node) => lightedFiltered.some(id => isConnected(id, c.channelId))

      let showRelatedLabels = this.props.width > 1024

      nodesCircle.style('opacity', d => (lightedFiltered.length == 0 || nodeLightedFiltered(d) || nodeRelated(d) ? 1 : 0.4))
      nodesCircle.style('stroke', d => nodeFiltered(d) ? '#ddd' : null)
      labels.style('display', d => nodeLightedFiltered(d) || (showRelatedLabels && nodeRelated(d)) ?  null :'none')
      //labels.style('fill', d =>  nodeLightedFiltered(d) ? null : '#aaa')
      labels.style('font-weight', d =>  nodeLightedFiltered(d) ? 'bold' : null)

      linkEnter.style('opacity', d => {
        let s = d.source as Node
        var t = d.target as Node
        return lightedFiltered.some(id => s.channelId == id || t.channelId == id) ? 0.4 : 0
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

    let zoomToExpectedScale = (width: number, height: number) => zoom(width, height, new DOMRect(0, 0, 1000, 1000), 0)
    let zoomToFit = (width: number, height: number) => zoom(width, height, nodesContainer.node().getBBox(), 1000)

    let zoom = (width: number, height: number, bounds: DOMRect, duration: number) => {
      let midX = bounds.x + bounds.width / 2
      let midY = bounds.y + bounds.height / 2
      var scale = 1 / Math.max(bounds.width / width, bounds.height / height)
      var t = { x: width / 2 - scale * midX, y: height / 2 - scale * midY, scale: scale }

      let trans = d3.zoomIdentity.translate(t.x, t.y).scale(t.scale)
      let s = duration > 0 ? container.transition().duration(duration) : container
      s.attr('transform', () => trans.toString())
      labels.attr('transform', d => `scale(${1/trans.k})`) // undo the zoom on labels
    }

    let ticks = 0
    let stopped = false
    let timesResized = 0

    this.stateRender  = (prevProps: Props) => {
      let svg = d3.select(this.ref)

      if (prevProps == null || prevProps.width != this.props.width || prevProps.height != this.props.height) {
        svg.attr('width', this.props.width)
        svg.attr('height', this.props.height)
        //if(timesResized == 0)
          zoomToExpectedScale(this.props.width, this.props.height)
        //if(stopped)
        //  zoomToFit(this.props.width, this.props.height) // first zoom should be to the expected bounds, nt the current ones
        updateLabels(false)
        timesResized++
      }
      updateVisibility()
    }

    let onTick = () => {
      updatePositions()
      updateLabels(true)
      ticks++
      if (ticks > 150) {
        lay.force.stop()
        stopped = true
        this.stateRender(null)
      }
    }

    //for (var i = 0; i < 10; i++) lay.force.tick()
    lay.force.on('tick', onTick)
    this.stateRender(null)
  }

  stateRender: (prevProps: Props) => void
}
