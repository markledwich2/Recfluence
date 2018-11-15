import * as React from 'react'
import * as d3 from 'd3'
import '../styles/Main.css'
import { layoutTextLabel, layoutGreedy, layoutLabel } from 'd3fc-label-layout'
import { Graph, YtNetworks, YtData } from '../ts/YtData'
import { ChartProps, DataSelections, DataComponentHelper, InteractiveDataState } from '../ts/Charts'
import * as _ from 'lodash'

interface State extends InteractiveDataState {}
interface Props extends ChartProps<YtData> {}
interface RelationSimLink extends d3.SimulationLinkDatum<ChannelSimNode> {
  strength: number
}

interface ChannelSimNode extends d3.SimulationNodeDatum {
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
    selections: new DataSelections()
  }

  componentDidMount() {
    this.loadChart()
  }

  componentDidUpdate(prevProps: Props, prevState: State) {
    this.stateRender()
  }

  render() {
    return <svg ref={ref => (this.ref = ref)} />
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
          } as ChannelSimNode)
      ).value()

    let links = _(this.props.dataSet.relations)
      .map(
        l =>
          ({
            source: l.FromChannelId,
            target: l.ChannelId,
            strength: +l.RecommendsPercent
          } as RelationSimLink)
      )
      .filter(
        l =>
          l.strength > 0.01 && (nodes.some(c => c.channelId == (l.source as string)) && nodes.some(c => c.channelId == (l.target as string)))
      ).value()

    let keyedNodes = nodes.filter(n => links.some(l => n.channelId == (l.source as string) || n.channelId == (l.target as string)))

    let adjlist = new Map()
    links.forEach(d => {
      adjlist.set(d.source + '-' + d.target, true)
      adjlist.set(d.target + '-' + d.source, true)
    })

    let isConnected = (a: string, b: string) => a == b || adjlist.get(a + '-' + b)

    return { nodes: keyedNodes, links:links, isConnected }
  }

  getLayout(nodes: ChannelSimNode[], links: RelationSimLink[]) {
    let w = this.props.width
    let h = this.props.height

    let maxStrength = d3.max(links, l => l.strength)
    let maxSize = d3.max(nodes, n => n.size)
    let getNodeRadius = (d: ChannelSimNode) => Math.sqrt(d.size > 0 ? d.size / maxSize : 1) * 30
    let getLineWidth = (d: RelationSimLink) => (d.strength / maxStrength) * 40
    let centerForce = d3.forceCenter()
    let force = d3
      .forceSimulation<ChannelSimNode, RelationSimLink>(nodes)
      .force('charge', d3.forceManyBody().strength(-100))
      .force('center', centerForce)
      .force(
        'link',
        d3
          .forceLink<ChannelSimNode, RelationSimLink>(links)
          .distance(1)
          .id(d => d.channelId)
          .strength(d => (d.strength / maxStrength) * 0.3)
      )
      .force('collide', d3.forceCollide<ChannelSimNode>(getNodeRadius))

    let onResize = () => {
      centerForce.x(this.props.width / 2)
      centerForce.y(this.props.height / 2)
    }
    onResize()

    return { force, getLineWidth, getNodeRadius, onResize }
  }

  async loadChart() {
    const { nodes, links, isConnected } = await this.getData()
    const lay = this.getLayout(nodes, links)

    let svg = d3.select(this.ref)
    let container = this.chart.createContainer(svg)

    let link = container
      .append('g')
      .attr('class', 'links')
      .selectAll('line')
      .data(links)
      .enter()
      .append('line')
      .attr('class', 'link')
      .attr('stroke-width', lay.getLineWidth)

    let node = container
      .append('g')
      .attr('class', 'nodes')
      .selectAll('g')
      .data(nodes)
      .enter()
      .append('g')
      .attr('class', 'node shape')
      .append('circle')
      .attr('class', 'shape')
      .attr('r', lay.getNodeRadius)
      .attr('fill', d => YtNetworks.lrColor(d.lr))

    this.chart.addDataShapeEvents(node, d => d.channelId, YtNetworks.ChannelIdPath)

    let labelPadding = 2
    let layoutLabels = layoutLabel<ChannelSimNode[]>(layoutGreedy())
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

    // label layout works at the group level, re-join to data
    let label = labelsGroup.selectAll('text').data(nodes)
    label.attr('pointer-events', 'none')

    let updateVisibility = () => {
      let lighted = this.chart.highlightedItems(YtNetworks.ChannelIdPath)
      let filtered = this.chart.filteredItems(YtNetworks.ChannelIdPath)
      let lightedFiltered = lighted.concat(filtered)

      let nodeLightedFiltered = (c: ChannelSimNode) =>
        lightedFiltered.some(id => id == c.channelId) || lightedFiltered.some(id => isConnected(id, c.channelId))

      node.style('opacity', d => (lightedFiltered.length == 0 || nodeLightedFiltered(d) ? 1 : 0.3))
      node.style('stroke', d => (filtered.some(id => id == d.channelId) ? '#ddd' : null))
      label.style('visibility', d => (nodeLightedFiltered(d) ? 'visible' : 'hidden'))

      link.style('opacity', d => {
        let s = d.source as ChannelSimNode
        var t = d.target as ChannelSimNode
        return lightedFiltered.some(id => s.channelId == id || t.channelId == id) ? 0.8 : 0
      })
    }

    function updatePositions(node: d3.Selection<d3.BaseType, ChannelSimNode, d3.BaseType, {}>, width: number, height: number) {
      var dx = (d: ChannelSimNode) => d.x //Math.max(lay.getNodeRadius(d), Math.min(width - lay.getNodeRadius(d), d.x))
      var dy = (d: ChannelSimNode) => d.y //Math.max(lay.getNodeRadius(d), Math.min(height - lay.getNodeRadius(d), d.y))

      node.attr('transform', d => {
        d.x = dx(d)
        d.y = dy(d)
        return `translate(${d.x}, ${d.y})`
      })

      let fixna = (x?: number) => (x != null && isFinite(x) ? x : 0)
      link
        .attr('x1', d => fixna((d.source as ChannelSimNode).x))
        .attr('y1', d => fixna((d.source as ChannelSimNode).y))
        .attr('x2', d => fixna((d.target as ChannelSimNode).x))
        .attr('y2', d => fixna((d.target as ChannelSimNode).y))
    }

    var zoom = d3
      .zoom()
      .scaleExtent([1, 8])
      .on('zoom', () => container.attr('transform', d3.event.transform))

    let zoomToFit = (width: number, height: number) => {
      var bounds = container
        .select<SVGGElement>('g.nodes')
        .node()
        .getBBox() // BBOX is the size of the container of drawn nodes
      let midX = bounds.x + bounds.width / 2
      let midY = bounds.y + bounds.height / 2
      var scale = 1 / Math.max(bounds.width / width, bounds.height / height)
      var translate = { w: width / 2 - scale * midX, h: height / 2 - scale * midY }

      container.transition().call(zoom.transform, d3.zoomIdentity.translate(translate.w, translate.h).scale(scale))
    }

    let tick = () => node.call(d => updatePositions(d, this.props.width, this.props.height))
    for (var i = 0; i < 50; i++) lay.force.tick()
    lay.force.on('tick', tick)

    this.stateRender = () => {
      let svg = d3.select(this.ref)
      svg.attr('width', this.props.width)
      svg.attr('height', this.props.height)

      labelsGroup.call(layoutLabels)
      lay.onResize()
      tick()
      zoomToFit(this.props.width, this.props.height)
      updateVisibility()
    }
    this.stateRender()
  }

  stateRender: () => void
}
