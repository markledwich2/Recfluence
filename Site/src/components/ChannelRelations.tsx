import * as React from 'react'
import * as d3 from 'd3'
import '../styles/Main.css'
import { layoutTextLabel, layoutGreedy, layoutLabel, layoutRemoveOverlaps } from 'd3fc-label-layout'
import { ChartProps, ChannelSimDatum, RecommendedSimDatum, YtNetworks } from '../ts/YouTubeNetworks'
import { max } from '../ts/Utils'

export class ChannelRelations extends React.Component<ChartProps, {}> {
  ref: SVGSVGElement
  onResize: () => void

  componentDidMount() {
    this.loadChart()
  }

  componentDidUpdate() {
    this.onResize()
  }

  render() {
    return (
      <svg
        width={this.props.width}
        height={this.props.height}
        className="container"
        ref={(ref: SVGSVGElement) => (this.ref = ref)}
      />
    )
  }

  async loadChart() {
    let pallet = ['#283044', '#78A1BB', '#8AA399', '#BFA89E', '#E6C79C']
    let lrPallet = YtNetworks.lrPallet();
    let colorScale = d3.scaleOrdinal(pallet)
    let linkOpacity = 0,
      nodeOpacity = 1

    function radius(d: ChannelSimDatum) {
      return Math.sqrt(d.size > 0 ? (d.size / maxSize) * 1800 : 1)
    }

    function neigh(a: string, b: string): boolean {
      return a == b || adjlist.get(a + '-' + b)
    }

    function focus(d: ChannelSimDatum) {
      var id = d.channelId
      node.style('opacity', d => (neigh(id, d.channelId) ? nodeOpacity : 0.3))
      link.style('opacity', d => {
        let s = d.source as ChannelSimDatum
        var t = d.target as ChannelSimDatum
        return s.channelId == id || t.channelId == id ? 0.8 : linkOpacity
      })
      label.style('visibility', d => (neigh(id, d.channelId) ? 'visible' : 'hidden'))
      updateLabels()
    }

    function updateNodeAndLinks(
      node: d3.Selection<d3.BaseType, ChannelSimDatum, d3.BaseType, {}>,
      width: number,
      height: number
    ) {
      var dx = (d: ChannelSimDatum) => Math.max(radius(d), Math.min(width - radius(d), d.x))
      var dy = (d: ChannelSimDatum) => Math.max(radius(d), Math.min(height - radius(d), d.y))

      //node.attr("transform", d => `translate(${d.x}, ${d.y})`);

      node.attr('transform', d => {
        d.x = dx(d)
        d.y = dy(d)
        return `translate(${d.x}, ${d.y})`
      })

      let fixna = (x?: number) => (x != null && isFinite(x) ? x : 0)
      link
        .attr('x1', d => fixna((d.source as ChannelSimDatum).x))
        .attr('y1', d => fixna((d.source as ChannelSimDatum).y))
        .attr('x2', d => fixna((d.target as ChannelSimDatum).x))
        .attr('y2', d => fixna((d.target as ChannelSimDatum).y))
    }

    let data = await YtNetworks.simData(this.props.dataPath)
    let maxStrength = max(data.links, l => l.strength)
    let maxSize = max([...data.nodes], n => n.size)

    let centerForce = d3.forceCenter(this.props.width / 2, this.props.height / 2)
    let force = d3
      .forceSimulation<ChannelSimDatum, RecommendedSimDatum>(data.nodes)
      .force('charge', d3.forceManyBody().strength(-120))
      .force('center', centerForce)
      .force(
        'link',
        d3
          .forceLink<ChannelSimDatum, RecommendedSimDatum>(data.links)
          .distance(1)
          .id(d => d.channelId)
          .strength(d => (d.strength / maxStrength) * 0.2)
      )
      .force('collide', d3.forceCollide<ChannelSimDatum>(d => radius(d)))

    let adjlist = new Map()
    data.links.forEach(d => {
      let s = d.source as ChannelSimDatum
      var t = d.target as ChannelSimDatum
      adjlist.set(s.channelId + '-' + t.channelId, true)
      adjlist.set(t.channelId + '-' + s.channelId, true)
    })

    let svg = d3.select(this.ref)
    let container = svg.append('g')

    let labelPadding = 2
    let layoutLabels = layoutLabel<ChannelSimDatum[]>(layoutGreedy())
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

    let labelsGroup = svg
      .append('g')
      .attr('class', 'labels')
      .datum(data.nodes)
      .call(layoutLabels)

    // label layout works at the group level, re-join to data
    let label = labelsGroup.selectAll('text').data(data.nodes)
    label.attr("pointer-events", "none")
    
    let link = container
      .append('g')
      .attr('class', 'links')
      .selectAll('line')
      .data(data.links)
      .enter()
      .append('line')
      .attr('class', 'link')
      .attr('stroke-width', d => (d.strength / maxStrength) * 50)

    let node = container
      .append('g')
      .attr('class', 'nodes')
      .selectAll('g')
      .data(data.nodes)
      .enter()
      .append('g')
      .attr('class', 'node')

    node
      .append('circle')
      .attr('r', d => radius(d))
      .attr('fill', d => lrPallet[d.lr])

    function updateVisibility() {
      node.style('opacity', nodeOpacity)
      label.style('visibility', 'hidden')
      link.style('opacity', linkOpacity)
      label.style('visibility', d => 'hidden') //(radius(d) > 10 ? 'visible' : 'hidden')
    }
    node.on('mouseover', focus).on('mouseout', updateVisibility)
    
    //label.on('mouseover', focus).on('mouseout', updateVisibility)

    function updateLabels() {
      labelsGroup.call(layoutLabels)
    }

    function tick(width: number, height: number) {
      node.call(d => updateNodeAndLinks(d, width, height))
    }

    for (var i = 0; i < 20; i++) force.tick()
    force.on("tick", () => tick(this.props.width, this.props.height));

    updateVisibility()

    this.onResize = () => {
      tick(this.props.width, this.props.height)
      labelsGroup.call(layoutLabels)
      centerForce.x(this.props.width / 2)
      centerForce.y(this.props.height / 2)
      force.restart()
    }

    tick(this.props.width, this.props.height)
  }
}
