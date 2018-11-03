import * as React from "react";
import * as d3 from "d3";
import "../styles/ChannelRelations.css";
import { layoutTextLabel, layoutGreedy,  layoutLabel, layoutRemoveOverlaps } from 'd3fc-label-layout';

interface Props {
  dataPath: string
  width:number
  height:number
}

interface Recommended extends d3.SimulationLinkDatum<Channel> {
  source: string | Channel
  target: string | Channel
  strength: number
}

interface Channel extends d3.SimulationNodeDatum {
  id: string
  title: string
  size: number
  type: string
  lr: string
}

export class ChannelRelations extends React.Component<Props, {}> {
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
      <svg width={this.props.width} height={this.props.height}  
      className="container"
      ref={(ref: SVGSVGElement) => (this.ref = ref)}>
          {/* SVG contents */}
      </svg>
    );
  }

  width() { return this.props.width }
  height() { return this.props.height } 

  async loadChart() {
    let channels = await d3.csv(this.props.dataPath + "Channels.csv")
    let recommends = await d3.csv(this.props.dataPath + "ChannelRelations.csv")
    let pallet = ["#283044", "#78A1BB", "#8AA399", "#BFA89E", "#E6C79C"]
    let colorScale = d3.scaleOrdinal(pallet)
    let linkOpacity = 0, nodeOpacity = 0.6

    function RowToRecommended(row: any): Recommended {
      return {
        source: row.FromChannelId,
        target: row.ChannelId,
        strength: row.RecommendsPerVideo
      };
    }

    function RowToChannel(row: any): Channel {
      return {
        id: row.Id,
        title: row.Title,
        size: row.ChannelVideoViews,
        type: row.Type,
        lr: row.LR
      };
    }

    function maxIn<T>(list: Array<T>, getValue: (item: T) => number): number {
      return list.reduce((max, n) => (getValue(n) > max ? getValue(n) : max), getValue(list[0]))
    }

    function radius(d: Channel) {
      return Math.sqrt(d.size > 0 ? (d.size / maxSize) * 200 : 1)
    }

    function neigh(a: string, b: string) {
      return a == b || adjlist.get(a + "-" + b)
    }

    function focus(d: Channel) {
      var id = d.id;
      node.style("opacity", d => neigh(id, d.id) ? nodeOpacity : 0.1)
      link.style("opacity", d => {
        let s = d.source as Channel
        var t = d.target as Channel
        return s.id == id || t.id == id ? 0.8 : linkOpacity
      });
      label.style("visibility", d => neigh(id, d.id) ? "visible" : "hidden")
    }

    function updateVisibility() {
      node.style("opacity", nodeOpacity)
      label.style("visibility", "hidden")
      link.style("opacity", linkOpacity)

      label.style("visibility", d => radius(d) > 10 ? "visible" : "hidden")
    }

    function fixna(x?: number): number {
      return x != null && isFinite(x) ? x : 0;
    }

    function updateNodeAndLinks(node:d3.Selection<d3.BaseType, Channel, d3.BaseType, {}>, width:number, height:number) {
      var dx = (d:Channel) => Math.max(radius(d), Math.min(width- radius(d), d.x))
      var dy = (d:Channel) => Math.max(radius(d), Math.min(height - radius(d), d.y))
      
      //node.attr("transform", d => `translate(${d.x}, ${d.y})`);

      node.attr("transform", d =>  {
        d.x = dx(d)
        d.y = dy(d)
        return `translate(${d.x}, ${d.y})`
      })

      link
        .attr("x1", d => fixna((d.source as Channel).x))
        .attr("y1", d => fixna((d.source as Channel).y))
        .attr("x2", d => fixna((d.target as Channel).x))
        .attr("y2", d => fixna((d.target as Channel).y))
    }

    const links = recommends.map(RowToRecommended)
    const nodes = channels.map(RowToChannel)
    let maxStrength = maxIn(links, l => l.strength)
    let maxSize = maxIn(nodes, n => n.size)

    let centerForce = d3.forceCenter(this.props.width / 2, this.props.height / 2)
    let force = d3
      .forceSimulation<Channel,Recommended>(nodes)
      .force("charge", d3.forceManyBody().strength(-80))
      .force("center", centerForce)
      .force(
        "link",
        d3
          .forceLink<Channel, Recommended>(links)
          .distance(1)
          .id(d => d.id)
          .strength(d => (d.strength / maxStrength) * 0.2)
      )
      .force("collide", d3.forceCollide<Channel>(d => radius(d)))

    let adjlist = new Map()
    links.forEach(d => {
      let s = d.source as Channel
      var t = d.target as Channel
      adjlist.set(s.id + "-" + t.id, true)
      adjlist.set(t.id + "-" + s.id, true)
    });
    
    const svg = d3.select(this.ref)
    let container = svg.append("g")

    const labelPadding = 2
    const layoutLabels = layoutLabel<Channel[]>(layoutGreedy())
      .size((_, i, g) => {
          let e = g[i] as Element
          const textSize = e.getElementsByTagName('text')[0].getBBox()
          return [textSize.width + labelPadding * 2, textSize.height + labelPadding * 2]
      })
      .component(layoutTextLabel()
        .padding(labelPadding)
        .value((d) => d.title))
    
    var labelsGroup = svg.append('g')
      .attr("class", "labels")
      .datum(nodes)
      .call(layoutLabels)

    // label layout works at the group level, re-join to data
    var label = labelsGroup.selectAll("text")
      .data(nodes)

    var link = container
      .append("g")
        .attr("class", "links")
      .selectAll("line").data(links)
        .enter()
        .append("line")
          .attr("class", "link")
          .attr("stroke-width", d => (d.strength / maxStrength) * 50)

    var node = container
      .append("g")
      .attr("class", "nodes")
      .selectAll("g")
      .data(nodes)
      .enter()
      .append("g")
      .attr("class", "node")

    node
      .append("circle")
      .attr("r", d => radius(d))
      .attr("fill", d => colorScale(d.type))

    node.on("mouseover", focus).on("mouseout", updateVisibility)
    label.on("mouseover", focus).on("mouseout", updateVisibility)

    function tick(width:number,height:number) {
      node.call(d => updateNodeAndLinks(d, width, height))
    }

    for (var i = 0; i < 100; i++) force.tick()
    //force.on("tick", () => tick(this.props.width, this.props.height))
    labelsGroup.call(layoutLabels)
    updateVisibility()

    this.onResize = () => {
      console.log(`resizing ${this.props.width}, ${this.props.height} `)

      tick(this.props.width, this.props.height);
      labelsGroup.call(layoutLabels)
      centerForce.x(this.props.width /2)
      centerForce.y(this.props.height /2)
      force.restart()
    }

    tick(this.props.width, this.props.height);
  }
}
