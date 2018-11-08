import * as React from 'react'
import * as d3 from 'd3'
import { sankey, sankeyLinkHorizontal, sankeyLeft } from 'd3-sankey'
import '../styles/Main.css'
import { YtNetworks, ChannelSkExtra, RelationSkExtra, ChannelSkNode, RelationSkLink, YtData } from '../ts/YtData'
import { ChartProps, ChartState,  DataSelections, Chart } from '../ts/Charts'
import { jsonEquals } from '../ts/Utils';
import clone from 'clone'

interface State extends ChartState {}
interface Props extends ChartProps<YtData> {}

export class RecommendFlows extends React.Component<Props, State> {
  ref: SVGSVGElement
  
  chart:Chart = new Chart(this)
  state: Readonly<State> = {
    selections: new DataSelections()
  }

  dataRender: (prevState:State) => void

  componentDidMount() {
    this.createChart()
  }

  componentDidUpdate(prevProps:ChartProps<YtData>, prevState:State) {
    this.dataRender(prevState)
  }

  render() {
    return (
      <svg width={this.props.width} height={this.props.height} className="container" ref={(ref: SVGSVGElement) => (this.ref = ref)}>
        {/* SVG contents */}
      </svg>
    )
  }

  renderedSelections:DataSelections

  layoutData() {
    let { nodes, links } = YtNetworks.sankeyData(this.props.dataSet)
    let channelId = this.chart.filteredItems(YtNetworks.ChannelIdPath).find(() => true)
    if (!channelId) return { nodes: [], links: [] }

    // sankey is not bi-directional, so clone channels with a new id to represent input channels.
    let inChannels = nodes.filter(c => links.some(r => r.target == c.channelId)).map(c => ({ ...c, channelId: 'in.' + c.channelId }))
    let inLinks = links.filter(r => r.target == channelId).map(r => ({ ...r, source: 'in.' + r.source }))
    let outChannels = nodes.filter(c => links.some(r => r.source == c.channelId)).map(c => ({ ...c, channelId: 'out.' + c.channelId }))
    let outLinks = links.filter(r => r.source == channelId).map(r => ({ ...r, target: 'out.' + r.target }))
    let channels = inChannels.concat(outChannels).concat(nodes.find(c => c.channelId == channelId))
    let finalLinks = inLinks.concat(outLinks).sort((a, b) => d3.descending(a.value, b.value))
    finalLinks = finalLinks
      .filter(l => channels.some(c => c.channelId == l.target) && channels.some(c => c.channelId == l.source))
      .filter((c, i) => i < 20)
    channels = channels.filter(c => finalLinks.some(l => l.source == c.channelId || l.target == c.channelId))

    return { nodes: channels, links: finalLinks }
  }

  async createChart() {
    let lrPallet = YtNetworks.lrPallet()
    let width = this.props.width
    let height = this.props.height

    let svg = d3.select(this.ref)
    let container = this.chart.createContainer(svg)

    let linkG = container.append('g').attr('class', 'links')

    let nodeG = container.append('g').attr('class', 'nodes')

    this.dataRender = () => {
      if(this.renderedSelections != null && jsonEquals(this.renderedSelections.filters, this.state.selections.filters))
        return;
      this.renderedSelections = clone(this.state.selections)

      let { nodes, links } = this.layoutData();
      let layout = sankey<ChannelSkExtra, RelationSkExtra>()
        .nodeWidth(36)
        .nodePadding(40)
        .size([width, height])
        .nodeAlign(sankeyLeft)
        .nodeId(d => d.channelId);
      let graph = layout({ nodes, links });
      let updateLink = linkG.selectAll('path').data(graph.links, (l: RelationSkLink) => l.id);
      let enterLink = updateLink.enter().append('path')
        .attr('class', 'link');
      updateLink.merge(enterLink)
        .attr('d', sankeyLinkHorizontal())
        .attr('stroke-width', d => d.width);


      updateLink.exit().remove();
      
      let updateNode = nodeG.selectAll('g').data(graph.nodes, (n: ChannelSkNode) => n.channelId);
      let enterNode = updateNode.enter().append('g');
      enterNode.append('rect')
        .attr('class', 'selectable')
        .attr('x', d => d.x0)
        .attr('y', d => d.y0)
        .attr('width', d => d.x1 - d.x0)
        .attr('height', d => Math.max(d.y1 - d.y0, 1));

      enterNode.append('text').text(d => d.title);
      this.chart.addDataShapeEvents(enterNode.select('rect'), n => n.channelId, YtNetworks.ChannelIdPath);


      let mergeNode = updateNode.merge(enterNode);
      mergeNode.select('rect')
        .transition().duration(300)
        .attr('x', d => d.x0)
        .attr('y', d => d.y0)
        .attr('height', d => Math.max(d.y1 - d.y0, 1))
        .attr('width', d => d.x1 - d.x0)
        .attr('fill', d => lrPallet[d.lr]);
      let isLeft = (d: ChannelSkNode) => d.x0 < width / 2;
      mergeNode.select('text')
        .style('opacity', 0)
        .attr('x', d => isLeft(d) ? d.x1 + 10 : d.x0 - 10)
        .attr('y', d => (d.y1 + d.y0) / 2)
        .attr('dy', '0.35em')
        .attr('text-anchor', d => isLeft(d) ? 'start' : 'end')
        .transition().delay(200).duration(200)
        .style('opacity', 1);
      updateNode
        .exit()
        .transition()
        .duration(500)
        .style('fill-opacity', 1e-6)
        .remove();
    }
  }
}
