import * as React from 'react'
import { InteractiveDataProps, InteractiveDataState, DataComponentHelper, DataSelections, ChartProps } from '../common/Charts'
import { YtNetworks, RelatiosData } from '../common/YtData'
import _ from 'lodash'
import * as d3 from 'd3'
import cloud from 'd3-cloud'
import gen from 'random-seed'

interface State extends InteractiveDataState { }
interface Props extends ChartProps<RelatiosData> { }

export class ChannelWords extends React.Component<Props, State> {
  ref: SVGElement
  chart: DataComponentHelper = new DataComponentHelper(this)
  state: Readonly<State> = {
    selections: this.props.initialSelection
  }

  dataRender: (words: WordElement[]) => void

  channel() {
    let channelId = this.chart.highlightedItems(YtNetworks.ChannelIdPath).find(() => true)
    if (!channelId) channelId = this.chart.filteredItems(YtNetworks.ChannelIdPath).find(() => true)
    return channelId ? this.props.dataSet.channels[channelId] : null
  }

  render() {
    return <svg ref={(ref: SVGElement) => (this.ref = ref)}>{/* SVG contents */}</svg>
  }

  componentDidMount() {
    let svg = d3.select(this.ref)
    svg.append("g")
  }

  componentDidUpdate(prevProps: ChartProps<RelatiosData>, prevState: State) {
    this.reloadChart()
  }

  async reloadChart() {
    let w = this.props.width
    let h = this.props.height
    let c = this.channel()

    if (!c) {
      return
    }

    
    let words = c.Words == null ?  [] : c.Words.map(w => ({ text: w.Word, size: w.TfIdf, count:w.Count } as WordElement))
    let wordSize = _(words.map(w => w.size))
    let wordCount = _(words.map(w => w.count))
    let sizeRange = wordSize.max() - wordSize.min()

    var fontScale = d3.scaleLinear() // scale algo which is used to map the domain to the range
      .domain([wordSize.min(), wordSize.max()]) //set domain which will be mapped to the range values
      .range([20, 100]); // set min/max font size (so no matter what the size of the highest word it will be set to 40 in this case)

    var colorScale = d3.scaleLinear().domain([wordCount.min(), wordCount.max()]).range([1, 0.2])

    let svg = d3.select(this.ref)
    let generator = this.generator

    this.dataRender = (elements: WordElement[]) => {
      svg
        .attr("width", w).attr("height", h)
        .attr("class", "wordcloud")

      var nodes = svg
        .selectAll("g")
        .attr("transform", `translate(${w / 2},${h / 2})`)
        .selectAll("text")
        .data(elements, (w:WordElement) => w.text)

      let enter = nodes
        .enter().append("text")
        .style("font-family", "Segoe UI Black")
        .attr("text-anchor", "middle")
        .text(function (d) { return d.text; })

      nodes.merge(enter)
        .style("font-size", d => `${d.size}px`)
        .style("fill", d => '#EEE' /*d3.interpolateGreys(colorScale(d.count))*/)
        .attr("transform", d => `translate(${[d.x, d.y]}) rotate(${d.rotate})`)

      nodes.exit().remove()
    }

    let rand = gen.create("abc123")

    cloud()
      .random(() => rand.random())
      .size([w, h])
      .words(words)
      .padding(5)
      .font("Segoe UI Black")
      .rotate(0)
      .fontSize(d => fontScale(d.size))
      .on("end", this.dataRender)
      .start();
  }
}

interface WordElement {
  text: string
  size: number
  count: number
  x?: number
  y?: number
  rotate?: number
}
