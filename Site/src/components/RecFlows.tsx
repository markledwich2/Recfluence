import * as React from 'react'
import { renderToString } from 'react-dom/server'
import * as d3 from 'd3'
import { sankey, sankeyLinkHorizontal, sankeyLeft, SankeyNode, SankeyLink } from 'd3-sankey'
import '../styles/Main.css'
import { YtModel, Graph, ChannelData, RecEx, RecDir, RecData } from '../common/YtModel'
import { YtInteractiveChartHelper } from "../common/YtInteractiveChartHelper"
import { compactInteger } from 'humanize-plus'
import * as _ from 'lodash'
import { ChartProps, InteractiveDataState } from '../common/Chart'
import { SelectableCell, ColEx, Cell, CellEx, DimQuery, Dim } from '../common/Dim'
import { typedKeys, merge, toRecord } from '../common/Utils'

interface State extends InteractiveDataState { }
interface Props extends ChartProps<YtModel> { }

interface RecFlowExtra {
  id: string
}

interface NodeExtra extends SelectableCell<RecData> {
  shapeId: string
  mode?: NodeMode
  incoming?: number
  outgoing?: number
}

type Node = SankeyNode<NodeExtra, RecFlowExtra>
type Link = SankeyLink<NodeExtra, RecFlowExtra>

type NodeMode =
  'left' | // from the center (i.e right hand side)
  'right' | // to the center (i.e. left hand side)
  'center'

export class RecFlows extends React.Component<Props, State> {
  ref: SVGSVGElement

  chart: YtInteractiveChartHelper = new YtInteractiveChartHelper(this, RecFlows.source)
  state: Readonly<State> = {
    selections: this.props.model.selectionState
  }

  static source = 'flow'

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

  betweenColorLayout(): Graph<Node[], Link[]> {
    const colorBy = this.chart.selections.params().colorBy
    const fromCol = RecEx.recCol('from', colorBy)
    const toCol = RecEx.recCol('to', colorBy)
    const from = (r: RecData) => r[fromCol]
    const to = (r: RecData) => r[toCol]

    const recs = this.props.model.recCats
    const recRows = recs.rows.filter(r => r[toCol] && r[fromCol]) // filter out null (recommends to channels outside dataset)

    const nodes = (dir: RecDir) => {
      const nodeCell = recs.cells({
        group: [RecEx.recCol(dir, colorBy)]
      }, recRows)

      const nodes = nodeCell
        .map(n => {
          // make the keys equivalent to the channels so that selections work. ideology=MRA instead of fromIdeology=MRA
          const keys = { [colorBy]: n.keys[RecEx.recCol(dir, colorBy)] }
          return ({
            keys,
            shapeId: `${dir}.${n.keys[dir == 'from' ? fromCol : toCol]}`,
            mode: dir == 'from' ? 'left' : 'right',
            color: n.color,
            label: n.label
          } as Node)
        })

      return nodes
    }

    const flows = _(recRows)
      .groupBy(r => `${from(r)}.${to(r)}`)
      .map(
        (g, t) => ({
          id: t,
          source: `from.${from(g[0])}`,
          target: `to.${to(g[0])}`,
          value: _.sumBy(g, r => r.relevantImpressionsDaily)
        } as Link))
      .value()

    return { nodes: nodes('from').concat(nodes('to')), links: flows }
  }

  get channels() { return this.props.model.channels }

  centerNodeLayout(selection: Record<keyof ChannelData, string>): Graph<Node[], Link[]> {
    if(!selection) return null

    const colorBy = this.chart.selections.params().colorBy
    const selectionCols = typedKeys(selection)

    const selectionCol = selectionCols.find(c => c == 'channelId') ?? selectionCols.find(c => c == colorBy) ?? null

    if(!selectionCol) return null

    const recs = selectionCol == 'channelId' ? this.props.model.recs : this.props.model.recCats

    /*

   structure of id's ( flow form left to right) for 2 key cols A|B and X|Y
      format: inNodeId (flowId) mainNodeId (flowId) outNodeId
      values: left.A:X (A:X|B:Y) center|B:Y (B:Y|A:X) right.A:X

    example row data for channels ABC and C is selected

    from  to  val   notes
    a     c   12    left
    b     c   6     left
    c     c   2     left & right
    c     a   5     right
    c     b   4     right


    left nodes


    */

    const nodeCol = (dir: RecDir) => RecEx.recCol(dir, selectionCol)
    const recCol = RecEx.recCol
    const flowPartId = (c: RecData | Cell<RecData>, dir: RecDir): string => CellEx.cellValue(c, RecEx.recCol(dir, selectionCol))
    const centerPartId = selection[selectionCol]
    const displayToUnknown = false
    const recRows: RecData[] = recs.rows.filter(r => 
      (flowPartId(r, 'from') == centerPartId || flowPartId(r, 'to') == centerPartId)
      && (displayToUnknown || flowPartId(r, 'to'))
      )

    const nodeCells = (mode: NodeMode) => {
      if (mode == 'center') throw 'nodeCell only works with left or right'
      const dir = mode == 'left' ? 'from' : 'to'
      const modeRecs = recRows.filter(r => flowPartId(r, mode == 'left' ? 'to' : 'from') == centerPartId)

      const cells = recs.cells({
        group: [nodeCol(dir)],
        measures: ['relevantImpressionsDaily'],
        order: { col: 'relevantImpressionsDaily', order: 'desc' },
        labelBy: recs.col(recCol(dir, selectionCols[0])).labelCol,
        colorBy: recCol(dir, colorBy)
      }, modeRecs)

      return cells.slice(null, 10)
    }

    const nodeId = (mode: NodeMode, r: Cell<RecData>): string => {
      const idValue = mode == 'center' ? flowPartId(r, 'from') : flowPartId(r, mode == 'left' ? 'from' : 'to')
      return `${mode}.${idValue}`
    }

    const recToChannelRecord = (r:Record<string, string>) => {
      const channelEntries = _.entries(r).filter(e => RecEx.channelCol(e[0])) // props that exist on channel
      const channelRecord = toRecord(channelEntries, e => RecEx.channelCol(e[0]), e => e[1])
      return channelRecord
    }

    const makeNode = (mode: NodeMode, r: Cell<RecData>): Node => {
      const cProps = recToChannelRecord(r.props)
      const cKey = recToChannelRecord(r.keys)
      return ({
        ...r,
        shapeId: nodeId(mode, r),
        mode: mode,
        keys: cKey,
        props: merge(r.props, cProps)
      })
    }

    const makeNodes = (mode: NodeMode) => nodeCells(mode).map(c => makeNode(mode, c))

    const leftNodes = makeNodes('left')
    const centerCell = leftNodes.find(c => flowPartId(c, 'from') == centerPartId)
    if (!centerCell) return { nodes: [], links: [] }
    const nodes = _.keyBy([makeNode('center', centerCell)].concat(leftNodes).concat(makeNodes('right')), n => n.shapeId)

    const flowCells = recs.cells({
      group: [nodeCol('from'), nodeCol('to')],
      measures: ['relevantImpressionsDaily'],
      order: { col: 'relevantImpressionsDaily', order: 'desc' }
    }, recRows)

    const createFlow = (r: Cell<RecData>, mode: NodeMode) => {
      const centerId = nodeId('center', centerCell)
      return {
        id: ColEx.valueString(_.values(r.keys)),
        source: mode == 'left' ? nodeId('left', r) : centerId,
        target: mode == 'left' ? centerId : nodeId('right', r),
        value: r.measures['relevantImpressionsDaily']
      }
    }

    const links = _(flowCells)
      .flatMap(r => {
        const toCenter = flowPartId(r, 'to') == centerPartId
        const fromCenter = flowPartId(r, 'from') == centerPartId
        return (toCenter && fromCenter) ?
          [createFlow(r, 'left'), createFlow(r, 'right')] // place flows to the center on both left & right sides
          : [createFlow(r, toCenter ? 'left' : 'right')]
      })
      .value()
      .filter(r => nodes[r.source] && nodes[r.target])


    return { nodes: _(nodes).values().value(), links }
  }

  async createChart() {
    let svg = d3.select(this.ref)
    let container = this.chart.createContainer(svg, 'flows')
    let padding = { top:40, bottom:40, left:10, right:10 }
    container.attr('transform', `translate(${padding.left}, ${padding.top})`) // move down 10 px to allow space for title
    let linkG = container.append('g').attr('class', 'links')
    let nodeG = container.append('g').attr('class', 'nodes')

    this.dataRender = () => {
      var w = this.props.width
      var h = this.props.height

      svg.attr('width', w)
      svg.attr('height', h)

      let selections = this.state.selections
      let hl = selections.highlighted

      let highlightedOrSelected = 
        (hl && hl.source != RecFlows.source ? hl.record : null) // ignore highlights from this component for changing center node
        ?? selections.selected.find(_ => true)?.record

      let { nodes, links } = this.centerNodeLayout(highlightedOrSelected) ?? this.betweenColorLayout()
      this.chart.selections.updateSelectableCells(nodes)

      let layout = sankey<NodeExtra, RecFlowExtra>()
        .nodeWidth(10)
        .nodePadding(32)
        .size([w - padding.left - padding.right, h - padding.top - padding.bottom])
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
          return s.mode == 'left' ? s.color : t.color
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
        .attr('rx', 5)

      // add events for both l/r and channel nodes
      this.chart.addShapeEvents(enterNodeRec)

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


      updateNode.exit().remove()

      this.chart.updateShapeEffects(mergedRec)

      let txtMode: Record<NodeMode, { anchor: string, getX: (d: Node) => number, getY: (d: Node) => number }> =
      {
        center:
        {
          anchor: 'start',
          getX: (d: Node) => d.x0,
          getY: (d: Node) => d.y0 - 45
        },
        left:
        {
          anchor: 'start',
          getX: (d: Node) => d.x1 + 10,
          getY: (d: Node) => (d.y1 + d.y0) / 2
        },
        right:
        {
          anchor: 'end',
          getX: (d: Node) => d.x0 - 10,
          getY: (d: Node) => (d.y1 + d.y0) / 2
        }
      }

      let labelText = (d: Node) => {
        // sourceLInks are objects with the source set to this object (i.e. outgoing)
        let incoming = d.incoming > 0 ? d.incoming : _.sum(d.targetLinks.map(l => l.value))
        let outgoing = d.outgoing > 0 ? d.outgoing : _.sum(d.sourceLinks.map(l => l.value))

        return (
          <text className={'label'} textAnchor={txtMode[d.mode].anchor}>
            {d.label}
            {incoming > 0 && (
              <>
                <tspan className={'subtitle-bold'} dy={'1.3em'} x={0}>
                  {compactInteger(incoming, 1)}
                </tspan>
                <tspan className={'subtitle'}> received</tspan>
              </>
            )}
            {outgoing > 0 && (
              <>
                <tspan className={'subtitle-bold'} dy={'1.3em'} x={0}>
                  {compactInteger(outgoing, 1)}
                </tspan>
                <tspan className={'subtitle'}> daily impressions</tspan>
              </>
            )}
          </text>
        )
      }

      mergeNode
        .select('g.label')
        .attr('transform', d => `translate(${txtMode[d.mode].getX(d)}, ${txtMode[d.mode].getY(d)})`) //translate makes g coordinates relative
        .html(d => renderToString(labelText(d)))
    }

    this.dataRender()
  }
}
