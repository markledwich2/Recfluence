import * as d3 from 'd3'
import _ from 'lodash'
import { toRecord } from './Utils'

/** A cell with state for display and interaction in a chart */
export interface SelectableCell<T> extends Cell<T> {
  /**
   * **true** when this segment is highlighted
   */
  highlighted?: boolean,

  /** 
   * **true** when other segments are highlighted/selected 
   * */
  dimmed?: boolean,

  /**
   * **true** when this segment it selected
   */
  selected?: boolean
}


export class CellEx {
  
  static cellValue<T>(c: Cell<T>|T, name: keyof T) {
    return CellEx.isCell(c) ?
       c.keys[name] ?? c.props[name] ?? c.measures[name]
       : c[name]
  }

  static isCell<T>(o: Object): o is Cell<T> {
    const cell = o as Cell<T>
    return cell && cell.keys !== undefined && cell.label !== undefined
  }
}


export interface Cell<T> {
  /**  coordinates to this piece of aggregated data */
  keys: Record<keyof T, string>

  /** property values for this cell */
  props: Record<keyof T, any>
  measures: Record<keyof T, number>

  label: string
  color?: string
}

export interface DimMeta<T> {
  name: string
  cols: Col<T>[]
}

export enum ColType {
  Dim,
  Measure
}

export enum Aggregation {
  Sum,
  Min,
  Max
}

export interface Col<T> {
  type?: ColType
  name: keyof T
  label?: string
  values?: ColValueMeta<string>[]
  pallet?: readonly string[]
  props?: (keyof T)[]
  aggregation?: Aggregation
  labelCol?: keyof T
}

export class ColEx {
  /** a string of all the values of a tuple combined */
  static valueString<T>(t: T[]): string {
    return t.join("|")
  }

  static valueStringCol<T>(row: T, cols: (keyof T)[]): string {
    return cols.map(c => row[c].toString()).join('|')
  }

  static isCol<T>(o: Object): o is Col<T> {
    const col = o as Col<T>
    return col && col.name !== undefined
  }

  /**  a simple way to access the explicit metadata labels for a column. For labelBy column logic use a cell query  */
  static labelFunc<T>(col: Col<T>): (value: string) => string {
    if (!col.values) return v => v
    const metaByVal = _.keyBy(col.values.filter(l => l.label), v => v.value)
    return v => metaByVal[v]?.label ?? v
  }

  /** a simple way to access the explicit metadata colors for a column. For pallet & colorBy column logic use a cell query  */
  static colorFunc<T>(col: Col<T>): (value: string) => string {
    if (!col.values) return v => v
    const metaByVal = _.keyBy(col.values.filter(l => l.color), v => v.value)
    return v => metaByVal[v]?.color ?? v
  }
}

export interface ColValueMeta<T> {
  value: T,
  color?: string,
  label?: string
}

export interface DimQuery<T> {
  /** cols to group by. Must be provided, even when preAggregated is true so that the results can be formed correctly  */
  group: (keyof T)[]
  preAggregated?: boolean
  colorBy?: keyof T
  labelBy?: keyof T
  order?: { col: keyof T, order: 'asc' | 'desc' }
  measures?: (keyof T)[],
  props?: (keyof T)[],
}

interface QueryContext<T> {
  q: DimQuery<T>,
  groupCols: (keyof T)[],
  props: (keyof T)[],
  valueCol: keyof T,
  getColor: (r: T | Cell<T>) => string,
  getLabel: (r: T | Cell<T>) => string
}

export class Dim<T> {

  meta: DimMeta<T>
  rows: T[]
  private colDic: _.Dictionary<Col<T>>


  constructor(dim: DimMeta<T>, rows?: T[]) {
    this.meta = dim
    this.rows = rows
    this.colDic = _(dim.cols).keyBy(c => c.name).value()
  }

  get name(): string {
    return this.meta.name
  }

  col(name: keyof T): Col<T> {
    return this.colDic[name as string] ?? { name: name }
  }

  labelCol(col: keyof T): Col<T> {
    return this.col(this.col(col).labelCol ?? col)
  }

  get cols(): Col<T>[] {
    return _(this.colDic).values().value()
  }

  queryContext(q: DimQuery<T>, rows?: T[]): QueryContext<T> {
    rows = rows ?? this.rows

    const groupProps = this.set(_(q.group).flatMap(k => {
      const col = this.col(k)
      return [k].concat(col.props).concat(col.labelCol)
    }).value())
    const allProps = this.set((q.props ?? []).concat(groupProps).concat(q.colorBy, q.order?.col))

    let defaultCol = q.group[0]

    // colours can come explicitly from a columns metadata, or fall back or a pallet then none
    let colorBy = this.col(q.colorBy ?? defaultCol)

    let colorValues = colorBy ? _.uniq(rows.map(r => r[colorBy.name]?.toString())) : [] // need the full list of possible values to create a pallet

    let pallet = colorBy ? d3.scaleOrdinal(colorBy.pallet ?? d3.schemeCategory10).domain(colorValues) : null
    let colorByValue = _(colorBy?.values ?? []).keyBy(c => c.value).value()
    let getColor = (r: T | Cell<T>) => {
      let value = CellEx.isCell(r) ? CellEx.cellValue(r, colorBy.name) : r[colorBy.name]
      return colorBy ? colorByValue[value?.toString()]?.color ?? pallet(value?.toString()) : null
    }

    // labels can come explicitly form a columns metadata, otherwise it will come from the default columns value label, otherwise it is the value
    let labelBy = this.col(q.labelBy ?? this.col(defaultCol).labelCol ?? defaultCol)
    let labelByValue = _(labelBy.values).keyBy(c => c.value).value()
    let getLabel = (r: T | Cell<T>) => {
      let value = CellEx.isCell(r) ? CellEx.cellValue(r, labelBy.name) : r[labelBy.name]
      value = value ?? ''
      return labelByValue[value?.toString()]?.label ?? value?.toString()
    }

    return { q, groupCols: q.group, props: allProps, valueCol: defaultCol, getColor, getLabel }
  }

  private createCell(x: QueryContext<T>, g: T[]): Cell<T> {
    let keys = toRecord(x.groupCols, c => c, c => g[0][c]?.toString())
    let measures = toRecord(x.q.measures, m => m, m => this.colValue(this.col(m), g) as number)
    let props = toRecord(x.props, p => p, p => this.colValue(this.col(p), g))
    let cell:Cell<T> = {
      keys,
      props,
      color: x.getColor(g[0]),
      label: x.getLabel(g[0]),
      measures
    }
    return cell
  }

  /** get cell information for data that is pre-aggregated */
  rowCells(q: DimQuery<T>, rows?: T[]): { row: T, cell: Cell<T> }[] {
    rows = rows ?? this.rows
    let x = this.queryContext(q, rows)
    let rowCells = rows.map(r => ({ row: r, cell: this.createCell(x, [r]) }))
    return rowCells
  }

  /** get cell information for a set of rows. This will aggregate the given rows into cells */
  cells(q: DimQuery<T>, rows?: T[]): Cell<T>[] {
    rows = rows ?? this.rows
    let x = this.queryContext(q, rows)
    let cells = _(rows)
      .groupBy(r => ColEx.valueString(x.groupCols.map(c => r[c]?.toString())))
      .map((g, k) => this.createCell(x, g))

    if (q.order)
      cells = cells.orderBy(c => CellEx.cellValue(c, q.order.col), q.order.order)

    return cells.value()
  }

  private colValue(col: Col<T>, g: T[]): string | number | string[] {
    if (col.type && col.type == ColType.Measure || typeof (g[0][col.name]) == "number") {
      return this.agg(col, g)
    }
    let uniqValues = _(g).groupBy(col.name).keys().value()
    return uniqValues.length == 1 ?
      uniqValues[0].toString()
      : uniqValues
  }

  private agg(col: Col<T>, g: T[]): number {
    let values = g.map(r => r[col.name] as unknown as number)
    let reducer = (sum: number, v: number) => sum + v
    if (col.aggregation) {
      switch (col.aggregation) {
        case Aggregation.Max:
          reducer = (max: number, v: number) => v > max ? v : max
          break
        case Aggregation.Min:
          reducer = (min: number, v: number) => v < min ? v : min
      }
    }

    let res = values.reduce(reducer, 0)
    return res
  }

  private set(cols: (keyof T)[]): (keyof T)[] {
    return _.uniq(cols.filter(c => c))
  }
}

