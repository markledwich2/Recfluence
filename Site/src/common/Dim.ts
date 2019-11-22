import * as d3 from 'd3'
import _ from 'lodash'
import { useCallback } from 'react'
import { toDic } from './Utils'

/** A cell with state for ocntrolid display and interaction in a chart */
export interface SelectableCell<T> extends Cell<T> {
  /**
   * **true** when this segement is highlighted
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
  static cellValue<T>(cell: Cell<T>, name: keyof T) {
    return cell.keys[name] ?? cell.props[name] ?? cell.measures[name]
  }

  static isCell<T>(o: Object): o is Cell<T> {
    const col = o as Cell<T>
    return col && col.keys !== undefined && col.label !== undefined && col.measures != undefined
  }
}


export interface Cell<T> {
  /**  coordinates to this peice of aggregated data */
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
  aggreagtion?: Aggregation
  valueLabel?: keyof T
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
  label?: keyof T
  order?: { col: keyof T, order: 'asc' | 'desc' }
  measures?: (keyof T)[],
}

interface QueryContext<T> {
  q: DimQuery<T>,
  groupCols: (keyof T)[],
  props: (keyof T)[],
  valueCol: keyof T,
  getColor: (r: T) => string,
  getLabel: (r: T) => string
}

export class Dim<T> {

  meta: DimMeta<T>
  private colDic: _.Dictionary<Col<T>>

  constructor(dim: DimMeta<T>, rows?: T[]) {
    this.meta = dim
    this.colDic = _(dim.cols).keyBy(c => c.name).value()
  }

  get name(): string {
    return this.meta.name
  }

  col(name: keyof T): Col<T> {
    return this.colDic[name as string] ?? { name: name }
  }

  get cols(): Col<T>[] {
    return _(this.colDic).values().value()
  }

  createCellContext(rows: T[], q: DimQuery<T>): QueryContext<T> {
    let groupCols = this.colSet(q.group, q.colorBy)
    let props = this.colSet(groupCols, q.order?.col, q.label) // unique attribute properties and order by
    let defaultCol = groupCols[0]

    // colours can come explicitly from a columns metadata, or fall back or a pallet then none
    let colorBy = this.col(q.colorBy ?? defaultCol)
    let colorValues = colorBy ? _.uniq(rows.map(r => r[colorBy.name]?.toString())) : [] // need the full list of possible vlaues to create a pallet

    let pallet = colorBy ? d3.scaleOrdinal(colorBy.pallet ?? d3.schemeCategory10).domain(colorValues) : null
    let colorByValue = _(colorBy?.values ?? []).keyBy(c => c.value).value()
    let getColor = (r: T) => colorBy ? colorByValue[r[colorBy.name]?.toString()]?.color ?? pallet(r[colorBy.name]?.toString()) : null

    // labels can come explicitly form a columns metadata, otherwise it will come from the default columsn value lable, otherwise it is the value
    let labelBy = this.col(q.label ?? this.col(defaultCol).valueLabel ?? defaultCol)
    let labelByValue = _(labelBy.values).keyBy(c => c.value).value()
    let getLabel = (r: T) => labelByValue[r[labelBy.name]?.toString()]?.label ?? r[labelBy.name]?.toString()

    return { q, groupCols, props, valueCol: defaultCol, getColor, getLabel }
  }

  private createCell(x: QueryContext<T>, g: T[]): Cell<T> {
    let keys = toDic(x.groupCols, c => c, c => g[0][c]?.toString())
    let measures = toDic(x.q.measures, m => m, m => this.colValue(this.col(m), g))
    let props = toDic(x.props, p => p, p => this.colValue(this.col(p), g))
    return <Cell<T>>{
      keys: keys,
      props: props,
      color: x.getColor(g[0]),
      label: x.getLabel(g[0]),
      measures: measures
    }
  }

  /** get cell information for data that is pre-aggregated */
  rowCells(rows: T[], q: DimQuery<T>): { row: T, cell: Cell<T> }[] {
    let x = this.createCellContext(rows, q)
    let rowCells = rows.map(r => ({ row: r, cell: this.createCell(x, [r]) }))
    return rowCells
  }

  /** get cell information for a set of rows. This will aggregate the given rows into cells */
  cells(rows: T[], q: DimQuery<T>): Cell<T>[] {
    let x = this.createCellContext(rows, q)
    let cells = _(rows)
      .groupBy(r => ColEx.valueString(x.groupCols.map(c => r[c].toString())))
      .map((g, k) => this.createCell(x, g))

    if (q.order) {

      cells = cells.orderBy(c => CellEx.cellValue(c, q.order.col), q.order.order)
    }

    return cells.value()
  }

  private colValue(col: Col<T>, g: T[]): string | number {
    if (col.type && col.type == ColType.Measure || typeof (g[0][col.name]) == "number") {
      return this.agg(col, g)
    }
    return g[0][col.name].toString()
  }

  private agg(col: Col<T>, g: T[]): number {
    let values = g.map(r => r[col.name] as unknown as number)
    let reducer = (sum: number, v: number) => sum + v
    if (col.aggreagtion) {
      switch (col.aggreagtion) {
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

  private colSet(cols: (keyof T)[], ...more: (keyof T)[]): (keyof T)[] {
    return _.uniq(cols.concat(more).filter(c => c))
  }
}

