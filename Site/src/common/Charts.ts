import * as React from 'react'
import * as d3 from 'd3'

export interface InteractiveDataProps<D>  {
  dataSet: D
  onSelection?: (selection: DataSelection) => void
  initialSelection:DataSelections
}

export interface ChartProps<D> extends InteractiveDataProps<D>{
  width: number
  height: number
}

export interface InteractiveDataState {
  selections: DataSelections
}

export class DataSelections {
  highlights = Array<DataSelection>()
  filters = Array<DataSelection>()

  // updates hightlights/filters by replacing any existing selections of the same tpye/path with this new value
  setSelection(item: DataSelection) {
    var collection = item.type == SelectionType.Filter ? this.filters : this.highlights

    var r = item.path == null ? [] : collection.filter(s => s.path != item.path)
    if (item != null && item.values.filter(v => v != null).length > 0) r = r.concat(item)

    item.type == SelectionType.Filter ? (this.filters = r) : (this.highlights = r)
  }

  clearFilters() {
    this.filters = []
  }

  filteredItems(path: string): string[] {
    let r = this.filters.find(s => s.path == path)
    return r != null && r.values != null ? r.values : []
  }
}

export interface DataSelection {
  path: string
  values: Array<any>
  type: SelectionType
}

export enum SelectionType {
  Filter = 'filter',
  Highlight = 'highlight'
}

export class DataComponentHelper {
  constructor(component: React.Component<InteractiveDataProps<any>, InteractiveDataState>) {
    this.component = component
  }

  component: React.Component<InteractiveDataProps<any>, InteractiveDataState>

  setSelection(selection: DataSelection) {
    if (this.component.props.onSelection) this.component.props.onSelection(selection)
  }

  createContainer(svg: d3.Selection<SVGSVGElement, {}, null, undefined>) {
    let container = svg
      .on('click', d => {
        this.setSelection({ path: null, values: [], type: SelectionType.Filter })
        this.setSelection({ path: null, values: [], type: SelectionType.Highlight })
      })
      .attr('class', 'chart')
      .append<SVGGElement>('g')
      .attr('class', 'chart')

    return container
  }

  addDataShapeEvents<N>(selector: d3.Selection<d3.BaseType, N, d3.BaseType, {}>, getValue: (d: N) => any, selectionPath: string) {
    function createHighlight(path: string, value: any) {
      return { path: path, values: value == null ? [] : [value], type: SelectionType.Highlight }
    }

    function createFilter(path: string, value: any) {
      return { path: path, values: value == null ? [] : [value], type: SelectionType.Filter }
    }

    function onClick(s:DataComponentHelper, d:N) {
      d3.event.stopPropagation()
      s.setSelection(createFilter(selectionPath, getValue(d)))
    }

    selector
      .on('click', d => onClick(this, d))
      .on('mouseover', d => this.setSelection(createHighlight(selectionPath, getValue(d))))
      .on('mouseout', () => this.setSelection(createHighlight(selectionPath, null)))
  }

  highlightedItems(path: string): string[] {
    let r = this.component.state.selections.highlights.find(s => s.path == path)
    return r != null && r.values != null ? r.values : []
  }

  filteredItems(path: string): string[] {
    let r = this.component.state.selections.filters.find(s => s.path == path)
    return r != null && r.values != null ? r.values : []
  }

  filteredOrHighlightedItems(path: string) {
    return this.highlightedItems(path).concat(this.filteredItems(path))
  }
}
