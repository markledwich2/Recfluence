import { Dic } from "./YtModel"
import _, { assign } from "lodash"
import { Col, ColEx, SelectableCell, CellEx } from "./Dim"
import { merge } from './Utils'


export type SelectionHandler = (action: Action) => void

export interface InteractiveDataProps<D> {
    model: D
    onSelection?: SelectionHandler
}

export interface ChartProps<D> extends InteractiveDataProps<D> {
    width: number
    height: number
}

export interface InteractiveDataState {
    selections: SelectionState
}

export interface Selection<V> {
    record: Record<string, V>
    source?: string
}

export interface SelectionState {
    selected: Selection<string>[]
    highlighted?: Selection<string>
    parameters: Selection<any>
}

export enum ActionType {
    select = 'select',
    highlight = 'highlight',
    clear = 'clear',
    ClearHighlight = 'clearHighlight',
    setParams = 'setParams'
}

export type Action = SelectAction | HighlightAction | ClearAction | ClearHighlight | SetParams

export interface CommonSelectAction {
    type: ActionType
}

export interface SelectAction extends CommonSelectAction {
    type: ActionType.select
    select: Selection<string>[]
}

interface HighlightAction extends CommonSelectAction {
    type: ActionType.highlight
    highlight: Selection<string>
}

interface ClearAction extends CommonSelectAction {
    type: ActionType.clear
}

interface ClearHighlight extends CommonSelectAction {
    type: ActionType.ClearHighlight
}

interface SetParams extends CommonSelectAction {
    type: ActionType.setParams
    params: Selection<any>
}

/** functionality to help modify selection state */
export class SelectionStateHelper<T, TParams> {

    onSelection: SelectionHandler
    getState: () => SelectionState
    source: string

    constructor(getState: () => SelectionState, onSelection?: SelectionHandler, source?: string) {
        this.getState = () => getState() ?? { selected: [{ record: {} }], parameters: { record: {} } }
        this.onSelection = onSelection ?? (a => Object.assign(getState(), this.applyAction(a)))
        this.source = source
    }

    // returns the updated state according to the given action
    applyAction = (action: Action): SelectionState => {
        let state = this.getState()
        let s = (s: any) => Object.assign({}, state, s)
        switch (action.type) {
            case ActionType.clear:
                return s({ selected: [], highlighted: null })

            case ActionType.select:
                const select = action as SelectAction
                const bySelectionKey = _(select.select).groupBy(this.selectionKey).value()
                const nonConflictingSelections = select.select.filter(s => !(this.selectionKey(s) in bySelectionKey))
                const newSelect = nonConflictingSelections.concat(select.select)
                return s({ selected: newSelect, highlighted: state.highlighted })

            case ActionType.highlight:
                return s({ highlighted: action.highlight })

            case ActionType.ClearHighlight:
                return s({ highlighted: null })

            case ActionType.setParams:
                return s({ parameters: action.params })
        }
    }

    private getRecord = (toSelect: keyof T | Col<T> | Record<keyof T, string> | SelectableCell<T>, value: string = null) => {
        if (typeof (toSelect) == 'string')
            return { [toSelect]: value }
        else if (CellEx.isCell(toSelect))
            return toSelect.keys // merge<any>({}, , toSelect.props) // props = selection OR making granular info available
        else if (ColEx.isCol(toSelect))
            return { [toSelect.name]: value }
        else
            return toSelect as Record<string, string>
    }

    highlight = (toHighlight: keyof T | Col<T> | Record<keyof T, string> | SelectableCell<T>, value: string = null) => {
        this.onSelection({
            type: ActionType.highlight,
            highlight: { record: this.getRecord(toHighlight, value), source: this.source }
        })
    }

    select = (toSelect: keyof T | Col<T> | Record<keyof T, string> | SelectableCell<T>, value: string = null) => {
        this.onSelection({
            type: ActionType.select,
            select: [{ record: this.getRecord(toSelect, value), source: this.source }]
        })
    }

    setParam = (params: Record<string, any>) => {
        this.onSelection({ type: ActionType.setParams, params: { record: params, source: this.source } })
    }

    params = (): TParams => this.getState().parameters.record

    clearAll = () => {
        this.onSelection({ type: ActionType.clear })
    }

    clearHighlight = () => {
        this.onSelection({ type: ActionType.ClearHighlight })
    }

    /** a unique string of the tuple columns (not values) e.g. 'name|type' */
    selectionKey = (selection: Selection<string>) => {
        return this.key(_.keys(selection.record))
    }

    key = (names: string[]) => names.join("|")

    //** a unique string of the tuples name+values e.g. 'name=Fred|type=Farmer' */
    valueKey = (tuple: Record<string, string>) => _(tuple).entries().map(t => `${t[0]}=${t[1]}`).join("|")

    getHighlightedValue = (name: string): string => {
        let state = this.getState()
        let h = state.highlighted?.record
        return h && name in h ? state.highlighted.record[name] : null
    }

    // return selected value for the attribute if only one item has been selected
    selectedSingleValue = (col: keyof T | Col<T>): string => {
        const values = this.selectedValues(col)
        return (values.length == 1) ? values[0] : null
    }

    /** return the selected values for the given column */
    selectedValues = (col: keyof T | Col<T>): string[] => {
        const state = this.getState()
        const name = ColEx.isCol<T>(col) ? col.name : col
        const selected = state.selected
            .filter(s => name in s.record)
            .map(s => s.record[name])
        return selected
    }

    highlightedOrSelected = (): Selection<string> => {
        const state = this.getState()
        if (state.highlighted) return state.highlighted
        return (state.selected.length == 1) ? state.selected[0] : null
    }

    highlightedOrSelectedValue = (col: keyof T | Col<T>): string => {
        const name = ColEx.isCol<T>(col) ? col.name : col
        const highlighted = this.getHighlightedValue(name)
        if (highlighted) return highlighted
        const selected = this.selectedSingleValue(name)
        return selected
    }

    updateSelectableCells = (cell: SelectableCell<any>[]) => {
        const state = this.getState()
        const empty = state.selected.length == 0 && !state.highlighted
        cell.forEach(t => {
            const highlighted = empty ? null : state.highlighted && this.selectableContains(t, state.highlighted)
            const selected = empty ? null : state.selected.some(s => s && this.selectableContains(t, s))
            t.selected = selected
            t.highlighted = highlighted
            t.dimmed = !empty && !highlighted && !selected
        })
    }

    selectableContains(cell: SelectableCell<any>, selection: Selection<string>) {
        const cellRecord = merge(cell.props, cell.keys)
        return this.overlaps(cellRecord, selection.record)
    }

    overlaps = (a: Record<string, string | string[]>, b: Record<string, string | string[]>) =>
        a && b && _.entries(b).some(bv => bv[0] in a && this.valueOverlaps(bv[1], a[bv[0]]))

    valueOverlaps = (a: string | string[], b: string | string[]) =>
        Array.isArray(a) ? _.intersection(a, b).length > 0 : a == b

    /** **true** if a is a super-set of b */
    superSetMatch = (a: Record<string, string>, b: Record<string, string>) =>
        a && b && _.entries(b).every(bv => bv[0] in a && bv[1] == a[bv[0]])

}


