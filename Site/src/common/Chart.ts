import { Dic } from "./YtModel"
import _ from "lodash"
import { Col, ColEx, SelectableCell } from "./Dim"


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

export interface SelectionState {
    selected: Record<string, string>[]
    highlighted?: Record<string, string>
    parameters: Record<string, any>
}

export enum ActionType {
    Select,
    Highlight,
    Clear,
    ClearHighlight,
    SetParams
}

export type Action = SelectAction | HighlightAction | ClearAction | ClearHighlight | SetParams

export interface SelectAction {
    type: ActionType.Select
    select: Record<string, string>[]
}

interface HighlightAction {
    type: ActionType.Highlight
    highlight: Record<string, string>
}

interface ClearAction {
    type: ActionType.Clear
}

interface ClearHighlight {
    type: ActionType.ClearHighlight
}

interface SetParams {
    type: ActionType.SetParams
    params: Record<string, any>
}

/** functionality to help modify selection state */
export class SelectionStateHelper<T, TParams> {

    onSelection: SelectionHandler
    getState: () => SelectionState

    constructor(onSelection: SelectionHandler, getState: () => SelectionState) {
        this.onSelection = onSelection
        this.getState = getState
    }

    // returns the updated state according to the given action
    applyAction = (action: Action): SelectionState => {
        let state = this.getState()
        let s = (s: any) => Object.assign({}, state, s)
        switch (action.type) {
            case ActionType.Clear:
                return s({ selected: [], highlighted: null })

            case ActionType.Select:
                const select = action as SelectAction
                const bySelectionKey = _(select.select).groupBy(this.selectionKey).value()
                const nonConflictingSelections = select.select.filter(s => !(this.selectionKey(s) in bySelectionKey))
                const newSelect = nonConflictingSelections.concat(select.select)
                return s({ selected: newSelect, highlighted: state.highlighted })

            case ActionType.Highlight:
                return s({ highlighted: action.highlight })

            case ActionType.ClearHighlight:
                return s({ highlighted: null })

            case ActionType.SetParams:
                return s({ parameters: action.params })
        }
    }


    highlight = (toHighlight: keyof T | Col<T> | Record<keyof T, string>, value: string = null) => {
        var action: HighlightAction
        if (typeof (toHighlight) == 'string')
            action = { type: ActionType.Highlight, highlight: { [toHighlight]: value } }
        else if (ColEx.isCol(toHighlight))
            action = { type: ActionType.Highlight, highlight: { [toHighlight.name]: value } }
        else
            action = { type: ActionType.Highlight, highlight: toHighlight as Record<string, string> }

        this.onSelection(action)
    }

    select = (toSelect: keyof T | Col<T> | Record<keyof T, string>, value: string = null) => {
        var action: SelectAction
        if (typeof (toSelect) == 'string')
            action = { type: ActionType.Select, select: [{ [toSelect]: value }] }
        else if (ColEx.isCol(toSelect))
            action = { type: ActionType.Select, select: [{ [toSelect.name]: value }] }
        else
            action = { type: ActionType.Select, select: [toSelect as Record<string, string>] }

        this.onSelection(action)
    }

    setParam = (params: Record<string, any>) => {
        this.onSelection({ type: ActionType.SetParams, params })
    }

    params = (): TParams => this.getState().parameters

    clearAll = () => {
        this.onSelection({ type: ActionType.Clear })
    }

    clearHighlight = () => {
        this.onSelection({ type: ActionType.ClearHighlight })
    }

    /** a unique string of the tuple columns (not values) e.g. 'name|type' */
    selectionKey = (selection: Record<string, string>) => {
        return this.key(_.keys(selection))
    }

    key = (names: string[]) => names.join("|")

    //** a unique string of the tuples name+values e.g. 'name=Fred|type=Farmer' */
    valueKey = (tuple: Record<string, string>) => _(tuple).entries().map(t => `${t[0]}=${t[1]}`).join("|")

    getHighlightedValue = (name: string): Record<string, string> => {
        let state = this.getState()
        return state.highlighted && name in state.highlighted ? state.highlighted : null
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
            .filter(s => name in s)
            .map(s => s[name])
        return selected
    }

    highlightedOrSelected = (): Record<string, string> => {
        const state = this.getState()
        if (state.highlighted) return state.highlighted
        return (state.selected.length == 1) ? state.selected[0] : null
    }

    highlightedOrSelectedValue = (col: keyof T | Col<T>): string => {
        const name = ColEx.isCol<T>(col) ? col.name : col
        const highlighted = this.getHighlightedValue(name)
        if (highlighted) return highlighted[name]
        const selected = this.selectedSingleValue(name)
        return selected
    }

    updateSelectableCells = (cell: SelectableCell<any>[]) => {
        const state = this.getState()
        const empty = state.selected.length == 0 && !state.highlighted
        cell.forEach(t => {
            const highlighted = empty ? null : this.selectableContains(t, state.highlighted)
            const selected = empty ? null : state.selected.some(s => this.selectableContains(t, s))
            t.selected = selected
            t.highlighted = highlighted
            t.dimmed = !empty && !highlighted && !selected
        })
    }

    selectableContains(a: SelectableCell<any>, b: Record<string, string>) {
        const keysProps = Object.assign({}, a.keys, a.props)
        return this.superSetMatch(keysProps, b)
    }

    /** **true** if a is a super-set of b */
    superSetMatch = (a: Record<string, string>, b: Record<string, string>) => {
        return a && b && _.entries(b).every(bv => bv[0] in a && bv[1] == a[bv[0]])
    }
}


