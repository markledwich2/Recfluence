import { Dic } from "./YtModel"
import _ from "lodash"
import { Col, ColEx, SelectableCell } from "./Dim"


export type SelectionHandler = (action: ActionClass) => void

export interface InteractiveDataProps<D> {
    dataSet: D
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
}

export type ActionClass = SelectAction | HighlightAction | ClearAction | ClearHighlight

export class SelectAction {

    constructor(...tuples: Record<string, string>[]) {
        this.tuples = tuples
    }

    type = ActionType.Select
    tuples: Record<string, string>[]
}

export class HighlightAction {
    constructor(tuple: Record<string, string>) {
        this.tuple = tuple
    }

    type = ActionType.Highlight
    tuple: Record<string, string>
}

class ClearAction {
    type = ActionType.Clear
}

class ClearHighlight {
    type = ActionType.ClearHighlight
}

export enum ActionType {
    Select = 'select',
    Highlight = 'highlight',
    Clear = 'clear',
    ClearHighlight = 'clearHighlight'
}

/** funcitoanlity to help modify selection state */
export class SelectionStateHelper {

    onSelection: SelectionHandler
    getState: () => SelectionState

    constructor(onSelection: SelectionHandler, getState: () => SelectionState) {
        this.onSelection = onSelection
        this.getState = getState
    }

    // returns the updated state according to the given action
    applyAction = (action: ActionClass): SelectionState => {
        let state = this.getState()
        switch (action.type) {
            case ActionType.Clear:
                return { selected: [], highlighted: null }

            case ActionType.Select:
                const select = action as SelectAction
                const bySelectionKey = _(select.tuples).groupBy(this.selectionKey).value()
                const nonConfictingSelections = select.tuples.filter(s => !(this.selectionKey(s) in bySelectionKey))
                const newSelect = nonConfictingSelections.concat(select.tuples)
                return { selected: newSelect, highlighted: state.highlighted }

            case ActionType.Highlight:
                return { selected: state.selected, highlighted: (action as HighlightAction).tuple }

            case ActionType.ClearHighlight:
                return { selected: state.selected, highlighted: null }
        }
    }


    highlight = (toHighlight: string | Col<any> | Record<string, string>, value: string = null) => {
        var action
        if (typeof (toHighlight) == 'string')
            action = new HighlightAction({ [toHighlight]: value })
        else if (ColEx.isCol(toHighlight))
            action = new HighlightAction({ [toHighlight.name]: value })
        else
            action = new HighlightAction(toHighlight)

        this.onSelection(action)
    }

    select = (toSelect: string | Col<any> | Record<string, string>, value: string = null) => {
        var action
        if (typeof (toSelect) == 'string')
            action = new SelectAction({ [toSelect]: value })
        else if (ColEx.isCol(toSelect))
            action = new SelectAction({ [toSelect.name]: value })
        else
            action = new SelectAction(toSelect)

        this.onSelection(action)
    }

    clearAll = () => {
        this.onSelection(new ClearAction())
    }

    clearHighlight = () => {
        this.onSelection(new ClearHighlight())
    }

    /** a unique string of the tuple columns (not values) e.g. 'name|type' */
    selectionKey = (selection: Record<string, string>) => {
        return this.key(_.keys(selection))
    }

    key = (names: string[]) => names.join("|")

    //** a unique stirng of the tuples name+values e.g. 'name=Fred|type=Farmer' */
    valueKey = (tuple: Record<string, string>) => _(tuple).entries().map(t => `${t[0]}=${t[1]}`).join("|")

    getHighlightedValue = (name: string): Record<string, string> => {
        let state = this.getState()
        return state.highlighted && name in state.highlighted ? state.highlighted : null
    }

    // return selected value for the attribute if only one item has been selected
    selectedSingleValue = (col: string | Col<any>): string => {
        const values = this.selectedValues(col)
        return (values.length == 1) ? values[0] : null
    }

    /** return the selected values for the given column */
    selectedValues = (col: string | Col<any>): string[] => {
        const state = this.getState()
        const name = ColEx.isCol(col) ? col.name : col
        const selected = state.selected
            .filter(s => name in s)
            .map(s => s[name])
        return selected
    }

    highlitedOrSelectedValue = (col: string | Col<any>): string => {
        const name = ColEx.isCol(col) ? col.name : col
        const highighted = this.getHighlightedValue(name)
        if (highighted) return highighted[name]
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
        return this.supersetMatch(keysProps, b)
    }

    /** **true** if a is a superset of b */
    supersetMatch = (a: Record<string, string>, b: Record<string, string>) => {
        return a && b && _.entries(b).every(bv => bv[0] in a && bv[1] == a[bv[0]])
    }
}


