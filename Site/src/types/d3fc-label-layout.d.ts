
declare module 'd3fc-label-layout' {

    export function layoutTextLabel(): LayoutTextLabel

    type SelectionCallable<GElement extends d3.BaseType, Datum> = (selection: d3.Selection<GElement, Datum, d3.BaseType, {}>, ...args: any[]) => void

    interface LayoutTextLabel {
        padding(padding: number): this
        value(): string
        value(value: ((d: any) => string)): this
    }

    interface LayoutStrategy {

    }

    interface LayoutLabel<Datum> extends SelectionCallable<d3.BaseType, Datum> {
        size(size: Array<number> | ((data: Datum, index: number, group: Array<d3.BaseType>) => Array<number>)): this
        position(position: Array<number>): this
        position(position: ((data: any) => Array<number>)): this
        component(component: any): this
    }

    export function layoutRemoveOverlaps(strategy: LayoutStrategy): LayoutStrategy

    export function layoutGreedy(): LayoutStrategy

    export function layoutLabel<Datum>(strategy: LayoutStrategy): LayoutLabel<Datum>

}