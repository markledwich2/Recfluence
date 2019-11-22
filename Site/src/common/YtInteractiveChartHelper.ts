import { event } from 'd3'
import { InteractiveDataProps, InteractiveDataState, ActionType, SelectionStateHelper } from './Chart'
import { SelectableCell } from './Dim'
import { ChannelData } from './YtModel'


export interface YtParams {
    colorBy: keyof ChannelData
}

export class YtInteractiveChartHelper {

    selections: SelectionStateHelper<ChannelData, YtParams>
    component: React.Component<InteractiveDataProps<any>, InteractiveDataState>

    constructor(component: React.Component<InteractiveDataProps<any>, InteractiveDataState>) {
        this.component = component
        this.selections = new SelectionStateHelper(component.props.onSelection, () => this.component.state.selections)
    }

    createContainer(svg: d3.Selection<SVGSVGElement, {}, null, undefined>, chartName: string) {
        let container = svg
            .on('click', () => {
                this.selections.clearAll()
            })
            .classed('chart', true)
            .classed(chartName, true)
            .append<SVGGElement>('g')
            .classed('chart', true)
            .classed(chartName, true)
        return container
    }


    addShapeEvents<GElement extends d3.BaseType, Datum extends SelectableCell<any>, PElement extends d3.BaseType>(
        selector: d3.Selection<GElement, Datum, PElement, {}>, selectable: boolean = true, hoverable: boolean = true) {
        function onClick(chartHelper: YtInteractiveChartHelper, d: Datum) {
            event.stopPropagation()
            chartHelper.selections.select(d.keys)
        }
        if (selectable)
            selector.on('click', d => onClick(this, d))

        if (hoverable)
            selector
                .on('mouseover', d => {
                    if (d.keys)
                        this.selections.highlight(d.keys)
                })
                .on('mouseout', d => this.selections.clearHighlight())
    }

    addShapeClasses<GElement extends d3.BaseType, Datum extends SelectableCell<any>, PElement extends d3.BaseType>(
        selector: d3.Selection<GElement, Datum, PElement, {}>, selectable: boolean = true) {

        selector
            .classed('selectable', selectable)
            .classed('highlighted', d => d.highlighted)
            .classed('selected', d => d.selected)
            .classed('dimmed', d => d.dimmed)
    }
}
