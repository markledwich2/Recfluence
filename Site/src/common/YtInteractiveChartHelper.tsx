import * as d3 from 'd3'
import { InteractiveDataProps, InteractiveDataState, ActionType, SelectionStateHelper } from './Chart'
import { SelectableCell } from './Dim'
import { ChannelData } from './YtModel'
import React from 'react'
import { range } from 'd3'
import { renderToString } from 'react-dom/server'
import { merge } from './Utils'


export interface YtParams {
    colorBy: keyof ChannelData
}


interface ShapeCfg<T> {
    selectable?: boolean
    hoverable?: boolean
    unselectedGlow?: (element: T) => boolean
}

export class YtInteractiveChartHelper {

    selections: SelectionStateHelper<ChannelData, YtParams>
    component: React.Component<InteractiveDataProps<any>, InteractiveDataState>

    constructor(component: React.Component<InteractiveDataProps<any>, InteractiveDataState>, source:string) {
        this.component = component
        this.selections = new SelectionStateHelper(component.props.onSelection, () => this.component.state.selections, source)
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

        var glowFilters = (glows: { name: string, blur?: number, intensity?: number }[]) => {
            return (<>
                {glows.map(g => (
                <filter key={g.name} id={g.name} width={'800%'} height={'800%'} x={'-400%'} y={'-400%'} > 
                    <feGaussianBlur stdDeviation={g.blur ?? 5} result='coloredBlur'/>
                    <feMerge>
                        {range(g.intensity ?? 1).map(_ => (<feMergeNode in='coloredBlur' />))}
                        <feMergeNode in='SourceGraphic' />
                    </feMerge>
                </filter>))}
            </>)
        }

        var defs = svg.append("defs")
        defs.html(renderToString(glowFilters([
          { name: 'glow', blur: 5 }, 
          { name: 'glowBig', blur: 10, intensity:3 }])))

        return container
    }


    addShapeEvents<GElement extends d3.BaseType, Datum extends SelectableCell<any>, PElement extends d3.BaseType>(
        selector: d3.Selection<GElement, Datum, PElement, {}>, cfg?: ShapeCfg<Datum>) {
        const selectable = cfg?.selectable ?? true
        const hoverable = cfg?.hoverable ?? true

        function onClick(chartHelper: YtInteractiveChartHelper, d: Datum) {
            event.stopPropagation()
            chartHelper.selections.select(d)
        }
        if (selectable)
            selector.on('click', d => onClick(this, d))

        if (hoverable)
            selector
                .on('mouseover', d => this.selections.highlight(d))
                .on('mouseout', _ => this.selections.clearHighlight())
    }

    updateShapeEffects<GElement extends d3.BaseType, Datum extends SelectableCell<any>, PElement extends d3.BaseType>(
        selector: d3.Selection<GElement, Datum, PElement, {}>, cfg?: ShapeCfg<Datum>) {

        const selectable = cfg?.selectable ?? true
        const unselectedGlow = cfg?.unselectedGlow ?? ((d: Datum) => true)

        selector
            .classed('selectable', selectable)
            .classed('highlighted', d => d.highlighted)
            .classed('selected', d => d.selected)
            .classed('dimmed', d => d.dimmed)

        if (selectable) {
            selector
                .attr('fill', d => d.selected || d.highlighted ? '#fff' : d.color)
                .attr('filter', d => d.selected || d.highlighted ? 'url(#glowBig)'
                    : unselectedGlow(d) ? 'url(#glow)' : null)
        }
    }
}
