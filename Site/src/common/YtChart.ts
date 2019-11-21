import { YtModel, ChannelData } from "./YtModel"
import * as _ from 'lodash'
import { InteractiveDataProps, InteractiveDataState } from "./Chart"


export interface ChannelChartState extends InteractiveDataState {
    colorCol: ChannelAttribute
}

type ChannelAttribute = keyof ChannelData

// For working with the channel/flows/relations charts
export class YtChart {

    static colorAttributeValue(component: React.Component<InteractiveDataProps<any>, ChannelChartState>, channel: ChannelData): string {
        return (channel as any)[component.state.colorCol]
    }
}