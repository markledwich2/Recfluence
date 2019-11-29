import React, { } from 'react'
import Select from 'react-select'
import _ from 'lodash'
import { YtInteractiveChartHelper, YtParams } from "../common/YtInteractiveChartHelper"
import { YtModel, ChannelData } from '../common/YtModel'
import { delay } from '../common/Utils'
import { InteractiveDataProps, ChartProps, InteractiveDataState, SelectionState, SelectionStateHelper } from '../common/Chart'
import { Dim, Col } from '../common/Dim'
import { YtTheme } from '../common/YtTheme'

interface State {
}
interface Props extends InteractiveDataProps<YtModel> { 
  selections:SelectionState
}

interface Option {
  value: string
  label: string
}

export class SearchChannels extends React.Component<Props, State> {
  state: Readonly<State>

  constructor(props:Readonly<Props>) {
    super(props)
  }

  ref: Select<Option>
  lastFocusedOption: Option
  lastSelectedOption: Option
  selectionHelper = new SelectionStateHelper<ChannelData, YtParams>(this.props.onSelection, () => this.props.selections)

  get channels(): Dim<ChannelData> {
    return this.props.model.channels
  }

  shouldComponentUpdate(props: Props, nextState: State) {
    let sh = new SelectionStateHelper<ChannelData, YtParams>(null, () => props.selections)
    const channelId = sh.selectedSingleValue('channelId')
    let r = this.ref as any
    let selectedOption = r.select.state.selectValue.find(() => true)
    let selectedValue = selectedOption ? selectedOption.value : undefined
    return channelId != selectedValue
  }

  onUserInteracted = () => {
    let r = this.ref as any
    delay(1).then(() => {
      const focusedOption = r.select.state.focusedOption

      if (this.lastFocusedOption !== focusedOption && r.state.menuIsOpen) {
        this.lastFocusedOption = focusedOption
        this.selectionHelper.select(this.idCol, focusedOption.value)
      }
    })
  }

  onSelected = (c: Option) => {
    this.selectionHelper.select(this.idCol, c.value)
  }

  private get idCol() : Col<ChannelData> {
    return this.channels.col("channelId")
  }

  render() {
    let channelId = this.selectionHelper.selectedSingleValue(this.idCol)
    let options = _(this.channels.rows)
      .map(c => ({ value: c.channelId, label: c.title } as Option))
      .orderBy(o => o.label)
      .value()

    let selectedValue = options.find(c => c.value == channelId)

    return (
      // onMouseMove={this.onUserInteracted onClick={this.onUserInteracted}
      <div>
        <Select<Option>
          //onKeyDown={this.onUserInteracted}
          onChange={this.onSelected}
          options={options}
          placeholder="Select Channel"
          styles={YtTheme.selectStyle}
          theme={YtTheme.selectTheme}
          value={selectedValue ? selectedValue : null}
          ref={(r) => (this.ref = r)}
        />
      </div>
    )
  }
}
