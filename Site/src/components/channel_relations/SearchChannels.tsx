import React, { } from 'react'
import Select, { createFilter } from 'react-select'
import _ from 'lodash'
import { YtInteractiveChartHelper, YtParams } from "../../common/YtInteractiveChartHelper"
import { YtModel, ChannelData } from '../../common/YtModel'
import { delay } from '../../common/Utils'
import { InteractiveDataProps, ChartProps, InteractiveDataState, SelectionState, SelectionStateHelper } from '../../common/Chart'
import { Dim, Col } from '../../common/Dim'
import { YtTheme } from '../../common/YtTheme'

interface State {
}
interface Props extends InteractiveDataProps<YtModel> {
  selections: SelectionState
}

interface Option {
  value: string
  label: string
}

export class SearchChannels extends React.Component<Props, State> {
  state: Readonly<State>

  constructor(props: Readonly<Props>) {
    super(props)
  }

  ref: Select<Option>
  lastFocusedOption: Option
  lastSelectedOption: Option
  selectionHelper = new SelectionStateHelper<ChannelData, YtParams>(() => this.props.selections, this.props.onSelection, SearchChannels.source)
  static source = 'search'

  shouldComponentUpdate(props: Props, nextState: State) {
    let sh = new SelectionStateHelper<ChannelData, YtParams>(() => props.selections, null, SearchChannels.source)
    const channelId = sh.selectedSingleValue('channelId')
    let r = this.ref as any
    if (!r) return true
    let selectedOption = r.select.state.selectValue.find(() => true)
    let selectedValue = selectedOption ? selectedOption.value : undefined
    return channelId != selectedValue
  }

  onUserInteracted = () => {
    let r = this.ref as any
    if (!r) return
    delay(1).then(() => {
      const focusedOption = r.select.state.focusedOption

      if (this.lastFocusedOption !== focusedOption && r.state.menuIsOpen) {
        this.lastFocusedOption = focusedOption
        this.selectionHelper.select('channelId', focusedOption.value)
      }
    })
  }

  onSelected = (c: Option) => {
    this.selectionHelper.select('channelId', c.value)
  }

  render() {
    const channels = this.props.model?.channels
    if (!channels) return <></>

    let channelId = this.selectionHelper.selectedSingleValue(channels.col('channelId'))
    let options = _(channels.rows)
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
          filterOption={createFilter({ ignoreAccents: false })}
          ref={(r) => (this.ref = r)}
        />
      </div>
    )
  }
}
