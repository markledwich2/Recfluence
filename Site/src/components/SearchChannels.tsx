import React, { } from 'react'
import Select from 'react-select'
import _ from 'lodash'
import { YtInteractiveChartHelper } from "../common/YtInteractiveChartHelper"
import { YtModel, ChannelData } from '../common/YtModel'
import { delay } from '../common/Utils'
import { InteractiveDataProps, ChartProps, InteractiveDataState, SelectionState, SelectionStateHelper } from '../common/Chart'
import { Dim, Col } from '../common/Dim'

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
  selectionHelper = new SelectionStateHelper(this.props.onSelection, () => this.props.selections)

  get dim(): Dim<ChannelData> {
    return this.props.dataSet.channelDim
  }

  shouldComponentUpdate(props: Props, nextState: State) {
    let sh = new SelectionStateHelper(null, () => props.selections)
    const channelId = sh.selectedSingleValue(this.idCol)
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
    return this.dim.col("channelId")
  }

  render() {
    console.log("search render", this.props.selections)
    
    let channelId = this.selectionHelper.selectedSingleValue(this.idCol)
    let options = _(this.props.dataSet.channels)
      .map(c => ({ value: c.channelId, label: c.title } as Option))
      .orderBy(o => o.label)
      .value()

    let selectedVlaue = options.find(c => c.value == channelId)

    return (
      // onMouseMove={this.onUserInteracted onClick={this.onUserInteracted}
      <div>
        <Select<Option>
          //onKeyDown={this.onUserInteracted}
          onChange={this.onSelected}
          options={options}
          placeholder="Select Channel"
          styles={{
            option: (p, s) => ({ ...p, color: '#ccc', backgroundColor: s.isFocused ? '#666' : 'none' }),
            control: styles => ({ ...styles, backgroundColor: 'none', borderColor: '#333', outline: 'none' }),
            dropdownIndicator: styles => ({ ...styles, color: '#ccc' }),
            placeholder: styles => ({ ...styles, outline: 'none', color: '#ccc' })
          }}
          theme={theme => ({
            ...theme,
            colors: {
              //...theme.colors,
              text: '#ccc',
              primary: '#ccc',
              neutral0: '#333'
            }
          })}
          value={selectedVlaue ? selectedVlaue : null}
          ref={(r) => (this.ref = r)}
        />
      </div>
    )
  }
}
