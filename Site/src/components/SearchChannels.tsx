import React, { Component, Fragment } from 'react';
import Select from 'react-select'
import _ from 'lodash'
import { InteractiveDataProps, InteractiveDataState, DataComponentHelper, DataSelections, ChartProps } from '../common/Charts'
import { YtNetworks, Graph, YtData } from '../common/YtData'
import { delay, jsonEquals } from '../common/Utils';

interface State extends InteractiveDataState {
}
interface Props extends InteractiveDataProps<YtData> { }

interface Option {
  value: string
  label: string
}

export class SearchChannels extends React.Component<Props, State> {
  chart: DataComponentHelper = new DataComponentHelper(this)
  state: Readonly<State> = {
    selections: this.props.initialSelection,
  }

  ref: Select<Option>
  lastFocusedOption: Option
  lastSelectedOption: Option

  shouldComponentUpdate(props: ChartProps<YtData>, nextState: State) {
    var channelId = this.chart.filteredItems(YtNetworks.ChannelIdPath).find(() => true);
    let r = this.ref as any
    let selectedOption =  r.select.state.selectValue.find(() => true)
    let selectedValue = selectedOption ? selectedOption.value : undefined

    return channelId != selectedValue;
  }

  onUserInteracted = () => {
    let r = this.ref as any
    delay(1).then(() => {
      const focusedOption = r.select.state.focusedOption

      if (this.lastFocusedOption !== focusedOption && r.state.menuIsOpen) {
        this.lastFocusedOption = focusedOption
        this.chart.setSelection(YtNetworks.createChannelHighlight(focusedOption.value))
      }
    })
  }

  onSelected = (c: Option) => {
    this.chart.setSelection(YtNetworks.createChannelFilter(c.value))
  }

  render() {
    let channelId = this.chart.filteredItems(YtNetworks.ChannelIdPath).find(() => true)
    let options = _(this.props.dataSet.channels)
      .map(c => ({ value: c.ChannelId, label: c.Title } as Option))
      .orderBy(o => o.label)
      .value()

    let selectedVlaue = options.find(c => c.value == channelId)

    return (
      <div onMouseMove={this.onUserInteracted} onClick={this.onUserInteracted}>
        <Select<Option>
          // value={this.state.selectedOption}
          onKeyDown={this.onUserInteracted}
          onChange={this.onSelected}
          options={options}
          placeholder="Select Channel"
          styles={{
            option: (p, s) => ({ ...p, color: '#ccc', backgroundColor: s.isFocused ? '#666' : 'none' }),
            control: styles => ({ ...styles, backgroundColor: 'none', borderColor: '#333', outline: 'none' }),
            //menu: styles => ({ ...styles, backgroundColor: '#333' }),
            dropdownIndicator: styles => ({ ...styles, color: '#ccc' }),
            //indicatorSeparator: styles => ({}),
            //valueContainer: styles => ({ ...styles, color: '#ccc'}),
            //singleValue: styles => ({...styles, color:'#ccc'}),
            //input: styles => ({...styles, color:'#ccc'})
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
