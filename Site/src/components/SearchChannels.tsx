import * as React from 'react'
import Select from 'react-select'
import * as _ from 'lodash'
import { InteractiveDataProps, InteractiveDataState, DataComponentHelper, DataSelections, ChartProps } from '../ts/Charts'
import { YtNetworks, Graph, YtData } from '../ts/YtData'
import { delay } from '../ts/Utils';

interface State extends InteractiveDataState {
}
interface Props extends InteractiveDataProps<YtData> {}

interface Option {
  value: string
  label: string
}



export class SearchChannels extends React.Component<Props, State> {
  chart: DataComponentHelper = new DataComponentHelper(this)

  ref: Select<Option>
  lastFocusedOption: Option

  shouldComponentUpdate(prevProps: ChartProps<YtData>, prevState: State) {
    return false;
  }

  onUserInteracted = () => {
    let r = this.ref as any
    const focusedOption = r.select.state.focusedOption
    
    if (this.lastFocusedOption !== focusedOption && r.state.menuIsOpen) {
      this.lastFocusedOption = focusedOption
      this.chart.setSelection(YtNetworks.createChannelHighlight(focusedOption.value))
    }
  }

  render() {
    //let channelId = this.chart.filteredItems(YtNetworks.ChannelIdPath).find(() => true)
    let options = _(this.props.dataSet.channels)
      .map(c => ({ value: c.ChannelId, label: c.Title } as Option))
      .value()
    //todo selected option for current channelId

    return (
      <div onMouseMove={this.onUserInteracted} onClick={this.onUserInteracted}>
        <Select<Option>
          // value={this.state.selectedOption}
          onKeyDown={this.onUserInteracted}
          onChange={(c: Option) => this.chart.setSelection(YtNetworks.createChannelFilter(c.value))}
          options={options}
          placeholder="Channel"
          styles={{
            option: (p, s) => ({ ...p, color: '#ccc', backgroundColor: s.isFocused ? '#666' : 'none' }),
            control: styles => ({ ...styles, backgroundColor: 'none', borderColor: '#444', outline: 'none' }),
            // menu: styles => ({ ...styles, backgroundColor: '#333' }),
            dropdownIndicator: styles => ({ ...styles, color: '#666' }),
            indicatorSeparator: styles => ({})
            //valueContainer: styles => ({}),
            // placeholder: styles => ({ ...styles, outline: 'none' })
          }}
          theme={theme => ({
            ...theme,
            colors: {
              ...theme.colors,
              text: '#ccc',
              primary: '#666',
              neutral0: '#333'
            }
          })}
          ref={ref => (this.ref = ref)}
        />
      </div>
    )
  }
}
