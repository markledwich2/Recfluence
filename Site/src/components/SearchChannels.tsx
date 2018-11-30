import * as React from 'react'
import Select from 'react-select'
import * as _ from 'lodash'
import { InteractiveDataProps, InteractiveDataState, DataComponentHelper, DataSelections, ChartProps } from '../common/Charts'
import { YtNetworks, Graph, YtData } from '../common/YtData'
import { delay } from '../common/Utils';

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
    selections: this.props.initialSelection
  }
  ref: Select<Option>
  lastFocusedOption: Option

   shouldComponentUpdate(prevProps: ChartProps<YtData>, prevState: State) {
      return false;
   }
  

  // componentDidUpdate() {
  //   var channelId = this.chart.filteredItems(YtNetworks.ChannelIdPath).find(c => true);
  //   let r = this.ref as any
  //   if (channelId)
  //     r.select.setValue(channelId)
  //   else
  //     r.select.setValue('')

  //   return false;
  // }

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

  render() {
    //let channelId = this.chart.filteredItems(YtNetworks.ChannelIdPath).find(() => true)
    let options = _(this.props.dataSet.channels)
      .map(c => ({ value: c.ChannelId, label: c.Title } as Option))
      .orderBy(o => o.label)
      .value()

    let channelid = this.chart.filteredItems(YtNetworks.ChannelIdPath).find(c => true), label:''

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
            //menu: styles => ({ ...styles, backgroundColor: '#333' }),
            dropdownIndicator: styles => ({ ...styles, color: '#ccc' }),
            //indicatorSeparator: styles => ({}),
            //valueContainer: styles => ({ ...styles, color: '#ccc'}),
            //singleValue: styles => ({...styles, color:'#ccc'}),
            //input: styles => ({...styles, color:'#ccc'})
            //placeholder: styles => ({ ...styles, outline: 'none', color:'#ccc' })
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
          
          value={channelid as any}
          ref={(ref:any) => (this.ref = ref)}
        />
      </div>
    )
  }
}
