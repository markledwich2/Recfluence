import * as React from 'react'
import ContainerDimensions from 'react-container-dimensions'
import { RecommendFlows } from './RecommendFlows'
import { ChannelRelations } from './ChannelRelations'
import { YtData, YtNetworks } from '../ts/YtData'
import { GridLoader } from 'react-spinners'
import { DataSelections, DataSelection, ChartProps, InteractiveDataState, InteractiveDataProps } from '../ts/Charts'
import { ChannelTitle } from './ChannelTitle'
import '../styles/Main.css'

interface Props {
  dataPath: string
}

interface State {
  isLoading: boolean
  data?: YtData
}

export class ChannelRelationsPage extends React.Component<Props, State> {
  constructor(props: Props) {
    super(props)
  }

  state: Readonly<State> = {
    isLoading: false,
    data: null
  }
  componentDidMount() {
    this.load()
  }

  componentWillUnmount() {}

  async load() {
    if (this.state.isLoading) return
    let data = await YtNetworks.dataSet(this.props.dataPath)
    try {
      this.setState({ data, isLoading: false })
    } catch (e) {}
  }

  selections: DataSelections = new DataSelections()

  onSelection(selection: DataSelection) {
    this.selections.setSelection(selection)
    this.graphComponents()
      .filter(g => g)
      .forEach(g => g.setState({ selections: this.selections }))
  }

  relations: ChannelRelations
  flows: RecommendFlows
  title: ChannelTitle

  graphComponents(): Array<React.Component<InteractiveDataProps<YtData>, InteractiveDataState>> {
    return [this.relations, this.flows, this.title]
  }

  render() {
    if (this.state.data) {
      return (
        <div className={'ChannelRelationPage'}>
          <ChannelTitle ref={r => (this.title = r)} dataSet={this.state.data} onSelection={this.onSelection.bind(this)} />

          <div className={'MainChartContainer'}>
            <div className={'Relations'}>
              <ContainerDimensions>
                {({ height, width }) => (
                  <ChannelRelations
                    ref={r => (this.relations = r)}
                    height={height}
                    width={width}
                    dataSet={this.state.data}
                    onSelection={this.onSelection.bind(this)}
                  />
                )}
              </ContainerDimensions>
            </div>
            <div className={'Flows'}>
              <ContainerDimensions>
                {({ height, width }) => (
                  <RecommendFlows
                    ref={r => (this.flows = r)}
                    height={height}
                    width={width}
                    dataSet={this.state.data}
                    onSelection={this.onSelection.bind(this)}
                  />
                )}
              </ContainerDimensions>
            </div>
          </div>
        </div>
      )
    } else {
      return (
        <div style={{ position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%,-50%)' }}>
          <GridLoader color="#3D5467" size={30} />
        </div>
      )
    }
  }
}
