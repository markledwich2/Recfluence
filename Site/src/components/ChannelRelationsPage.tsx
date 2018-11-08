import * as React from 'react'
import ContainerDimensions from 'react-container-dimensions'
import { RecommendFlows } from './RecommendFlows'
import { ChannelRelations } from './ChannelRelations'
import { YtData, YtNetworks } from '../ts/YtData'
import { GridLoader } from 'react-spinners'
import { DataSelections, DataSelection, ChartProps, ChartState } from '../ts/Charts'

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
    console.log('componentDidMount')
    this.load()
  }

  componentWillUnmount() {
    console.log('componentWillMount')
  }

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

    this.graphComponents().forEach(g => g.setState({ selections: this.selections }))
  }

  relations: ChannelRelations
  flows: RecommendFlows
  graphComponents(): Array<React.Component<ChartProps<YtData>, ChartState>> {
    return [this.relations, this.flows]
  }

  render() {
    return this.state.data ? (
      <div>
        <div style={{ height: '70vh', width: '100%' }}>
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
        <div style={{ height: '800px', width: '100%' }}>
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
    ) : (
      <div style={{ position: 'absolute', top: '50%', left: '50%', transform: 'translate(-50%,-50%)' }}>
        <GridLoader color="#3D5467" size={30} />
      </div>
    )
  }
}
