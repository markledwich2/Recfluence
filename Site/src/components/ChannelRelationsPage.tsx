import * as React from 'react'
import ContainerDimensions from 'react-container-dimensions'
import { RecommendFlows } from './RecommendFlows'
import { ChannelRelations } from './ChannelRelations'
import { YtData, YtNetworks } from '../common/YtData'
import { GridLoader } from 'react-spinners'
import { DataSelections, DataSelection, SelectionType, ChartProps, InteractiveDataState, InteractiveDataProps } from '../common/Charts'
import { jsonClone } from '../common/Utils'
import { ChannelTitle } from './ChannelTitle'
import '../styles/Main.css'
import { Mention } from 'react-twitter-widgets'
import { ChannelWords } from './ChannelWords';

interface Props { }

interface State {
  isLoading: boolean
  data?: YtData
}

export class ChannelRelationsPage extends React.Component<Props, State> {
  constructor(props: any) {
    super(props)

    this.selections = new DataSelections()
  }

  state: Readonly<State> = {
    isLoading: false,
    data: null
  }

  selections: DataSelections
  relations: ChannelRelations
  flows: RecommendFlows
  title: ChannelTitle
  words: ChannelWords

  componentDidMount() {
    const params = new URLSearchParams(location.search)
    if (params.has('c'))
      this.selections.filters.push({ path: YtNetworks.ChannelIdPath, values: [params.get('c')], type: SelectionType.Filter })
    if (params.has('v'))
      this.version = params.get('v')
    
    this.load()
  }

  version:string = '2019-04-04'
  //resultUrl() { return `https://ytnetworks.azureedge.net/data/results/${this.version}/` }

  resultUrl() { return `` }

  async load() {
    if (this.state.isLoading) return
    let data = await YtNetworks.dataSet(this.resultUrl())
    try {
      this.setState({ data, isLoading: false })
    } catch (e) { }
  }

  onSelection(selection: DataSelection) {
    this.selections.setSelection(selection)
    //let hist = createBrowserHistory()
    const params = new URLSearchParams(location.search)
    if ((selection.type == SelectionType.Filter && selection.path == YtNetworks.ChannelIdPath) || selection.path == null) {
      let channelId = selection.path == null ? null : selection.values.find(() => true)
      if (params.has('c')) params.delete('c')
      if (channelId) params.append('c', channelId)

      history.replaceState(null, '', `?${params}`)
    }

    this.updateComponentSelections()
  }

  private updateComponentSelections() {
    let components = this.graphComponents()
    components.forEach(g => g.setState({ selections: jsonClone(this.selections) }))
  }

  graphComponents(): Array<React.Component<InteractiveDataProps<YtData>, InteractiveDataState>> {
    return [this.relations, this.flows, this.title, this.words].filter(g => g)
  }

  render() {
    if (this.state.data) {
      return (
        <div className={'ChannelRelationPage'}>
          <ChannelTitle
            ref={r => (this.title = r)}
            dataSet={this.state.data}
            onSelection={this.onSelection.bind(this)}
            initialSelection={this.selections}
          />

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
                    initialSelection={this.selections}
                  />
                )}
              </ContainerDimensions>
            </div>
            <div className={'Words'}>
              <ContainerDimensions>
                {({ height, width }) => (
                  <ChannelWords
                    ref={r => (this.words = r)}
                    height={height}
                    width={width}
                    dataSet={this.state.data}
                    onSelection={this.onSelection.bind(this)}
                    initialSelection={this.selections}
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
                    initialSelection={this.selections}
                  />
                )}
              </ContainerDimensions>
            </div>
          </div>
          <div className={'footer'}>
            <a href={'https://twitter.com/mark_ledwich'}>@mark_ledwich</a>
            <a href={'mailto:mark@ledwich.com.au?Subject=Political YouTube'}>mark@ledwich.com.au</a>
            <span>
              download &nbsp;
            <a href={this.resultUrl + 'VisChannels.csv'}>Channel</a>
              &nbsp; and &nbsp;
                <a href={this.resultUrl + 'VisRelations.csv'}>Relation</a>
              &nbsp;data
            </span>
            <span>See &nbsp;<a href={'https://github.com/markledwich2/YouTubeNetworks'}>project source</a> &nbsp;</span>
          </div>
        </div >
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
