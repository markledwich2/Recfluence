import * as React from 'react'
import ContainerDimensions from 'react-container-dimensions'
import { RecommendFlows } from './RecommendFlows'
import { ChannelRelations } from './ChannelRelations'
import { YtModel } from '../common/YtModel'
import { GridLoader } from 'react-spinners'
import { jsonClone } from '../common/Utils'
import { ChannelTitle } from './ChannelTitle'
import '../styles/Main.css'
import { InteractiveDataProps, SelectionStateHelper, InteractiveDataState, ActionType, Action } from '../common/Chart'

interface Props { }

interface State {
  isLoading: boolean
  data?: YtModel
}

export class ChannelRelationsPage extends React.Component<Props, State> {
  selections: SelectionStateHelper<any, any>
  relations: ChannelRelations
  flows: RecommendFlows
  title: ChannelTitle
  state: Readonly<State> = {
    isLoading: false,
    data: null
  }

  constructor(props: any) {
    super(props)
    this.selections = new SelectionStateHelper<any, any>(this.onSelection, () => this.state.data.selectionState)
  }

  componentDidMount() {
    const params = new URLSearchParams(location.search)

    if (params.has('c'))
      this.selections.select(YtModel.channelDimStatic.col("channelId"), params.get('c'))
    if (params.has('v'))
      this.version = params.get('v')

      this.load()
  }

  version: string = '2019-11-18'
  resultsPath: string = `https://pyt.blob.core.windows.net/data/results/${this.version}/` //https://ytnetworks.azureedge.net/data/results/${this.version}/

  resultUrl() { return this.resultsPath }

  async load() {
    if (this.state.isLoading) return
    let data = await YtModel.dataSet(this.resultUrl())
    try {
      this.setState({ data, isLoading: false })
    } catch (e) { }
  }

  onSelection(action: Action) {
    const params = new URLSearchParams(location.search)
    const idAttribute = YtModel.channelDimStatic.col("channelId")

    if (action.type == ActionType.Clear && params.has('c'))
      params.delete('c')

    if (action.type == ActionType.Select) {
      let channelId = this.selections.selectedSingleValue(idAttribute)
      if(channelId) {
        if (params.has('c')) params.delete('c')
        if (channelId) params.append('c', channelId)
      }
    }

    this.state.data.selectionState = this.selections.applyAction(action)
    this.graphComponents().forEach(g => g.setState({ selections: jsonClone(this.state.data.selectionState) }))
  }

  graphComponents(): Array<React.Component<InteractiveDataProps<YtModel>, InteractiveDataState>> {
    return [this.relations, this.flows, this.title].filter(g => g)
  }

  render() {
    if (this.state.data) {
      return (
        <div className={'ChannelRelationPage'}>
          <ChannelTitle
            ref={r => (this.title = r)}
            model={this.state.data}
            onSelection={this.onSelection.bind(this)}
          />

          <div className={'MainChartContainer'}>
            <div className={'Relations'}>
              <ContainerDimensions>
                {({ height, width }) => (
                  <ChannelRelations
                    ref={r => (this.relations = r)}
                    height={height}
                    width={width}
                    model={this.state.data}
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
                    model={this.state.data}
                    onSelection={this.onSelection.bind(this)}
                  />
                )}
              </ContainerDimensions>
            </div>
          </div>
          <div className={'footer'}>
            <a href={'https://twitter.com/mark_ledwich'}>@mark_ledwich</a>
            <a href={'mailto:mark@ledwich.com.au?Subject=Political YouTube'}>mark@ledwich.com.au</a>
            <span> &nbsp;<a href={'https://github.com/markledwich2/YouTubeNetworks'}>GitHUb project</a> &nbsp;</span>
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
