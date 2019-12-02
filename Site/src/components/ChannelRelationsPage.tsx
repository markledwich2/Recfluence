import * as React from 'react'
import ContainerDimensions from 'react-container-dimensions'
import { RecFlows } from './RecFlows'
import { ChannelRelations } from './ChannelRelations'
import { YtModel } from '../common/YtModel'
import { ChannelTitle } from './ChannelTitle'
import '../styles/Main.css'
import { InteractiveDataProps, SelectionStateHelper, InteractiveDataState, ActionType, Action } from '../common/Chart'
import _ from 'lodash'
import { toRecord } from '../common/Utils'

interface Props {
  dataUrl: string
}

interface State {
  model?: YtModel
}

export class ChannelRelationsPage extends React.Component<Props, State> {
  selections: SelectionStateHelper<any, any>
  relations: ChannelRelations
  flows: RecFlows
  title: ChannelTitle
  state: Readonly<State> = {
    model: null
  }

  static source = 'page'

  constructor(props: any) {
    super(props)
    this.selections = new SelectionStateHelper<any, any>(() => this.state.model.selectionState, this.onSelection, ChannelRelationsPage.source)
  }

  componentDidMount() {
     
    this.load()
  }

  resultUrl() { return `${this.props.dataUrl}/${YtModel.version}/latest/` }

  async load() {
    let data = await YtModel.dataSet(this.resultUrl())

    const params = new URLSearchParams(location.search)
    if (Array.from(params).length > 0) {
      var selectRecord:Record<string, any> = {}
      params.forEach((v,k) => selectRecord[k] = v)
      let sh = new SelectionStateHelper(() => data.selectionState)
      sh.select(selectRecord)
    }

    try {
      this.setState({ model: data })
    } catch (e) { }


  }

  onSelection = (action: Action) => {
    const params = new URLSearchParams(location.search)
    const updateUrl = () => history.replaceState({}, '', `${location.pathname}?${params}`)

    if (action.type == ActionType.clear) {
      // in the future, if this page use params for anything else we will need to determine which is a selection
      params.forEach((_,k) => params.delete(k))
      updateUrl()
    }

    if (action.type == ActionType.select) {
      params.forEach((_,k) => params.delete(k))
      if(action.select.length == 1) {
        const rec = action.select[0].record
        for(let k in rec)
          params.append(k, rec[k])
      }
      updateUrl()
    }

    console.log('onSelection', action)

    if(this.state.model) {
      this.state.model.selectionState = this.selections.applyAction(action)
      this.graphComponents().forEach(g => {
        const selections = this.state.model.selectionState
        return g.setState({ selections })
      })
    }
  }

  graphComponents(): Array<React.Component<InteractiveDataProps<YtModel>, InteractiveDataState>> {
    return [this.relations, this.flows, this.title].filter(g => g)
  }

  render() {
    if (this.state.model) {
      return (
        <div className={'ChannelRelationPage'}>
          <ChannelTitle
            ref={r => (this.title = r)}
            model={this.state.model}
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
                    model={this.state.model}
                    onSelection={this.onSelection.bind(this)}
                  />
                )}
              </ContainerDimensions>
            </div>
            <div className={'Flows'}>
              <ContainerDimensions>
                {({ height, width }) => (
                  <RecFlows
                    ref={r => (this.flows = r)}
                    height={height}
                    width={width}
                    model={this.state.model}
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
          <img src='spinner.svg'></img>
        </div>
      )
    }
  }
}
