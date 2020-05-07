import * as React from 'react'
import ContainerDimensions from 'react-container-dimensions'
import { RecFlows } from './RecFlows'
import { ChannelRelations } from './ChannelRelations'
import { YtModel } from '../../common/YtModel'
import { ChannelRelationsTitle } from './ChannelRelationsTitle'
import { InteractiveDataProps, SelectionStateHelper, InteractiveDataState, ActionType, Action } from '../../common/Chart'
import _ from 'lodash'
import { RouteComponentProps } from "@reach/router"
import styled from 'styled-components'
import { media } from '../MainLayout'

interface Props extends RouteComponentProps {
  dataUrl: string
}

interface State {
  model?: YtModel
}

const ChannelRelationsStyles = styled.div`
  font-size: 11px;
  @media (${media.width.medium}) {
    font-size: 14px;
  }

  height:100vh;
    
  /* Title */
  div.title-details > div {
    margin-bottom: 1px;
  }

  g.chart path.link {
    opacity: 0.4;
    fill: none;
  }

  g.chart text {
    text-shadow: -1px 1 black, 1 1px black, 1px 1 black, 0 -1px black;
    fill: #eee;
  }

  g.chart text tspan.subtitle {
    fill: #aaa;
  }

  g.chart text tspan.subtitle-bold {
    fill: #ccc;
  }

  /* common chart shapes */
  g.chart g.node .selectable {
    stroke: none;
  }

  g.chart g.node .selectable.dimmed {
    opacity: 0.5;
  }

  g.chart text.label.selected {
    font-weight: bold;
  }

  /* relations chart */
  g.chart.relations g.node circle.related {
    opacity: 1;
  }

  g.chart.relations g.labels rect,
  g.chart.relations g.labels circle {
    display: none;
  }

  g.chart.relations line.link {
    display: none;
  }
  g.chart.relations line.link.related {
    display: inherit;
  }

  g.chart.relations g.label text {
    display: none;
    fill: white;
  }

  g.chart.relations g.label text.selected,
  g.chart.relations g.label text.highlighted,
  g.chart.relations g.label text.related {
    display: inherit;
  }

  g.chart.relations g.label text.related {
    font-size: 0.9em;
    fill-opacity: 0.8;
  }

  g.chart.relations g.label text.highlighted,
  g.chart.relations g.label text.selected {
    font-size: 1em;
    fill-opacity: 1;
  }

  /* flows chart */
  g.chart.flows g.node .selectable.dimmed {
    opacity: 1; /* remove dimming effect */
  }

  /* Relations page layout */

  div.ChannelRelationPage svg.chart {
    position: absolute;
  }

  div.ChannelRelationPage {
    width: 100%;
  }

  div.ChannelRelationPage > * {
    padding: 5px;
  }

  div.footer {
    display: flex;
    flex-flow: wrap;
    justify-content: left;
  }

  div.footer > * {
    padding-right: 2em;
  }

  @media all and (min-width: 1200px) {

    div.footer {
      position: absolute;
      bottom: 1px;
      margin: 5px;
    }
  }
`

const MainChartStyled = styled.div`
  > * {
    padding: 0px;
  }

  display: flex;
  flex-flow: row wrap;
  align-items: stretch;
  flex: 1;

  height:none;
  > .Relations {
    height: 50vh;
    min-height: 400px;
    flex: 1 100%;
  }

  > .Flows {
    position: relative;
    height: 50vh;
    min-height: 800px;
    flex: 1 100%;
  }

  
  @media all and (min-width: 1200px) {
    height: 100%;

    > .Relations {
      height: 100%;
      flex: 2 auto;
    }

    > .Flows {
      position: absolute;
      right: 5px;
      width: 30%;
      min-width: 500px;
      height: 80vh;
      background-color: rgba(0, 0, 0, 0.5);
    }
  }
`

export class ChannelRelationsPage extends React.Component<Props, State> {
  selections: SelectionStateHelper<any, any>
  relations: ChannelRelations
  flows: RecFlows
  title: ChannelRelationsTitle
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
      var selectRecord: Record<string, any> = {}
      params.forEach((v, k) => selectRecord[k] = v)
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
      params.forEach((_, k) => params.delete(k))
      updateUrl()
    }

    if (action.type == ActionType.select) {
      params.forEach((_, k) => params.delete(k))
      if (action.select.length == 1) {
        const rec = action.select[0].record
        for (let k in rec)
          params.append(k, rec[k])
      }
      updateUrl()
    }

    console.log('onSelection', action)

    if (this.state.model) {
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
        <ChannelRelationsStyles>
          <ChannelRelationsTitle
            ref={r => (this.title = r)}
            model={this.state.model}
            onSelection={this.onSelection.bind(this)}
          />

          <MainChartStyled>
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
          </MainChartStyled>
          <div className={'footer'}>
            <a href={'https://twitter.com/mark_ledwich'}>@mark_ledwich</a>
            <a href={'mailto:mark@ledwich.com.au?Subject=Political YouTube'}>mark@ledwich.com.au</a>
            <span> &nbsp;<a href={'https://github.com/markledwich2/YouTubeNetworks'}>GitHub project</a> &nbsp;</span>
          </div>
        </ChannelRelationsStyles>
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
