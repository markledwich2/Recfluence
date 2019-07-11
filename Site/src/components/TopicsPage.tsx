import * as React from 'react'
import { RelatiosData, YtNetworks } from '../common/YtData'
import '../styles/Main.css'

interface Props { }

interface State {
  data?: RelatiosData
}

export class TopicsPage extends React.Component<Props, State> {
    constructor(props: any) {
      super(props)
    }
  
    state: Readonly<State> = {
      data: null
    }

    async componentDidMount() {
        let data = await YtNetworks.dataSet('')

    }

    render() {
        if(this.state.data.channels) {
            
        }
    }
}