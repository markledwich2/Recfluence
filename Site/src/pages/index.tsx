import * as React from 'react'
import { Link } from 'gatsby'
import { ChannelRelations } from '../components/ChannelRelations'
import { RecommendFlows } from '../components/RecommendFlows'
import ContainerDimensions from 'react-container-dimensions'

export default () => (
  <div>
    <div style={{ height: '70vh', width: '100%' }}>
      <ContainerDimensions>
        {({ height, width }) => <ChannelRelations height={height} width={width} dataPath="data/" />}
      </ContainerDimensions>
    </div>

    <div style={{ height: '80vh', width: '100%' }}>

      <ContainerDimensions>
        {({ height, width }) => <RecommendFlows height={height} width={width} dataPath="data/" />}
      </ContainerDimensions>
    </div>
  </div>
)
