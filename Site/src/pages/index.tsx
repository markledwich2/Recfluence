import * as React from "react";
import { Link } from "gatsby";
import { ChannelRelations } from "../components/ChannelRelations";
import ContainerDimensions from "react-container-dimensions";

export default () => (

  <div style={{ height: "100vh", width: "100%" }}>
    <ContainerDimensions>
      { ({ height, width }) => <ChannelRelations height={height} width={width} dataPath="data/"/> }
    </ContainerDimensions>
  </div>
);
