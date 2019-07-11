import * as React from 'react'
import { ChannelRelationsPage } from '../components/ChannelRelationsPage';
import { TopicsPage } from '../components/TopicsPage';
import Helmet from 'react-helmet';

const App = () => (
  <>
  <Helmet>
    <title>Political YouTube</title>
  </Helmet>
  <ChannelRelationsPage /> 
  {/* <TopicsPage /> */}
  </>
);

export default App;