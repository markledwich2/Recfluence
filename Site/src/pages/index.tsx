import * as React from 'react'
import { ChannelRelationsPage } from '../components/ChannelRelationsPage';
import Helmet from 'react-helmet';

const App = () => (
  <>
  <Helmet>
    <title>Political YouTube</title>
  </Helmet>
  <ChannelRelationsPage />
  </>
);

export default App;