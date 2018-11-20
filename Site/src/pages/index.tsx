import * as React from 'react'
import { ChannelRelationsPage } from '../components/ChannelRelationsPage';
import { BrowserRouter as Router, Route, Link, Switch } from "react-router-dom";

const AppRouter = () => (
  <Router>
    <Switch>
      <Route exact path="/" component={ChannelRelationsPage} />
    </Switch>
  </Router>
);

export default AppRouter;