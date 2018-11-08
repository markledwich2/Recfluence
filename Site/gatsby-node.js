exports.onCreateWebpackConfig = ({ stage, actions }) => {
  actions.setWebpackConfig({
    node: {
      fs: 'empty'
    },
    devtool: "eval-source-map"
  })
}