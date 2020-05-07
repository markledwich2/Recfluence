exports.onCreateWebpackConfig = ({ stage, actions }) => {
  if (stage == 'develop') {
    actions.setWebpackConfig({
      node: {
        fs: 'empty'
      },
      devtool: "eval-source-map"
    })
  }
}