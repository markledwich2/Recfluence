exports.onCreateWebpackConfig = ({ stage, actions }) => {
  if (stage == 'develop') {
    actions.setWebpackConfig({
      node: {
        fs: 'empty'
      },
      devtool: "eval-source-map"
    })
  }

  if (stage === "build-html") {
    /*
     * During the build step, `auth0-js` will break because it relies on
     * browser-specific APIs. Fortunately, we don’t need it during the build.
     * Using Webpack’s null loader, we’re able to effectively ignore `auth0-js`
     * during the build. (See `src/utils/auth.js` to see how we prevent this
     * from breaking the app.)
     */
    actions.setWebpackConfig({
      module: {
        rules: [
          {
            test: /auth0-js/,
            use: loaders.null(),
          },
        ],
      },
    })
  }
}

// // Implement the Gatsby API “onCreatePage”. This is
// // called after every page is created.
// exports.onCreatePage = async ({ page, actions }) => {
//     const { createPage } = actions
//     // Only update the `/app` page.
//     if (page.path.match(/^\/video/)) {
//         // page.matchPath is a special key that's used for matching pages
//         // with corresponding routes only on the client.
//         page.matchPath = "/video/*"
//         // Update the page.
//         createPage(page)
//     }
// }