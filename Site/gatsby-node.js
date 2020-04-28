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