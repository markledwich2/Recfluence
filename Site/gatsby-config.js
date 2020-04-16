
//require('dotenv')
// const getBranch = require('current-git-branch')
// if (process.env.BRANCH)
//   console.log(`branch '${process.env.BRANCH}' specified from BRANCH environment variable`)
// const branch = process.env.BRANCH || getBranch()
// const assetPrefix = (!branch || branch == "master") ? null : '/branch/' + branch

// console.log(`assetPrefix: '${assetPrefix}'`)

module.exports = {
  siteMetadata: {
    title: `Recfluence`
    // dataUrl: process.env.RESULTS_URL || `https://pyt-data.azureedge.net/data/results`,
    // funcUrl: process.env.FUNC_URL || `https://recfluence.azurewebsites.net`
  },
  plugins: [
    `gatsby-plugin-react-helmet`,
    `gatsby-plugin-styled-components`,
    {
      resolve: `gatsby-plugin-typescript`,
      options: {
        isTSX: true, // defaults to false
        allExtensions: true, // defaults to false
      },
    },
    {
      resolve: `gatsby-plugin-google-analytics`,
      options: {
        trackingId: "UA-130770302-1",
        head: true,
      }
    },
    {
      resolve: `gatsby-plugin-create-client-paths`,
      options: { prefixes: [`/video/*`] },
    }
  ]
}