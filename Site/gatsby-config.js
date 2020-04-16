// let activeEnv =
//   process.env.GATSBY_ACTIVE_ENV || process.env.NODE_ENV || "development"
// console.log(`Using environment: '${activeEnv}'`)

// require('dotenv').config({
//   path: `.env.${activeEnv}`,
// })

// if (process.env.GATSBY_PATH_PREFIX) // this is for building a beta version that you can upload to a subdirectory
//   console.log(`GATSBY_PATH_PREFIX: '${process.env.GATSBY_PATH_PREFIX}'`)

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
  //pathPrefix: process.env.GATSBY_PATH_PREFIX
}
