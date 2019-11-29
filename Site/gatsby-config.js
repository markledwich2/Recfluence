let activeEnv =
  process.env.GATSBY_ACTIVE_ENV || process.env.NODE_ENV || "development"
console.log(`Using environment: '${activeEnv}'`)

require('dotenv').config({
  path: `.env.${activeEnv}`,
})

if(process.env.GATSBY_PATH_PREFIX) // this is for building a beta version that you can upload to a subdirectory
  console.log(`GATSBY_PATH_PREFIX: '${process.env.GATSBY_PATH_PREFIX}'`)

module.exports = {
    siteMetadata: {
      title: `Recfluence`,
      dataUrl: process.env.RESULTS_URL || `https://pyt-data.azureedge.net/data/results`
    },
    plugins: [ 
      `gatsby-plugin-typescript`,
      `gatsby-plugin-react-helmet`,
      {
        resolve: `gatsby-plugin-google-analytics`,
        options: {
          trackingId: "UA-130770302-1",
          head: true,
        }
      }
    ],
    pathPrefix:process.env.GATSBY_PATH_PREFIX
  }
  