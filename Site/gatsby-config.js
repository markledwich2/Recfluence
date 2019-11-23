let activeEnv =
  process.env.GATSBY_ACTIVE_ENV || process.env.NODE_ENV || "dev"
console.log(`Using environment config: '${activeEnv}'`)
require("dotenv").config({
  path: `.env.${activeEnv}`,
})

module.exports = {
    siteMetadata: {
      title: `Political YouTube`
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
    pathPrefix:process.env.PREFIX
  }
  