let activeEnv =
  process.env.GATSBY_ACTIVE_ENV || process.env.NODE_ENV || "development"
console.log(`Using environment: '${activeEnv}'`)
console.log(`GATSBY_PATH_PREFIX: '${process.env.GATSBY_PATH_PREFIX}'`)

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
    pathPrefix:process.env.GATSBY_PATH_PREFIX
  }
  