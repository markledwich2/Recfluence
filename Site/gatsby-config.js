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
    ]
  }
  