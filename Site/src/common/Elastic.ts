
const regex = /\w+|"[^"]+"/g

export function queryHighlights(query: string): string[] {
  try {
    const matches = query.match(regex)
    var i = matches.length
    while (i--) matches[i] = matches[i].replace(/"/g, "")
    return matches
  }
  catch (error) {
    console.log(`error parsing query ${query}: ${error}`)
    return []
  }
}

export function esCfgFromEnv(): EsCfg {
  const prefix = process.env.BRANCH_ENV ? `${process.env.BRANCH_ENV}-` : ''
  return {
    url: process.env.ES_URL,
    creds: process.env.ES_CREDS,
    prefix: process.env.ES_PREFIX,
    indexes: {
      caption: `${prefix}caption`,
      channel: `${prefix}channel`,
      video: `${prefix}video`
    }
  }
}

export interface EsCfg {
  url: string
  creds: string
  prefix: string
  indexes: EsIndexes
}

export interface EsIndexes {
  caption: string
  video: string
  channel: string
}