
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
  return { url: process.env.ES_URL, creds: process.env.ES_CREDS }
}

export interface EsCfg {
  url: string
  creds: string
}