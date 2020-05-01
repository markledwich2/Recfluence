
const regex = /\w+|"[^"]+"/g

export function queryHighlights(query: string): string[] {
    try {

        //const matches = query.split(/^(\"[^\"]\")/)
        //return deepTerms(parse(query))
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

interface EsQuery {
    query?: {
        match?: { [x: string]: any }
        match_none?: {}
    }
    sort?: {
        [field: string]: { order: 'asc' | 'desc' }
    }
}