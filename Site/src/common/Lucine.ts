import { parse, LuceneTerm, LuceneLeftRight } from 'lucene-query-parser'

const deepTerms = (part: LuceneTerm | LuceneLeftRight): LuceneTerm[] => {
    if ('term' in part)
        return [part]
    return (part.left ? deepTerms(part.left) : [])
        .concat(part.right ? deepTerms(part.right) : [])
}

export function luceneTerms(query: string): LuceneTerm[] {
    try {
        return deepTerms(parse(query))
    }
    catch (error) {
        console.log(`error parsing query ${query}: ${error}`)
        return []
    }
}

console.log(luceneTerms('title:"The Right Way" AND text:go -nothis'))