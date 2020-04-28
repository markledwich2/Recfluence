declare module 'lucene-query-parser' {
    export function parse(input: string, options?: any): LuceneTerm | LuceneLeftRight


    export interface LuceneLeftRight {
        left: LuceneTerm | LuceneLeftRight
        operator: string
        right: LuceneTerm | LuceneLeftRight
    }

    export interface LuceneTerm {
        field: string
        term: string
        similarity?: number
        boost?: number
        prefix?: string
    }
}
