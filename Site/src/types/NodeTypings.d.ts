declare namespace NodeJS {

    // Merge the existing `ProcessEnv` definition with ours
    // https://www.typescriptlang.org/docs/handbook/declaration-merging.html#merging-interfaces
    export interface ProcessEnv {
        NODE_ENV: "development" | "production"
        RESULTS_URL: string
        FUNC_URL: string
        AUTH0_DOMAIN: string
        AUTH0_CLIENTID: string
        ES_URL: string
        ES_CREDS: string
    }
}