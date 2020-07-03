declare namespace NodeJS {

    // Merge the existing `ProcessEnv` definition with ours
    // https://www.typescriptlang.org/docs/handbook/declaration-merging.html#merging-interfaces
    export interface ProcessEnv {
        SNOWFLAKE_JSON: string
        REPO: string
        // this is the repo branch (not the branch environment)
        BRANCH: string
        DATAFORM_RUN_ARGS: string
        SEQ:string
    }
}