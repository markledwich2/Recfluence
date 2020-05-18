import { promises as fsp } from 'fs'
import { exec } from 'promisify-child-process'
import dateformat from 'dateformat'
import * as bunyan from 'bunyan'
import formatMs from 'humanize-duration'
import { performance } from 'perf_hooks'


/** Dataform's config to provide in .df-credentials.json */
export interface DataformSfCfg {
    accountId: string
    role?: string
    username: string
    password: string
    databaseName: string
    warehouse: string
}

/** Type from YtReader.Db.Snowflake.SnowflakeCfg */
export interface YtSfCfg {
    account: string
    creds: string
    warehouse: string
    db: string
    schema: string
}

export async function writeDataformCreds(sfCfg:YtSfCfg, path:string): Promise<void> {
    if(!sfCfg.creds)  throw new Error('no creds supplied')
    const [user, pass] = sfCfg.creds.split(':')
    const dfCfg:DataformSfCfg = {
        accountId: sfCfg.account,
        databaseName: sfCfg.db,
        warehouse: sfCfg.warehouse,
        username: user,
        password: pass,
        role: 'dataform'
    }
    await fsp.writeFile(`${path}/.df-credentials.json`, JSON.stringify(dfCfg))
}

function fDuration(from:number, to?:number) {
    return formatMs(to ?? performance.now() - from, {  maxDecimalPoints:1 })
}

export async function run(branch: string, repo: string, sfCfg: YtSfCfg, runArgs:string, log:bunyan) {
    const start = performance.now()
    const runId = dateformat(new Date(), 'yyyy-mm-dd_hh-mm-ss')
    const runPath = `./.run/${runId}`

    log = log.child({runId:runId})

    const exe = async (name:string, cmd:string) => {
        const execLog = log.child({cmdName:name})
        log.debug({cmd:cmd}, 'executing sub-process ')
        const task = exec(cmd, { cwd: runPath })
        task.stdout.on('data', (d:string) => execLog.debug({process:'dataform'}, d))
        const res = await task
        return res
    }


    await fsp.mkdir(runPath, { recursive: true })
    await exe('git clone', `git clone -b ${branch} ${repo} .`)
    await writeDataformCreds(sfCfg, runPath)
    
    await exe('npm i', `npm i`)

    const dfCmd = `dataform run ${runArgs ?? ''}`
    log.info({cmd: dfCmd, dir:runPath}, 'dataform update %s - starting > %s', runId, dfCmd)
    const res = await exe('dataform run', dfCmd)
    
    log.info({cmd: dfCmd, dir:runPath, stdout:res.stdout}, 'dataform update - complete in %s', fDuration(start))
}

