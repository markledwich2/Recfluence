import { promises as fsp } from 'fs'
import { exec } from 'promisify-child-process'
import dateformat from 'dateformat'
import * as bunyan from 'bunyan'
import formatMs from 'humanize-duration'
import { performance } from 'perf_hooks'
import stripAnsi from 'strip-ansi'
import _ from 'lodash'

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
    host: string
    creds: string
    warehouse?: string
    db?: string
    schema?: string
    role?: string
}

export async function writeDataformCreds(sfCfg: YtSfCfg, path: string): Promise<void> {
    if (!sfCfg.creds) throw new Error('no creds supplied')
    const [user, pass] = sfCfg.creds.split(':')

    const dfCfg: DataformSfCfg = {
        accountId: _(sfCfg.host.split('.')).takeWhile(h => h.toLowerCase() != 'snowflakecomputing').join('.'),
        databaseName: sfCfg.db,
        warehouse: sfCfg.warehouse,
        username: user,
        password: pass,
        role: sfCfg.role
    }
    await fsp.writeFile(`${path}/.df-credentials.json`, JSON.stringify(dfCfg))
}

function fDuration(from: number, to?: number) {
    return formatMs(to ?? performance.now() - from, { maxDecimalPoints: 1 })
}

function tryParseJSON(jsonString) {
    try {
        var o = JSON.parse(jsonString)

        // Handle non-exception-throwing cases:
        // Neither JSON.parse(false) or JSON.parse(1234) throw errors, hence the type-checking,
        // but... JSON.parse(null) returns null, and typeof null === "object", 
        // so we must check for that, too. Thankfully, null is falsey, so this suffices:
        if (o && typeof o === "object") {
            return o
        }
    }
    catch (e) { }
    return false
};

interface DataformLogLine {
    message: string,
    level: 'TRACE' | 'INFO' | 'DEBUG'
}

export async function run(branch: string, repo: string, sfCfg: YtSfCfg, runArgs: string, log: bunyan) {
    const start = performance.now()
    const runId = dateformat(new Date(), 'yyyy-mm-dd_hh-mm-ss')
    const runPath = `./.run/${runId}`

    log = log.child({ runId: runId })

    const exe = async (name: string, cmd: string) => {
        const execLog = log.child({ cmdName: name })
        log.debug({ cmd: cmd }, 'executing sub-process ')
        const task = exec(cmd, { cwd: runPath, })
        task.stdout.on('data', (d: string) => {
            const msg: DataformLogLine | false = tryParseJSON(d)
            if (!msg || !['TRACE', 'DEBUG'].includes(msg.level))
                execLog.info(stripAnsi(d))
        })
        const res = await task
        return res
    }

    await fsp.mkdir(runPath, { recursive: true })
    await exe('git clone', `git clone -b ${branch} ${repo} .`)
    await writeDataformCreds(sfCfg, runPath)

    await exe('npm i', `npm i`)

    const dfCmd = `dataform run ${runArgs ?? ''}`
    log.info({ cmd: dfCmd, dir: runPath }, 'dataform update %s - starting > %s', runId, dfCmd)
    const res = await exe('dataform run', dfCmd)
    const stdout = (res.stdout instanceof Buffer) ? "(buffer)" : stripAnsi(res.stdout)
    log.info({ cmd: dfCmd, dir: runPath, stdout }, 'dataform update - complete in %s', fDuration(start))
}

