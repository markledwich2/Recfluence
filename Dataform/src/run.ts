
import { YtSfCfg, writeDataformCreds, run } from './Dataform'
import { config as dotenvCfg } from 'dotenv'
import * as bunyan from 'bunyan'
import * as seq from 'bunyan-seq'
import { LoggerOptions } from 'bunyan'
import bunyanDebugStream from 'bunyan-debug-stream'

dotenvCfg()
const env = process.env
const logCfg: LoggerOptions = {
    name: 'recfluence_dataform',
    streams: [{
        level: 'debug',
        type: 'raw',
        stream: bunyanDebugStream({
            basepath: __dirname, // this should be the root folder of your project.
            forceColor: true
        })
    }],
    serializers: bunyanDebugStream.serializers
}
const seqUrl = env.SEQ
if (seqUrl)
    logCfg.streams.push(
        seq.createStream({
            serverUrl: seqUrl,
            level: 'debug'
        }))

var log = bunyan.createLogger(logCfg)
log.info('recfluence dataform container started')

const sfJson = process.env.SNOWFLAKE_JSON
if (!sfJson) throw new Error('no snowflake connection details provided in env:SNOWFLAKE_JSON')
const sfCfg: YtSfCfg = JSON.parse(sfJson)
const repo = process.env.REPO
if (!repo) throw new Error('no dataform repo provided env:REPO')
const branch = process.env.BRANCH ?? 'master'

const dfRunArgs = process.env.DATAFORM_RUN_ARGS;

(async () => {
    // get latest code & configure dataform settings
    await run(branch, repo, sfCfg, dfRunArgs, log)

})().catch((e: any) => {
    if (e instanceof Error)
        log.error(e, e.message)
    else
        log.error(e)
    throw e
})
