
import { YtSfCfg, writeDataformCreds, run } from './Dataform'
import { config as dotenvCfg } from 'dotenv'
import * as bunyan from 'bunyan'
import * as seq from 'bunyan-seq'
import { LoggerOptions } from 'bunyan'
import bunyanDebugStream from 'bunyan-debug-stream'
import * as yargs from 'yargs'
import stripAnsi from 'strip-ansi'
import { delay } from 'lodash'

dotenvCfg()
const env = process.env

const argv = yargs.options({
    dataformArgs: {
        type: 'string',
        alias: 'd',
        default: <string>null,
        description: 'The raw dataform arguments to pass through (e.g. "--include-deps --tags standard")'
    }
}).argv
const dfRunArgs = (argv.dataformArgs ?? process.env.DATAFORM_RUN_ARGS)

const logCfg: LoggerOptions = {
    name: 'recfluence_dataform',
    streams: [{
        level: 'info',
        type: 'raw',
        stream: bunyanDebugStream({
            basepath: __dirname, // this should be the root folder of your project.
            forceColor: true,
            showProcess: false,
            showDate: false,
            showLevel: false,
            showMetadata: false
        })
    }],
    serializers: bunyanDebugStream.serializers
}
const seqUrl = env.SEQ
const seqStream = seqUrl ? seq.createStream({
    serverUrl: seqUrl,
    level: 'info',
    maxBatchingTime: 1000,
    onError: (e) => console.log('seq error', e)
}) : null
if (seqStream)
    logCfg.streams.push(seqStream)

const sfJson = process.env.SNOWFLAKE_JSON
if (!sfJson) throw new Error('no snowflake connection details provided in env:SNOWFLAKE_JSON')
const sfCfg: YtSfCfg = JSON.parse(sfJson)
const repo = process.env.REPO
if (!repo) throw new Error('no dataform repo provided env:REPO')
const branch = process.env.BRANCH ?? 'master'

var log = bunyan.createLogger(logCfg).child({repo, db:sfCfg.db})
log.info('recfluence dataform container started');

(async () => {
    // get latest code & configure dataform settings
    await run(branch, repo, sfCfg, dfRunArgs, log)
})().catch((e: any) => {
    const msg:string = (e instanceof Error) ? e.message : e
    log.error(stripAnsi(msg))
    delay(() => process.exit(1), 1500) // no flush option. give streams a chance to finish
})
