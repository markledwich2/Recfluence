import _ from "lodash"
import * as dateformat from 'dateformat'

export function toRecord<T, V>(items: T[], getKey: (r: T) => string, getValue: (r: T) => V): Record<string, V> {
    return _.mapValues(_.keyBy(items, getKey), getValue)
}

export function jsonEquals(a: any, b: any) {
    return JSON.stringify(a) == JSON.stringify(b)
}

export function jsonClone<T>(o: T): T {
    return JSON.parse(JSON.stringify(o))
}

export async function delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms))
}

export function classNames(classes: Record<string, boolean>) {
    return _.keys(classes).filter(k => classes[k]).join(' ')
}

export function typedKeys<T>(o: T) { return Object.keys(o) as Array<keyof T> }

/** for s=thingLikeThis returns THING_LIKE_THIS */
export function capitalCase(s: string) { return _.kebabCase(s).replace('-', '_').toUpperCase() }

//export function capitalize(s: string) { return s?.length >= 1 ? s.charAt(0).toUpperCase() + s.slice(1) : s }

/** like Object.assign, but doesn't mutate a */
export function merge<T>(a: T, b: any, c?: any): T { return Object.assign({}, a, b, c) }

/** GET a json object and deserialize it */
export async function getJson<T>(url: RequestInfo, cfg?: RequestInit): Promise<T> {
    const res = await fetch(url, Object.assign({ method: 'GET' }, cfg))
    const json = await res.json()
    return json as T
}

// example ndjson streaming from  https://github.com/deanhume/streams/blob/master/main.js

function splitStream(splitOn: string): TransformStream {
    let buffer = ''
    return new TransformStream({
        transform(chunk, controller) {
            buffer += chunk
            const parts = buffer.split(splitOn)
            parts.slice(0, -1).forEach(part => controller.enqueue(part))
            buffer = parts[parts.length - 1]
        },
        flush(controller) {
            if (buffer) controller.enqueue(buffer)
        }
    })
}

function parseJSON<T>(): TransformStream<string, T> {
    return new TransformStream<string, T>({
        transform(chunk, controller) {
            controller.enqueue(JSON.parse(chunk))
        }
    })
}

export async function getJsonl<T>(url: RequestInfo, cfg?: RequestInit): Promise<T[]> {
    const res = await fetch(url, Object.assign({ method: 'GET' }, cfg))
    const reader = res.body
        .pipeThrough(new TextDecoderStream())
        .pipeThrough(splitStream('\n'))
        .pipeThrough(parseJSON<T>())
        .getReader()

    const items = []
    while (true) {
        var r = await reader.read()
        if (r.done) break
        items.push(r.value)
    }
    return items
}

export async function putJson(url: RequestInfo, data: any, cfg?: RequestInit): Promise<Response> {
    const res = await fetch(url, Object.assign({
        method: 'PUT',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(data)
    }, cfg))
    return res
}

export function secondsToHHMMSS(time: number) {
    var pad = function (num: number, size: number) { return ('000' + num).slice(size * -1) },
        hours = Math.floor(time / 60 / 60),
        minutes = Math.floor(time / 60) % 60,
        seconds = Math.floor(time - minutes * 60)
    return pad(hours, 2) + ':' + pad(minutes, 2) + ':' + pad(seconds, 2)
}

export function dateFormat(d: Date | string) { return d ? dateformat(d, 'd mmm yyyy') : d }