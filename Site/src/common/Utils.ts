import _ from "lodash"

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
    const res = await fetch(url, Object.assign(<RequestInit>{ method: 'GET' }, cfg))
    const json = await res.json()
    return json as T
}

export function secondsToHHMMSS(time: number) {
    var pad = function (num: number, size: number) { return ('000' + num).slice(size * -1) },
        hours = Math.floor(time / 60 / 60),
        minutes = Math.floor(time / 60) % 60,
        seconds = Math.floor(time - minutes * 60)
    return pad(hours, 2) + ':' + pad(minutes, 2) + ':' + pad(seconds, 2)
}