import _ from "lodash"

export function toDic<T, V>(items: T[], getKey: (r: T) => string, getValue: (r: T) => V) {
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

export function classNames(classes:Record<string, boolean>) {
    return _.keys(classes).filter(k => classes[k]).join(' ')
}

export function typedKeys<T>(o:T) {
    return Object.keys(o) as Array<keyof T>
}