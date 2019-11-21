import { json } from "d3"
import _ from "lodash"

// interface Dic<T> {
//     [index: Extract<keyof T, string>]: T;
//   }

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