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

export function classNames(classes: Record<string, boolean>) {
    return _.keys(classes).filter(k => classes[k]).join(' ')
}

export function typedKeys<T>(o: T) { return Object.keys(o) as Array<keyof T> }

/** for s=thingLikeThis returns THING_LIKE_THIS */
export function capitalCase(s:string) { return _.kebabCase(s).replace('-', '_').toUpperCase() }

//export function capitalize(s: string) { return s?.length >= 1 ? s.charAt(0).toUpperCase() + s.slice(1) : s }

/** like Object.assign, but doesn't mutate a */
export function assign<T>(a:T, b:any):T { return Object.assign({}, a, b);}
