import { json } from "d3";

export function toMap<K, V>(array: Array<V>, getKey: ((item: V) => K)): Map<K, V> {
    let m = new Map<K, V>();
    array.forEach(e => {
      m.set(getKey(e), e);
    });
    return m;
}

export function jsonEquals(a:any, b:any) {
    return JSON.stringify(a) == JSON.stringify(b)
}

export function jsonClone<T>(o: T): T {
    return JSON.parse(JSON.stringify(o));
}

export async function delay(ms: number) {
    return new Promise(resolve => setTimeout(resolve, ms) );
}