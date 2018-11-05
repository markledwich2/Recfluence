export function toMap<K, V>(array: Array<V>, getKey: ((item: V) => K)): Map<K, V> {
    let m = new Map<K, V>();
    array.forEach(e => {
      m.set(getKey(e), e);
    });
    return m;
}

export function max<T>(items: Array<T>, getValue: (item: T) => number): number {
    return items.reduce((max, n) => Math.max(getValue(n), max), 0);
}
