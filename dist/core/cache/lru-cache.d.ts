export declare class LRUCache<K, V> {
    private readonly _maxBytes;
    private readonly _sizeof;
    private readonly map;
    private readonly head;
    private readonly tail;
    private _bytes;
    private _hits;
    private _misses;
    private _ops;
    private readonly WINDOW;
    /**
     * @param maxBytes Batas memori dalam bytes (default 256 MB).
     *                 Jika diisi angka kecil (<= 10000), dianggap jumlah entry
     *                 dan dikonversi ke estimasi bytes (backward compat).
     * @param sizeof   Fungsi untuk menghitung ukuran satu value.
     */
    constructor(maxBytes: number, sizeof?: (v: V) => number);
    get(key: K): V | undefined;
    set(key: K, val: V): void;
    delete(key: K): boolean;
    has(key: K): boolean;
    /** Hapus semua entry dan reset statistik. */
    clear(): void;
    get size(): number;
    get bytes(): number;
    get maxBytes(): number;
    get capacity_(): number;
    get hitRate(): number;
    entries(): IterableIterator<[K, V]>;
    /** G1: evict LRU entries sampai penggunaan bytes di bawah limit. */
    private _evictIfNeeded;
    private _detach;
    private _insertAfterHead;
    private _maybeResetWindow;
}
//# sourceMappingURL=lru-cache.d.ts.map