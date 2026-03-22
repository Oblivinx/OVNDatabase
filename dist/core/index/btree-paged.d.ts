import { PageManager } from '../storage/page-manager.js';
import type { RecordPointer } from '../../types/index.js';
export declare class PagedBPlusTree {
    private readonly pm;
    private _size;
    private _firstLeafId;
    private _lastLeafId;
    constructor(pm: PageManager);
    get size(): bigint;
    init(): Promise<void>;
    /**
     * Hapus semua entry dan reset tree ke root leaf kosong.
     * O(1) — tidak ada loop per-record.
     * Dipakai oleh StorageEngine.deleteAll().
     */
    clear(): Promise<void>;
    count(): Promise<bigint>;
    get(key: string): Promise<RecordPointer | undefined>;
    has(key: string): Promise<boolean>;
    set(key: string, val: RecordPointer): Promise<void>;
    delete(key: string): Promise<boolean>;
    range(gte?: string, lte?: string): AsyncIterableIterator<[string, RecordPointer]>;
    /**
     * G8: entries() dengan support limit dan after (cursor pagination).
     * Jauh lebih efisien dari meload semua entry lalu slice.
     */
    entries(opts?: {
        gte?: string;
        limit?: number;
        after?: string;
    }): AsyncIterableIterator<[string, RecordPointer]>;
    private _insert;
    private _insertNonFull;
    private _splitChild;
    private _findInLeaf;
    private _findFirstLeaf;
    private _findLastLeaf;
    private _bsearch;
    private _readLeaf;
    private _writeLeaf;
    private _readInternal;
    private _writeInternal;
}
//# sourceMappingURL=btree-paged.d.ts.map