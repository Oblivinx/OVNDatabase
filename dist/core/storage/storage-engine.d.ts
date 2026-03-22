import type { OvnStats } from '../../types/index.js';
import { SegmentManager } from './segment-manager.js';
import { PageManager } from './page-manager.js';
import { PagedBPlusTree } from '../index/btree-paged.js';
import { WAL } from '../wal/wal.js';
import { MVCCManager } from '../transaction/mvcc.js';
import { SecondaryIndexManager } from '../index/secondary-index.js';
export declare class StorageEngine {
    readonly dirPath: string;
    private readonly colName;
    readonly segments: SegmentManager;
    readonly pageManager: PageManager;
    readonly tree: PagedBPlusTree;
    readonly wal: WAL;
    readonly mvcc: MVCCManager;
    private readonly cache;
    private readonly writeBuffer;
    private _bufferBytes;
    private _secondaryIdx?;
    private flushTimer;
    private compactTimer;
    private _flushPromise;
    private _closed;
    private _bulkMode;
    /** G15: inject compress/decompress functions */
    compressFn?: (buf: Buffer) => Buffer;
    decompressFn?: (buf: Buffer) => Buffer;
    /** Inject decrypt function untuk CollectionV2 */
    decryptFn?: (buf: Buffer) => Buffer;
    /**
     * SECURITY: inject 32-byte HMAC key untuk manifest integrity verification.
     * Jika di-set, manifest ditulis/dibaca dengan HMAC-SHA256 bukan SHA-256 plain.
     * Derive dari passphrase yang sama dengan encryption key menggunakan HKDF:
     *   engine.integrityKey = crypto.hkdfSync('sha256', masterKey, salt,
     *     Buffer.from('ovndb-manifest-hmac'), 32);
     */
    integrityKey?: Buffer;
    constructor(dirPath: string, colName: string, cacheBytes?: number);
    open(): Promise<void>;
    close(): Promise<void>;
    setSecondaryIndex(mgr: SecondaryIndexManager): void;
    beginBulkLoad(): void;
    endBulkLoad(): Promise<void>;
    insertBulk(id: string, data: Buffer, txId?: bigint): Promise<void>;
    insert(id: string, data: Buffer, txId?: bigint): Promise<void>;
    update(id: string, data: Buffer, txId?: bigint): Promise<void>;
    upsert(id: string, data: Buffer, txId?: bigint): Promise<void>;
    delete(id: string, txId?: bigint): Promise<boolean>;
    /**
     * G2: deleteAll() — O(1) path.
     * Alih-alih loop per record, kita:
     * 1. Flush pending buffer
     * 2. B+ Tree.clear() — reset ke root leaf kosong (O(1))
     * 3. SegmentManager.markAllDeleted() — set status DELETED per segment (1 pass per segment)
     * 4. Clear LRU cache dan writeBuffer
     */
    deleteAll(): Promise<void>;
    read(id: string): Promise<Buffer | null>;
    scan(decryptFn?: (b: Buffer) => Buffer): AsyncIterableIterator<[string, Buffer]>;
    scanRange(gte?: string, lte?: string): AsyncIterableIterator<[string, Buffer]>;
    flush(): Promise<void>;
    forceCompact(): Promise<void>;
    backup(destPath: string): Promise<void>;
    stats(collection: string): Promise<OvnStats>;
    private _liveExists;
    private _bufferWrite;
    private _flush;
    private _doFlush;
    private _readFromSegment;
    /**
     * G3: autoCompact callback menerima _id sekarang.
     * Constraint baru: SegmentManager.autoCompact menerima callback dengan signature baru
     * yang juga memberikan _id sehingga B+ Tree update jadi O(1) per record.
     * Karena SegmentManager lama tidak pass _id, kita scan data untuk extract _id.
     */
    private _autoCompact;
}
//# sourceMappingURL=storage-engine.d.ts.map