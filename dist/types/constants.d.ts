export declare const OVN_MAGIC: Buffer<ArrayBuffer>;
export declare const WAL_MAGIC: Buffer<ArrayBuffer>;
export declare const PAGE_MAGIC: Buffer<ArrayBuffer>;
export declare const PAGE_SIZE: number;
export declare const BUFFER_POOL_SIZE = 16384;
export declare const EVICT_BATCH = 64;
export declare const SEGMENT_SIZE: number;
export declare const MANIFEST_FILE = "manifest.json";
export declare const FLUSH_INTERVAL_MS = 50;
export declare const FLUSH_THRESHOLD = 2000;
export declare const BULK_FLUSH_THRESHOLD = 50000;
export declare const WAL_GROUP_SIZE = 256;
export declare const WAL_GROUP_WAIT_MS = 5;
export declare const WAL_MAX_SIZE_BYTES: number;
export declare const MAX_CACHE_BYTES: number;
export declare const MAX_BUFFER_BYTES: number;
export declare const DOC_CACHE_SIZE = 100000;
export declare const COMPACTION_RATIO = 0.35;
export declare const COMPACTION_CHECK_MS = 30000;
export declare const REC_STATUS_SIZE = 1;
export declare const REC_TXID_SIZE = 8;
export declare const REC_DATALEN_SIZE = 4;
export declare const REC_CRC_SIZE = 4;
export declare const REC_PREFIX_SIZE: number;
export declare const REC_OVERHEAD: number;
export declare const HEADER_SIZE = 128;
export declare const enum RecordStatus {
    DELETED = 0,
    ACTIVE = 1
}
export declare const enum WalOp {
    INSERT = 1,
    UPDATE = 2,
    DELETE = 3,
    CHECKPOINT = 4,
    TX_BEGIN = 5,
    TX_COMMIT = 6,
    TX_ABORT = 7
}
export declare const enum PageType {
    FREE = 0,
    INTERNAL = 1,
    LEAF = 2
}
export declare const enum FileFlags {
    NONE = 0,
    ENCRYPTED = 1,
    COMPRESSED = 2
}
export declare const MANIFEST_CHECKSUM_ALGO: "sha256";
//# sourceMappingURL=constants.d.ts.map