// ============================================================
//  OvnDB v1.0 — Core Types & Constants
// ============================================================

// File-level magic bytes: "OVN" + version byte
export const OVN_MAGIC   = Buffer.from([0x4f, 0x56, 0x4e, 0x01]);
export const WAL_MAGIC   = Buffer.from([0x4f, 0x57, 0x4c, 0x01]);
export const HEADER_SIZE = 64;

// Tuning knobs — safe to adjust
export const CHUNK_SIZE          = 64 * 1024 * 1024; // 64 MB
export const FLUSH_INTERVAL_MS   = 50;               // max ms before write-buffer flush
export const FLUSH_THRESHOLD     = 512;              // flush after N buffered records
export const COMPACTION_RATIO    = 0.35;             // compact when dead space > 35%
export const DEFAULT_CACHE_SIZE  = 100_000;          // LRU entries per collection

export const enum RecordStatus {
  DELETED = 0x00,
  ACTIVE  = 0x01,
}

export const enum WalOp {
  INSERT     = 0x01,
  UPDATE     = 0x02,
  DELETE     = 0x03,
  CHECKPOINT = 0x04,
}

export const enum FileFlags {
  NONE      = 0,
  ENCRYPTED = 1 << 0,
}

// Record on-disk layout:
//   [1] status   RecordStatus
//   [4] dataLen  uint32 LE
//   [N] data     JSON / ciphertext
//   [4] crc32    uint32 LE
export const RECORD_PREFIX_SIZE = 5;  // status(1) + dataLen(4)
export const RECORD_SUFFIX_SIZE = 4;  // crc32(4)
export const RECORD_OVERHEAD    = RECORD_PREFIX_SIZE + RECORD_SUFFIX_SIZE; // 9

export interface FileHeader {
  version:     number;
  flags:       FileFlags;
  recordCount: number;
  liveCount:   number;
  dataEnd:     number;
  createdAt:   number;
  updatedAt:   number;
}

export interface RecordPointer {
  offset:    number;
  totalSize: number;
  dataSize:  number;
}

// ── Public document shape ────────────────────────────────────
export interface OvnDocument {
  _id: string;
  [key: string]: unknown;
}

// ── Query DSL ────────────────────────────────────────────────
export type Scalar = string | number | boolean | null;

export interface FieldOps {
  $eq?:     Scalar;
  $ne?:     Scalar;
  $gt?:     number | string;
  $gte?:    number | string;
  $lt?:     number | string;
  $lte?:    number | string;
  $in?:     Scalar[];
  $nin?:    Scalar[];
  $exists?: boolean;
  $regex?:  string | RegExp;
}

export interface QueryFilter {
  [field: string]: Scalar | FieldOps | QueryFilter[] | QueryFilter | undefined;
  $and?: QueryFilter[];
  $or?:  QueryFilter[];
  $not?: QueryFilter;
}

export interface QueryOptions {
  limit?:      number;
  skip?:       number;
  sort?:       Record<string, 1 | -1>;
  projection?: Record<string, 0 | 1>;
}

export interface UpdateSpec {
  $set?:    Record<string, unknown>;
  $unset?:  Record<string, 1>;
  $inc?:    Record<string, number>;
  $push?:   Record<string, unknown>;
  $pull?:   Record<string, unknown>;
  $rename?: Record<string, string>;
  [key: string]: unknown;
}

export interface PendingWrite {
  id:   string;
  data: Buffer;
  op:   WalOp;
}

export interface OvnStats {
  collection:    string;
  recordCount:   number;
  liveCount:     number;
  fileSize:      number;
  fragmentRatio: number;
  cacheSize:     number;
  cacheHitRate:  number;
  indexEntries:  number;
  walPending:    number;
}

/** Type alias for documenting foreign key fields */
export type ForeignKey<_Collection extends string = string> = string;
export type ForeignKeyArray<_Collection extends string = string> = string[];
