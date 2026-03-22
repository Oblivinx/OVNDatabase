// ============================================================
//  OvnDB v3.0 — Konstanta global & tuning parameter
//  update: tambah MAX_CACHE_BYTES, MAX_BUFFER_BYTES, WAL_MAX_SIZE,
//          COMPRESSION_LEVEL, MANIFEST_CHECKSUM_ALGO
// ============================================================

export const OVN_MAGIC   = Buffer.from([0x4f, 0x56, 0x4e, 0x02]);
export const WAL_MAGIC   = Buffer.from([0x4f, 0x57, 0x4c, 0x02]);
export const PAGE_MAGIC  = Buffer.from([0x4f, 0x50, 0x47, 0x02]);

// ── Page & Buffer Pool ────────────────────────────────────────
export const PAGE_SIZE         = 16 * 1024;
export const BUFFER_POOL_SIZE  = 16_384;
export const EVICT_BATCH       = 64;

// ── Segment File ─────────────────────────────────────────────
export const SEGMENT_SIZE      = 512 * 1024 * 1024;
export const MANIFEST_FILE     = 'manifest.json';

// ── WAL & Flush ──────────────────────────────────────────────
export const FLUSH_INTERVAL_MS = 50;
export const FLUSH_THRESHOLD   = 2_000;
export const BULK_FLUSH_THRESHOLD = 50_000;
export const WAL_GROUP_SIZE    = 256;
export const WAL_GROUP_WAIT_MS = 5;

// G5: WAL max size before rotation (256 MB)
export const WAL_MAX_SIZE_BYTES = 256 * 1024 * 1024;

// ── Memory Limits ─────────────────────────────────────────────
// G1: Byte-based LRU cache limit (256 MB per collection)
export const MAX_CACHE_BYTES   = 256 * 1024 * 1024;

// G4: WriteBuffer byte limit (64 MB) — flush dipaksa sebelum OOM
export const MAX_BUFFER_BYTES  = 64 * 1024 * 1024;

// ── LRU Cache (legacy — dipertahankan untuk kompatibilitas) ───
export const DOC_CACHE_SIZE    = 100_000;

// ── Compaction ───────────────────────────────────────────────
export const COMPACTION_RATIO    = 0.35;
export const COMPACTION_CHECK_MS = 30_000;

// ── Record Layout (on-disk) ──────────────────────────────────
export const REC_STATUS_SIZE   = 1;
export const REC_TXID_SIZE     = 8;
export const REC_DATALEN_SIZE  = 4;
export const REC_CRC_SIZE      = 4;
export const REC_PREFIX_SIZE   = REC_STATUS_SIZE + REC_TXID_SIZE + REC_DATALEN_SIZE;
export const REC_OVERHEAD      = REC_PREFIX_SIZE + REC_CRC_SIZE;

// ── File Header ───────────────────────────────────────────────
export const HEADER_SIZE       = 128;

// ── Record Status ────────────────────────────────────────────
export const enum RecordStatus {
  DELETED = 0x00,
  ACTIVE  = 0x01,
}

// ── WAL Operation Codes ──────────────────────────────────────
export const enum WalOp {
  INSERT     = 0x01,
  UPDATE     = 0x02,
  DELETE     = 0x03,
  CHECKPOINT = 0x04,
  TX_BEGIN   = 0x05,
  TX_COMMIT  = 0x06,
  TX_ABORT   = 0x07,
}

// ── Page Type ────────────────────────────────────────────────
export const enum PageType {
  FREE     = 0x00,
  INTERNAL = 0x01,
  LEAF     = 0x02,
}

// ── File Flags ───────────────────────────────────────────────
export const enum FileFlags {
  NONE       = 0,
  ENCRYPTED  = 1 << 0,
  COMPRESSED = 1 << 1,  // G15: sekarang diimplementasi
}

// G17: Manifest checksum algorithm
export const MANIFEST_CHECKSUM_ALGO = 'sha256' as const;
