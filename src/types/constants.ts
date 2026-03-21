// ============================================================
//  OvnDB v2.0 — Konstanta global & tuning parameter
//
//  Semua angka magic di sini — jangan scatter di seluruh codebase.
//  Nilai default sudah di-tune untuk throughput tinggi di SSD NVMe.
//  Override lewat environment variable atau OvnDBOptions.
// ============================================================

/** Magic bytes untuk file header: "OVN" + version */
export const OVN_MAGIC   = Buffer.from([0x4f, 0x56, 0x4e, 0x02]); // v2
export const WAL_MAGIC   = Buffer.from([0x4f, 0x57, 0x4c, 0x02]);
export const PAGE_MAGIC  = Buffer.from([0x4f, 0x50, 0x47, 0x02]);

// ── Page & Buffer Pool ────────────────────────────────────────
/** Ukuran satu page di disk (16KB). Setiap node B+ Tree = 1 page. */
export const PAGE_SIZE         = 16 * 1024;         // 16 KB

/** Jumlah page maksimum di buffer pool per collection.
 *  16384 × 16KB = 256 MB RAM per collection. Aman untuk 10 collection = 2.5 GB. */
export const BUFFER_POOL_SIZE  = 16_384;             // pages

/** Batas halaman yang di-flush sekaligus saat eviction (batch eviction). */
export const EVICT_BATCH       = 64;

// ── Segment File ─────────────────────────────────────────────
/** Max ukuran satu segment file data (512 MB).
 *  Saat melewati batas ini, segment baru dibuat otomatis.
 *  Compaction bekerja per-segment secara paralel → tidak pernah lock seluruh collection. */
export const SEGMENT_SIZE      = 512 * 1024 * 1024; // 512 MB

/** Nama manifest yang menyimpan daftar semua segment. */
export const MANIFEST_FILE     = 'manifest.json';

// ── WAL & Flush ──────────────────────────────────────────────
/** Interval flush WAL ke disk (ms). Setiap flush = satu fdatasync(). */
export const FLUSH_INTERVAL_MS = 50;

/** Jumlah operasi sebelum flush dipaksa (back-pressure) — mode normal. */
export const FLUSH_THRESHOLD   = 2_000;

/**
 * Flush threshold saat bulk-load mode aktif.
 * Lebih besar = lebih sedikit flush = lebih sedikit fdatasync() = JAUH lebih cepat.
 * Risiko: lebih banyak data di RAM sebelum ditulis. Oke untuk batch import.
 */
export const BULK_FLUSH_THRESHOLD = 50_000;

/** Max operasi yang dikumpulkan sebelum satu group-commit WAL dikirim.
 *  Group commit mengurangi jumlah fdatasync() secara drastis di high-throughput. */
export const WAL_GROUP_SIZE    = 256;

/** Max waktu tunggu group commit WAL (ms). Commit dipaksa setelah batas ini. */
export const WAL_GROUP_WAIT_MS = 5;

// ── LRU Cache ────────────────────────────────────────────────
/** Jumlah entry dokumen per collection di LRU cache (bukan pages). */
export const DOC_CACHE_SIZE    = 100_000;

// ── Compaction ───────────────────────────────────────────────
/** Threshold fragmentasi sebelum auto-compaction dipicu (35%). */
export const COMPACTION_RATIO  = 0.35;

/** Interval pengecekan auto-compaction (ms). */
export const COMPACTION_CHECK_MS = 30_000;

// ── Record Layout (on-disk) ──────────────────────────────────
//
//  Setiap record di segment file:
//    [1]  status    RecordStatus (ACTIVE=0x01, DELETED=0x00)
//    [8]  txId      BigInt uint64 LE — MVCC version
//    [4]  dataLen   uint32 LE
//    [N]  data      JSON bytes / ciphertext
//    [4]  crc32     uint32 LE (covers status+txId+dataLen+data)
//
export const REC_STATUS_SIZE   = 1;
export const REC_TXID_SIZE     = 8;
export const REC_DATALEN_SIZE  = 4;
export const REC_CRC_SIZE      = 4;
export const REC_PREFIX_SIZE   = REC_STATUS_SIZE + REC_TXID_SIZE + REC_DATALEN_SIZE; // 13
export const REC_OVERHEAD      = REC_PREFIX_SIZE + REC_CRC_SIZE;                     // 17

// ── File Header (collection.seg-NNNN.ovn, 128 bytes) ─────────
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
  INTERNAL = 0x01, // B+ Tree internal node
  LEAF     = 0x02, // B+ Tree leaf node
}

// ── File Flags ───────────────────────────────────────────────
export const enum FileFlags {
  NONE      = 0,
  ENCRYPTED = 1 << 0,
  COMPRESSED = 1 << 1,
}
