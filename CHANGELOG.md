# OvnDB Changelog

## [4.0.0] — v4.0 Feature Release

### ✨ New Features

#### Compound Index
**Files:** `src/types/index.ts`, `src/core/index/secondary-index.ts`  
`createIndex({ field: ['city', 'role'] })` now creates a multi-field composite key.
The composite key is stored as `"val1\x00val2"` (NUL-byte separator that cannot appear in JSON).
Index file names use `__` separator (e.g., `col.idx.city__role`).
The query planner automatically uses compound indexes for multi-field equality queries.

#### Bloom Filter (per-segment)
**Files:** `src/core/storage/bloom-filter.ts` (new), `src/core/storage/segment-manager.ts`  
Each segment now maintains an in-memory Bloom Filter (FNV-1a double-hashing, default 1% FPR).
`findOne` / `read` operations check the filter before disk I/O. Guaranteed false-negative-free:
if the filter says "absent", no disk read is needed.

#### Full-text Search (FTS Index)
**Files:** `src/core/index/fts-index.ts` (new), `src/collection/collection.ts`  
`await col.createTextIndex('fieldName')` builds an inverted posting-list index persisted as JSON.
Query with `find({ $text: 'word1 word2' })` — multi-word queries use AND semantics.
The index is updated automatically on `insertOne`, `updateOne`, and `deleteOne`.

#### Savepoints
**File:** `src/core/transaction/transaction.ts`  
`tx.savepoint('name')` records the current applied-ops position.
`await tx.rollbackTo('name')` undoes all operations applied after the savepoint.
The transaction remains in a `pending` state after rollback, allowing further ops or `commit()`.

#### Import / Export
**Files:** `src/collection/import-export.ts` (new), `src/collection/collection.ts`  
`col.exportTo(path, opts?)` — exports to NDJSON (default) or JSON array.
`col.importFrom(path, opts?)` — imports from NDJSON or JSON, returns `{ total, inserted, errors }`.
Options: `format`, `mode` ('insert'|'upsert'), `projection`, `continueOnError`.

### 🔒 Security Hardening

- **`__proto__` injection detection** — `validateQueryFilter`, `validateUpdateSpec`, `validateDocumentKeys` now use `Object.getPrototypeOf()` to detect `{ '__proto__': x }` object-literal injection (which sets the prototype chain, bypassing `Object.keys`).
- **insertOne validates `doc` BEFORE spread** — `{ ...doc }` normalizes the prototype, so we now call `validateDocumentKeys(doc)` before spreading.
- **integrityKey validated in `OvnDB.open()`** — throws immediately if `integrityKey.length !== 32`, not deferred to `collection()`.
- **`$where` blocked at `matchFilter` top level** — was only blocked in `matchField` before.
- **`Reflect.ownKeys` in security validators** — catches string keys that `Object.keys` misses.

### 🐛 Bug Fixes

- **secondary-index `save()`** — added `fsp.mkdir(path.dirname(idxPath), { recursive: true })` before write to prevent `ENOENT` when compound index directory doesn't exist yet.
- **id-generator comments** — removed literal `Math.random` string from JSDoc comments to prevent false-positives in source-code security tests.
- **aggregation `$sort` prototype check** — use `Reflect.ownKeys` + `Object.getPrototypeOf` to detect `__proto__` as sort key.

### 📦 Public API Changes

```typescript
// New exports from 'ovndb'
export { FTSIndex }           // Full-text Search index class
export { BloomFilter }        // Bloom Filter data structure
export { exportTo, importFrom } // Import/Export utilities
export const VERSION = '4.0.0'; // Version constant

// New types
export type { TextIndexDefinition, ExportOptions, ImportOptions, ImportResult }

// Collection API additions
col.createTextIndex(field: string): Promise<void>
col.exportTo(filePath: string, opts?: ExportOptions): Promise<number>
col.importFrom(filePath: string, opts?: ImportOptions): Promise<ImportResult>

// Transaction API additions
tx.savepoint(name: string): void
tx.rollbackTo(name: string): Promise<void>

// Index definition now accepts compound fields
createIndex({ field: string | string[], unique: boolean })
```

---

## [3.0.0] — Full Production Release


### 🔴 Critical Bug Fixes

#### G1 — LRU Cache berbasis bytes (bukan jumlah entry)
**File:** `src/core/cache/lru-cache.ts`
Sebelumnya `DOC_CACHE_SIZE = 100_000` bisa menyebabkan OOM jika tiap dokumen besar.
Sekarang `LRUCache` menggunakan batas byte (`MAX_CACHE_BYTES = 256 MB`). Eviction otomatis
saat bytes melebihi limit — tidak ada lagi OOM dari cache.

#### G2 — `deleteAll()` O(1) bukan O(n)
**File:** `src/core/storage/storage-engine.ts`
Sebelumnya loop serial per-record via B+ Tree. Sekarang:
- `tree.clear()` → reset tree ke root kosong (O(1) via `PageManager.reset()`)
- `segments.markAllDeleted()` → satu pass per segment, bukan per-record B+ Tree lookup

#### G3 — `autoCompact` pointer update O(1) bukan O(n²)
**File:** `src/core/storage/storage-engine.ts`
Sebelumnya setiap pointer update membutuhkan full tree scan O(n).
Sekarang callback menerima data record sehingga `_id` bisa di-extract langsung → O(1) per record.

#### G4 — WriteBuffer byte limit (64 MB)
**File:** `src/core/storage/storage-engine.ts`
Flush sekarang dipicu oleh count ATAU bytes threshold — mencegah 2000 dokumen 1MB memenuhi 2GB RAM.

#### G5 — WAL rotation (max 256 MB)
**File:** `src/core/wal/wal.ts`
WAL di-rotate ke `.wal.bak` saat melampaui `WAL_MAX_SIZE_BYTES`. Mencegah WAL tumbuh tak terbatas
saat crash sebelum checkpoint.

#### G6 — WAL replay tidak lagi apply operasi dari TX_ABORT
**File:** `src/core/wal/wal.ts`
Bug kritis: operasi dari transaksi yang di-abort di-replay saat crash recovery → data corruption.
Sekarang `_replay()` melacak semua `TX_ABORT` txId dan memfilter operasinya.

#### G13 — Key versioning (1 byte header)
**File:** `src/crypto/crypto-layer.ts`
Format ciphertext v3: `[1 keyVersion][12 IV][16 Tag][N CT]` (29 bytes overhead).
Memungkinkan key rotation bertahap tanpa harus downtime seluruh collection.

#### G17 — Manifest checksum SHA-256
**File:** `src/core/storage/segment-manager.ts`
Manifest `.json` sekarang disertai checksum SHA-256. Jika manifest corrupt saat open(),
error dilempar sebelum data dibaca → tidak ada silent data corruption.

### 🟡 Medium Fixes & Improvements

#### G7 — `PagedBPlusTree.clear()` method
O(1) tree reset via `PageManager.reset()`. Dipakai oleh `deleteAll()`.

#### G8 — `entries()` dengan limit + cursor pagination
`tree.entries(opts?)` support `gte`, `limit`, `after` untuk efisien cursor pagination.

#### G9 — Dirty-aware page eviction
`PageManager` kini memilih clean pages untuk di-evict terlebih dahulu. Dirty pages hanya
di-evict jika tidak ada clean page — mencegah latency spike dari forced synchronous write.

#### G14 — `CollectionV2.rotateEncryptionKey(newCrypto)`
Rotate key semua record dalam collection tanpa downtime. Update `decryptFn` engine setelah selesai.

#### G15 — Compression via `compressFn/decompressFn` hooks
`FileFlags.COMPRESSED` kini diimplementasi. Inject compression di `OvnDB.open()` atau langsung
ke `StorageEngine`. Kompatibel dengan `zlib.gzipSync`, `lz4`, `zstd`, dll.

#### G16 — `scanAll()` partial scan (fromSegment, fromOffset)
`SegmentManager.scanAll()` sekarang bisa mulai dari segment dan offset tertentu — efisien untuk
large cursor-based pagination.

### ✨ New Features

#### G10 — Query operators: `$type`, `$where`, `$mod`, `$not`
**File:** `src/core/query/filter.ts`
- `$type: 'string'|'number'|'boolean'|'array'|'object'|'null'` — cek tipe nilai
- `$where: (val) => boolean` — predikat kustom
- `$mod: [divisor, remainder]` — modulo check
- `$not: { $gt: 5 }` — negasi operator

#### G11 — Aggregation: `$bucket`, `$facet`, `$densify`, `$setWindowFields`
**File:** `src/core/query/aggregation.ts`
- `$bucket` — distribusikan docs ke range buckets (histogram)
- `$facet` — jalankan multiple sub-pipeline paralel, return satu output dokumen
- `$densify` — isi gap dalam sequence numerik (time-series)
- `$setWindowFields` — window functions: `$rank`, `$denseRank`, `$sum`, `$avg` dengan window frames

#### G12 — `FieldCrypto` — enkripsi per-field
**File:** `src/crypto/crypto-layer.ts`
```ts
const fc = new FieldCrypto(cryptoLayer);
const doc = fc.encryptFields({ name, phone, ssn }, ['phone', 'ssn']);
const plain = fc.decryptFields(doc);
```
Field yang terenkripsi disimpan sebagai base64. Tidak terenkripsi tetap bisa di-query secara normal.

#### G18 — `MigrationRunner` — schema evolution
**File:** `src/migration/migration-runner.ts`
```ts
const runner = new MigrationRunner(collection);
await runner.migrate(2, (doc) => ({ ...doc, fullName: `${doc.first} ${doc.last}` }), {
  batchSize: 500, dryRun: false, continueOnError: true,
  onProgress: (p) => console.log(`${p.migrated}/${p.total}`),
});
```
- Cursor-based iteration (aman untuk collection besar)
- `dryRun` mode untuk preview
- `countPending()` dan `isComplete()`

#### G19 — `findWithStats()` — execution statistics
**File:** `src/collection/collection.ts`
```ts
const { docs, stats } = await col.findWithStats({ role: 'admin' });
// stats.planType, stats.indexUsed, stats.totalDocsScanned,
// stats.totalKeysScanned, stats.nReturned, stats.executionTimeMs
```

### Changed

- `OvnDB.open()` sekarang menerima `cacheBytes` (byte limit) alih-alih `cacheSize` (count).
  `cacheSize` masih diterima sebagai alias untuk backward compat.
- `OvnDB.open()` menerima `compressFn` dan `decompressFn` untuk compression global.
- `LRUCache` constructor sekarang `maxBytes` bukan `capacity`. Angka kecil (≤ 10000)
  dianggap jumlah entry × 4096 bytes untuk backward compat.
- Manifest format `v3`: tambah field `checksum` SHA-256. Manifest v2 tetap dibaca tanpa error
  (checksum di-skip jika tidak ada).
