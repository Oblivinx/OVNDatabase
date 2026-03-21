# OvnDB Changelog

## [0.0.2] — Big Update

### Fixed

- **BUG KRITIS** `StorageEngine.scan()` tidak mendekripsi data di `writeBuffer` sebelum di-yield.
  Sebelumnya `find({})` melewati writeBuffer tanpa `decryptFn`, menyebabkan JSON parse error
  dan dokumen fresh (belum di-flush) tidak muncul. Sekarang `scan()` selalu apply `decryptFn`
  ke data dari writeBuffer juga, konsisten dengan segment scan.

- **BUG KRITIS** `CollectionV2._withEncryption()` menggunakan monkey-patching pada
  `engine.insert/update/upsert`. Pendekatan ini rentan race condition bila ada dua operasi
  concurrent (misalnya via `Promise.all`). Diganti dengan override bersih pada
  `_serialize()` dan `_parse()` di `Collection`.

- `StorageEngine.read()` tidak mendekripsi data dari `writeBuffer`. Sekarang fixed — apapun
  yang keluar dari `read()` selalu plaintext.

- `StorageEngine._bufferWrite()` sekarang cache plaintext (hasil decrypt) di LRU, bukan
  ciphertext. Cache hit rate naik signifikan pada encrypted collections.

- `StorageEngine.scanRange()` juga fix — writeBuffer di-decrypt sebelum di-yield.

- `Collection.aggregate()` sekarang menggunakan `engine.scan()` yang sudah ada `decryptFn`,
  sehingga aggregate pada encrypted collection berfungsi benar.

- `Transaction` sekarang wrap error tiap op dengan context `op[N] (kind)` yang lebih informatif.

### Added

#### `Collection`
- `findById(id)` — shortcut `findOne({ _id })` tanpa object allocation
- `findManyById(ids[])` — batch read multiple IDs, deduplicates, paralel, skip missing
- `exists(filter)` — cek eksistensi tanpa load full document
- `bulkWrite(ops[], { ordered })` — batch operasi mixed (insert/update/delete/upsert/replace)
  dalam satu call. Support `ordered` (default true) dan `unordered` mode
- `truncate()` — hapus semua dokumen lebih efisien dari `deleteMany({})`
- `findOneAndReplace(filter, replacement)` — atomic find + replace + return new doc
- `compact()` — trigger manual compaction untuk collection ini
- `aggregate(pipeline, lookupResolver?)` — sekarang support custom `lookupResolver` untuk
  cross-collection `$lookup`
- `_serialize(doc)` — override hook untuk subclass (dipakai CollectionV2)

#### `Transaction`
- `upsert(col, filter, spec)` — upsert dalam transaction dengan rollback support
- `replace(col, filter, replacement)` — replace dalam transaction dengan rollback support

#### `UpdateSpec`
- `$setOnInsert` — hanya di-apply saat dokumen baru dibuat (upsert insert path)

#### `StorageEngine`
- `deleteAll()` — hapus semua record (untuk `truncate()`) tanpa per-record overhead
- `forceCompact()` — trigger manual compaction
- `backup(destPath)` — salin semua file collection ke destPath secara konsisten (flush dulu)

#### `OvnDB`
- `backup(destPath)` — full database backup (semua collection terbuka)
- `status()` — laporan kesehatan DB: `CollectionStatus[]`, `totalSize`, `isHealthy`
- `collectionExists(name)` — cek apakah collection sudah ada di disk
- `gracefulShutdown` option — auto-close pada SIGTERM/SIGINT/beforeExit

#### `CryptoLayer`
- `verify(ciphertext)` — cek integritas buffer tanpa throw
- `reencrypt(ciphertext, newCrypto)` — re-encrypt dengan key baru (key rotation)
- `rotateKey(ciphertexts[], newCrypto)` — mass key rotation
- `CryptoLayer.isEncryptedBuffer(buf)` — heuristik cek apakah buffer adalah ciphertext

#### `LRUCache`
- `clear()` — kosongkan semua entry (dipakai oleh `deleteAll()`)

#### `SecondaryIndexManager`
- `clearAll()` — kosongkan semua index data (dipakai oleh `truncate()`)

#### Types
- `BulkWriteOp<T>` — discriminated union untuk semua op type di `bulkWrite()`
- `BulkWriteResult` — hasil detail `bulkWrite()` (counts, insertedIds, errors)
- `DBStatus` — struktur hasil `db.status()`
- `CollectionStatus` — status per-collection dalam `DBStatus`
- `CursorOptions<T>` — untuk implementasi cursor pagination di masa depan

### Changed

- `CollectionV2` tidak lagi menggunakan monkey-patching. Semua write ops kini
  menggunakan `_serialize()` override, semua read ops menggunakan `_parse()` override.
  Behavior dari luar tidak berubah.
- `upsertOne()` sekarang support `$setOnInsert` (backward compatible — jika tidak ada `$setOnInsert`, behavior sama seperti sebelumnya).
