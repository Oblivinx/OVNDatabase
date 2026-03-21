# OvnDB v0.0.2

**High-performance embedded NoSQL database for Node.js — scales to trillions of records.**

Zero external dependencies. Pure TypeScript. MongoDB-compatible API.

---

## What's new in v0.0.2

| Feature | v0.0.1 | v0.0.2 |
|---|---|---|
| **Index storage** | In-memory B+ Tree (RAM = index size) | **On-disk Paged B+ Tree** (256 MB buffer pool serves 1T+ records) |
| **Data files** | Single `.ovn` file per collection | **Multi-segment** (512 MB each, parallel compaction) |
| **WAL** | 1 `fdatasync` per operation | **Group commit** (~128 ops per sync, ~10× throughput) |
| **Record count** | uint32 (max 4.29B) | **BigInt / uint64** (unlimited) |
| **Transactions** | Compensating writes (can fail) | **MVCC Snapshot Isolation** (rollback = abort txId) |
| **Aggregation** | None | **Full pipeline** ($match $group $sort $lookup $unwind…) |
| **CDC** | None | **`collection.watch()`** — real-time change events |
| **Query planner** | None | **Cost-based optimizer** with index selection |
| **Schema** | None | **SchemaValidator** with fluent `field()` builder |
| **Metrics** | Basic | **P50/P95/P99 per op**, Prometheus export, slow query log |

---

## Quick Start

```typescript
import { OvnDB } from 'ovndb';

const db    = await OvnDB.open('./data');
const users = await db.collection<User>('users');

// Insert
const user = await users.insertOne({ name: 'Budi', phone: '628123', points: 0 });

// Find
const vips = await users.find(
  { points: { $gte: 1000 } },
  { sort: { points: -1 }, limit: 10 }
);

// Update
await users.updateOne({ _id: user._id }, { $inc: { points: 50 } });

// Upsert
await users.upsertOne({ _id: user._id }, { $inc: { points: 10 } });

// Aggregate
const report = await users.aggregate([
  { $match: { active: true } },
  { $group: { _id: '$city', total: { $sum: '$points' }, count: { $sum: 1 } } },
  { $sort: { total: -1 } },
  { $limit: 5 },
]);

await db.close();
```

---

## API Reference

### OvnDB

```typescript
// Buka database
const db = await OvnDB.open('./data', {
  cacheSize: 100_000,  // LRU cache entries per collection
  fileLock:  true,     // prevent multi-process corruption
  mkdirp:    true,     // auto-create directory
});

// Collection standar
const col = await db.collection<MyDoc>('collection-name');

// Collection dengan enkripsi + secondary index
const col = await db.collectionV2<MyDoc>('col', {
  crypto:  await cryptoFromPassphrase(process.env.DB_KEY!, './data'),
  indexes: [{ field: 'email', unique: true }],
});

// Transaction
const tx = db.beginTransaction();
tx.insert(col1, { name: 'Budi' });
tx.update(col2, { _id: id }, { $inc: { balance: -100 } });
await tx.commit(); // auto-rollback jika ada yang gagal

// Maintenance
await db.flushAll();
await db.dropCollection('col-name');
const names = await db.listCollections();
await db.close();
```

### Collection

```typescript
// ── Insert ──────────────────────────────────────────────────
const doc = await col.insertOne({ name: 'Budi', points: 0 });          // _id auto-generated
const doc = await col.insertOne({ _id: 'u1', name: 'Siti', points: 0 });
const docs = await col.insertMany([{ name: 'A' }, { name: 'B' }]);

// ── Find ────────────────────────────────────────────────────
const doc  = await col.findOne({ _id: 'u1' });
const docs = await col.find({ city: 'Jakarta' }, { sort: { points: -1 }, limit: 10 });
const docs = await col.find({ points: { $gte: 100, $lte: 500 } });
const docs = await col.find({ $or: [{ city: 'Jakarta' }, { active: true }] });
const docs = await col.find({ tags: { $in: ['nodejs', 'typescript'] } });
const docs = await col.find({ name: { $regex: /^Budi/i } });

// Cursor pagination (lebih efisien dari skip untuk dataset besar)
const page1 = await col.find({}, { limit: 20, sort: { _id: 1 } });
const page2 = await col.find({}, { limit: 20, sort: { _id: 1 }, after: page1.at(-1)?._id });

// ── Update ──────────────────────────────────────────────────
await col.updateOne({ _id: 'u1' }, { $inc: { points: 50 } });
await col.updateOne({ _id: 'u1' }, { $set: { name: 'New Name' } });
await col.updateOne({ _id: 'u1' }, { $push: { tags: 'nodejs' } });
await col.updateMany({ active: false }, { $set: { active: true } });
const doc = await col.upsertOne({ _id: 'u1' }, { $set: { name: 'Budi' } });
const doc = await col.findOneAndUpdate({ _id: 'u1' }, { $inc: { points: 1 } });

// ── Delete ──────────────────────────────────────────────────
await col.deleteOne({ _id: 'u1' });
await col.deleteMany({ active: false });
const doc = await col.findOneAndDelete({ _id: 'u1' });

// ── Query Info ──────────────────────────────────────────────
const count  = await col.countDocuments({ city: 'Jakarta' });
const cities = await col.distinct('city');
const plan   = col.explain({ city: 'Jakarta' }); // query plan tanpa eksekusi
const stats  = await col.stats();

// ── Index ───────────────────────────────────────────────────
await col.createIndex({ field: 'email', unique: true });
await col.createIndex({ field: 'city',  unique: false, sparse: true });
await col.createIndex({ field: 'score', unique: false, partial: { active: true } });
```

### Aggregation Pipeline

```typescript
const result = await col.aggregate([
  { $match:   { active: true, city: { $in: ['Jakarta', 'Bandung'] } } },
  { $group:   { _id: '$city', total: { $sum: '$points' }, avg: { $avg: '$points' }, count: { $sum: 1 } } },
  { $addFields: { avgRounded: { $add: ['$avg', 0] } } },
  { $sort:    { total: -1 } },
  { $limit:   5 },
  { $project: { _id: 1, total: 1, count: 1 } },
]);

// $unwind
const expanded = await col.aggregate([
  { $unwind: '$tags' },
  { $group: { _id: '$tags', count: { $sum: 1 } } },
  { $sort: { count: -1 } },
]);

// $count
const [{ total }] = await col.aggregate([{ $count: 'total' }]);
```

### Query Operators

| Operator | Deskripsi |
|---|---|
| `$eq` `$ne` | Equal / Not equal |
| `$gt` `$gte` `$lt` `$lte` | Comparison |
| `$in` `$nin` | In / Not in array |
| `$exists` | Field exists check |
| `$regex` | Regular expression |
| `$and` `$or` `$nor` `$not` | Logical operators |
| `$size` | Array length |
| `$all` | Array contains all |
| `$elemMatch` | Array element match |

### Update Operators

| Operator | Deskripsi |
|---|---|
| `$set` `$unset` | Set / Remove field |
| `$inc` | Increment number |
| `$mul` | Multiply number |
| `$min` `$max` | Set if less/greater |
| `$push` | Append to array (+ `$each` `$sort` `$slice`) |
| `$pull` | Remove from array |
| `$addToSet` | Add if not exists |
| `$rename` | Rename field |

### Encryption (CollectionV2)

```typescript
import { OvnDB, cryptoFromPassphrase } from 'ovndb';

const db  = await OvnDB.open('./data');
const key = await cryptoFromPassphrase(process.env.DB_KEY!, './data');

const users = await db.collectionV2<User>('users', {
  crypto:  key,                                          // AES-256-GCM per-record
  indexes: [{ field: 'phone', unique: true }],
});

// API identik dengan Collection — enkripsi/dekripsi transparan
const user = await users.findOne({ phone: '628123' });
```

### Schema Validation

```typescript
import { SchemaValidator, field } from 'ovndb';

const schema = new SchemaValidator({
  name:   field('string').required().minLength(2).maxLength(100).build(),
  phone:  field('string').required().pattern(/^628\d{8,12}$/).build(),
  points: field('number').min(0).max(1_000_000).default(0).build(),
  tags:   field('array').maxItems(20).items(field('string').build()).build(),
  role:   field('string').enum('admin', 'user', 'guest').build(),
  meta:   field('object').properties({
    level: field('number').integer().min(1).max(10).build(),
  }).build(),
});

// Soft validation (tidak throw)
const { ok, errors } = schema.validate(doc);

// Hard validation (throw ValidationError)
schema.validateOrThrow(doc);

// Apply default values
schema.applyDefaults(doc);
```

### TTL Index

```typescript
import { TTLIndex } from 'ovndb';

const sessions = await db.collection('sessions');
const ttl      = new TTLIndex(sessions, {
  field:         'expiresAt',
  checkInterval: 60_000,     // cek setiap 1 menit
  batchSize:     1_000,      // hapus max 1000 per siklus
  onPurge:       (deleted, ms, total) => console.log(`Purged ${deleted} in ${ms}ms`),
});
ttl.start();

// Insert dokumen dengan TTL
await sessions.insertOne({
  sessionId: 'abc123',
  userId:    'u1',
  data:      { ... },
  expiresAt: TTLIndex.expiresIn(30, 'minutes'),
});

// Stop worker saat shutdown
ttl.stop();
```

### Change Stream (CDC)

```typescript
const stream = users.watch({
  filter: { active: true }, // optional: hanya emit event untuk dokumen tertentu
});

stream.on('insert',  event => io.emit('newUser', event.fullDocument));
stream.on('update',  event => cache.invalidate(event.documentKey._id));
stream.on('delete',  event => cleanup(event.documentKey._id));
stream.on('change',  event => console.log(event.operationType));

// Unsubscribe
stream.close();
```

### Observability

```typescript
import { getObservability } from 'ovndb';

const obs = getObservability({ slowQueryThresholdMs: 50 });

// Wrap operasi dengan timing otomatis
const docs = await obs.measure('find', 'users', () =>
  users.find({ city: 'Jakarta' })
);

// Report lengkap
const report = obs.report();
console.log(report.collections[0].p99Ms); // P99 per operasi

// Prometheus endpoint
app.get('/metrics', (req, res) => {
  res.type('text/plain').send(obs.toPrometheus());
});
```

---

## Architecture

```
OvnDB (entry point)
├── Collection / CollectionV2
│   ├── QueryPlanner (cost-based index selection)
│   ├── AggregationPipeline ($match $group $sort $lookup …)
│   ├── ChangeStreamRegistry (CDC / watch())
│   └── SecondaryIndexManager
│
├── StorageEngine
│   ├── SegmentManager       ← petabyte-scale multi-file storage
│   │   └── .seg-0000.ovn, .seg-0001.ovn, …
│   ├── PagedBPlusTree       ← on-disk index, 256 MB buffer pool
│   │   └── PageManager (LRU buffer pool → disk)
│   ├── WAL (group commit)   ← crash recovery, ~10× throughput vs v1
│   ├── LRUCache<string,Buffer> (hot document cache)
│   └── MVCCManager          ← Snapshot Isolation transactions
│
├── CryptoLayer (AES-256-GCM per-record)
├── SchemaValidator (runtime validation)
├── TTLIndex (auto-expiry background worker)
└── Observability (P95/P99 metrics, Prometheus)
```

### File Layout

```
data/
└── users/                          ← satu direktori per collection
    ├── users.manifest.json         ← daftar semua segment + metadata
    ├── users.seg-0000.ovn          ← segment 0 (max 512 MB)
    ├── users.seg-0001.ovn          ← segment 1 (auto-created)
    ├── users.ovni                  ← on-disk B+ Tree index (page file)
    ├── users.wal                   ← Write-Ahead Log (di-clear setelah flush)
    ├── users.idx.city              ← secondary index untuk field 'city'
    └── .salt                       ← PBKDF2 salt (jika enkripsi aktif)
```

### Record Format (on-disk)

```
[1 byte]   status   — 0x01 ACTIVE, 0x00 DELETED
[8 bytes]  txId     — MVCC version (BigInt uint64 LE)
[4 bytes]  dataLen  — payload length (uint32 LE)
[N bytes]  data     — JSON payload (atau ciphertext jika enkripsi aktif)
[4 bytes]  CRC32    — checksum (covers semua field sebelumnya)
```

---

## Performance

Diukur pada SSD NVMe, Node.js v22, 100K records:

| Operation | Throughput |
|---|---|
| `insertOne` | ~20,000 ops/s |
| `findOne` by `_id` (cache hit) | ~35,000 ops/s |
| `updateOne` | ~15,000 ops/s |
| `deleteOne` | ~22,000 ops/s |
| Full scan (100K docs) | ~120 ms |
| `aggregate` ($match + $group) | ~90 ms |

Jalankan benchmark sendiri:
```bash
npm run bench          # 100K records (default)
npm run bench:1m       # 1M records
BENCH_N=500000 npm run bench
```

---

## Running Tests

```bash
npm install

# Semua test
npm test

# Unit test saja (cepat, tidak butuh disk)
npm run test:unit

# Integration test (butuh tmpdir)
npm run test:integration

# Watch mode untuk development
npm run test:watch
```

---

## Environment Variables

| Variable | Default | Deskripsi |
|---|---|---|
| `OVNDB_LOG` | `info` | Log level: `debug` `info` `warn` `error` `silent` |
| `NODE_ENV` | — | Set ke `production` untuk JSON log output |
| `BENCH_N` | `100000` | Jumlah record untuk benchmark |

---

## License

MIT
