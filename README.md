# OvnDB v4.0

**High-performance embedded NoSQL database for Node.js — scales to trillions of records.**

Zero external dependencies. Pure TypeScript. MongoDB-compatible API.

---

## What's new in v4.0

| Feature | v1 | v2 | v3 | **v4** |
|---|---|---|---|---|
| **Index storage** | In-memory B+ Tree | On-disk Paged B+ Tree | Same | **Compound Index** (multi-field) |
| **Data files** | Single `.ovn` | Multi-segment (512 MB) | Parallel compaction | **Bloom Filter** per-segment |
| **WAL** | 1 `fdatasync` per op | Group commit | Group commit + rotation | Same |
| **Transactions** | Compensating writes | MVCC Snapshot Isolation | GC improvements | **Savepoints** (partial rollback) |
| **Aggregation** | None | Full pipeline | `$bucket` `$facet` `$densify` | Same |
| **Full-text Search** | None | None | None | **FTS Index** — `createTextIndex` + `$text` |
| **Import/Export** | None | None | None | **NDJSON + JSON** export/import |
| **Encryption** | None | AES-256-GCM | Field-level encryption | Same |
| **Security** | Basic | Prototype pollution guards | ReDoS protection | **`__proto__` injection detection** via prototype chain |

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
// Open database
const db = await OvnDB.open('./data', {
  cacheBytes: 256 * 1024 * 1024,  // byte-aware LRU cache (default 256 MB)
  fileLock:  true,                // prevent multi-process corruption
  mkdirp:    true,                // auto-create directory
});

// Standard collection
const col = await db.collection<MyDoc>('collection-name');

// Encrypted collection with secondary indexes
const col = await db.collectionV2<MyDoc>('col', {
  crypto:  await cryptoFromPassphrase(process.env.DB_KEY!, './data'),
  indexes: [{ field: 'email', unique: true }],
});

// Transaction (MVCC Snapshot Isolation)
const tx = db.beginTransaction();
tx.insert(col1, { name: 'Budi' });
tx.update(col2, { _id: id }, { $inc: { balance: -100 } });
await tx.commit(); // auto-rollback on failure

// Database status
const status = await db.status();
// → { path, openedAt, collections, isHealthy }

// List & manage collections
const names   = await db.listCollections();
const exists  = await db.collectionExists('users');
await db.dropCollection('old-collection');

// Backup
await db.backup('./backup-dir');

// Maintenance
await db.flushAll();
await db.close();
```

### Collection

```typescript
// ── Insert ──────────────────────────────────────────────────
const doc  = await col.insertOne({ name: 'Budi', points: 0 });         // _id auto-generated
const doc  = await col.insertOne({ _id: 'u1', name: 'Siti', points: 0 });
const docs = await col.insertMany([{ name: 'A' }, { name: 'B' }]);

// ── Find ────────────────────────────────────────────────────
const doc  = await col.findOne({ _id: 'u1' });
const doc  = await col.findById('u1');                                  // shorthand
const docs = await col.findManyById(['u1', 'u2']);                      // batch lookup
const docs = await col.find({ city: 'Jakarta' }, { sort: { points: -1 }, limit: 10 });
const docs = await col.find({ points: { $gte: 100, $lte: 500 } });
const docs = await col.find({ $or: [{ city: 'Jakarta' }, { active: true }] });
const docs = await col.find({ tags: { $in: ['nodejs', 'typescript'] } });
const docs = await col.find({ name: { $regex: /^Budi/i } });

// Cursor pagination (more efficient than skip for large datasets)
const page1 = await col.find({}, { limit: 20, sort: { _id: 1 } });
const page2 = await col.find({}, { limit: 20, sort: { _id: 1 }, after: page1.at(-1)?._id });

// Projection
const docs = await col.find({ city: 'Jakarta' }, { projection: { name: 1, city: 1 } });

// ── Update ──────────────────────────────────────────────────
await col.updateOne({ _id: 'u1' }, { $inc: { points: 50 } });
await col.updateOne({ _id: 'u1' }, { $set: { name: 'New Name' } });
await col.updateOne({ _id: 'u1' }, { $push: { tags: 'nodejs' } });
await col.updateMany({ active: false }, { $set: { active: true } });
const doc = await col.upsertOne({ _id: 'u1' }, { $set: { name: 'Budi' } });
const doc = await col.findOneAndUpdate({ _id: 'u1' }, { $inc: { points: 1 } });

// Replace entire document
await col.replaceOne({ _id: 'u1' }, { name: 'New', points: 0 });

// ── Delete ──────────────────────────────────────────────────
await col.deleteOne({ _id: 'u1' });
await col.deleteMany({ active: false });
const doc = await col.findOneAndDelete({ _id: 'u1' });

// ── BulkWrite ───────────────────────────────────────────────
const result = await col.bulkWrite([
  { op: 'insertOne',  doc: { _id: 'u1', name: 'Alice' } },
  { op: 'updateOne',  filter: { _id: 'u1' }, spec: { $set: { verified: true } } },
  { op: 'deleteOne',  filter: { _id: 'u2' } },
]);
// result = { ops, insertedCount, updatedCount, deletedCount, errors }

// ── Query Info ──────────────────────────────────────────────
const count  = await col.countDocuments({ city: 'Jakarta' });
const exists = await col.exists({ _id: 'u1' });
const cities = await col.distinct('city');
const plan   = col.explain({ city: 'Jakarta' }); // query plan without execution
const stats  = await col.stats();

// ── Index ───────────────────────────────────────────────────
// Single field
await col.createIndex({ field: 'email', unique: true });
await col.createIndex({ field: 'city',  unique: false, sparse: true });
await col.createIndex({ field: 'score', unique: false, partial: { active: true } });

// v4.0: Compound index (multi-field)
await col.createIndex({ field: ['city', 'active'], unique: false });
await col.createIndex({ field: ['name', 'role'],   unique: true  });
// Compound queries now use the index automatically:
const docs = await col.find({ city: 'Jakarta', active: true });

// ── Flush & Compact ─────────────────────────────────────────
await col.flush();
await col.compact();
```

### Full-text Search (v4.0)

```typescript
// Create text index on a field
await col.createTextIndex('name');
await col.createTextIndex('description');

// Search — single word
const docs = await col.find({ $text: 'budi' });

// Search — multiple words (AND semantics: all words must appear)
const docs = await col.find({ $text: 'budi jakarta' });

// Combined with other filters
const docs = await col.find({ $text: 'santoso', city: 'Jakarta' });

// findOne + $text
const doc = await col.findOne({ $text: 'alice' });
```

### Savepoints (v4.0)

```typescript
const tx = db.beginTransaction();

// Stage some operations
tx.insert(col, { _id: 'a', balance: 100 });
tx.insert(col, { _id: 'b', balance: 200 });

// Create a savepoint
tx.savepoint('before-extra');

// More operations after savepoint
tx.insert(col, { _id: 'c', balance: 999 });

// Rollback to savepoint — undoes everything after 'before-extra'
await tx.rollbackTo('before-extra');

// Transaction is still pending — commit remaining ops or add more
await tx.commit(); // commits 'a' and 'b', not 'c'
```

### Import / Export (v4.0)

```typescript
// Export to NDJSON (streaming, memory-efficient)
const count = await col.exportTo('./backup.ndjson');

// Export to JSON array
const count = await col.exportTo('./backup.json', { format: 'json' });

// Export with projection
const count = await col.exportTo('./names.ndjson', {
  projection: { name: 1, city: 1 },
});

// Import from NDJSON
const result = await col.importFrom('./backup.ndjson');
// result = { total, inserted, updated, errors: [] }

// Import with upsert (update if _id exists)
const result = await col.importFrom('./data.ndjson', { mode: 'upsert' });

// Import with error recovery
const result = await col.importFrom('./data.json', {
  format: 'json',
  continueOnError: true,
});
console.log(`${result.inserted} inserted, ${result.errors.length} errors`);
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

// $lookup (cross-collection join)
const joined = await col.aggregate([
  { $lookup: { from: 'orders', localField: '_id', foreignField: 'userId', as: 'orders' } },
]);

// $replaceRoot
const flat = await col.aggregate([
  { $replaceRoot: { newRoot: '$profile' } },
]);

// $bucket
const histogram = await col.aggregate([
  { $bucket: { groupBy: '$age', boundaries: [0, 18, 30, 50, 100], default: 'other' } },
]);

// $facet — multiple pipelines in parallel
const facets = await col.aggregate([
  { $facet: {
    byCity:  [{ $group: { _id: '$city', count: { $sum: 1 } } }],
    byAge:   [{ $bucket: { groupBy: '$age', boundaries: [0, 25, 50] } }],
  }},
]);

// $densify — fill gaps in sequences
const filled = await col.aggregate([
  { $densify: { field: 'timestamp', range: { step: 1, unit: 'hour' } } },
]);

// $setWindowFields — window functions
const ranked = await col.aggregate([
  { $setWindowFields: {
    partitionBy: '$city',
    sortBy: { score: -1 },
    output: { rank: { $rank: {} } },
  }},
]);
```

### Query Operators

| Operator | Description |
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
| `$type` | Type checking |
| `$mod` | Modulo operation |
| **`$text`** | **Full-text search (v4.0)** |

### Update Operators

| Operator | Description |
|---|---|
| `$set` `$unset` | Set / Remove field |
| `$inc` | Increment number |
| `$mul` | Multiply number |
| `$min` `$max` | Set if less/greater |
| `$push` | Append to array (+ `$each` `$sort` `$slice`) |
| `$pull` | Remove from array |
| `$addToSet` | Add if not exists |
| `$rename` | Rename field |
| `$setOnInsert` | Set only during insert (upsert) |

### Aggregation Expression Operators

| Operator | Description |
|---|---|
| `$add` `$subtract` `$multiply` `$divide` | Arithmetic |
| `$concat` | String concatenation |
| `$cond` | Conditional (`if`/`then`/`else`) |
| `$ifNull` | Null coalescing |
| `$sum` `$avg` `$min` `$max` | Group accumulators |
| `$count` `$first` `$last` `$push` `$addToSet` | Group accumulators |
| `$rank` `$denseRank` | Window functions |

### Encryption (CollectionV2)

```typescript
import { OvnDB, cryptoFromPassphrase } from 'ovndb';

const db  = await OvnDB.open('./data');
const key = await cryptoFromPassphrase(process.env.DB_KEY!, './data');

const users = await db.collectionV2<User>('users', {
  crypto:  key,                                          // AES-256-GCM per-record
  indexes: [{ field: 'phone', unique: true }],
});

// API identical to Collection — encryption/decryption is transparent
const user = await users.findOne({ phone: '628123' });

// Key rotation — re-encrypt all records with new key
const newKey = await cryptoFromPassphrase('new-passphrase', './data', 1);
await users.rotateEncryptionKey(newKey);
```

#### Field-Level Encryption

```typescript
import { CryptoLayer, FieldCrypto } from 'ovndb';

const layer = CryptoLayer.fromKey(cryptoKey, 0);
const fc    = new FieldCrypto(layer);

// Encrypt only sensitive fields — other fields remain queryable
const encrypted = fc.encryptFields(doc, ['ssn', 'phone']);
const decrypted = fc.decryptFields(encrypted);
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

// Soft validation (no throw)
const { ok, errors } = schema.validate(doc);

// Hard validation (throws ValidationError)
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
  checkInterval: 60_000,     // check every 1 minute
  batchSize:     1_000,      // delete max 1000 per cycle
  onPurge:       (deleted, ms, total) => console.log(`Purged ${deleted} in ${ms}ms`),
});
ttl.start();

// Insert document with TTL
await sessions.insertOne({
  sessionId: 'abc123',
  userId:    'u1',
  data:      { /* ... */ },
  expiresAt: TTLIndex.expiresIn(30, 'minutes'),
});

// Stop worker on shutdown
ttl.stop();
```

### Change Stream (CDC)

```typescript
const stream = users.watch({
  filter: { active: true }, // optional: only emit events for matching docs
});

stream.on('insert',  event => io.emit('newUser', event.fullDocument));
stream.on('update',  event => cache.invalidate(event.documentKey._id));
stream.on('delete',  event => cleanup(event.documentKey._id));
stream.on('change',  event => console.log(event.operationType));

// Unsubscribe
stream.close();
```

### RelationManager

```typescript
import { RelationManager } from 'ovndb';

const rel = new RelationManager();
rel.register('users',  usersCol);
rel.register('groups', groupsCol);

// Populate single document
const msg  = await messages.findOne({ _id: id });
const full = await rel.populate(msg, { userId: 'users', groupId: 'groups' });
// full.userId is now a User object, not a string ID

// Batch populate — much more efficient than loop populate()
const msgs = await messages.find({ groupId: 'xyz' });
const pop  = await rel.populateMany(msgs, { userId: 'users' });
```

### MigrationRunner

```typescript
import { MigrationRunner } from 'ovndb';

const migration = new MigrationRunner(db, {
  version: 2,
  up: async (db) => {
    const users = await db.collection('users');
    await users.updateMany({}, { $set: { tier: 'free' } });
  },
});

await migration.run(); // only runs if current version < target version
```

### Observability

```typescript
import { getObservability } from 'ovndb';

const obs = getObservability({ slowQueryThresholdMs: 50 });

// Wrap operations with auto-timing
const docs = await obs.measure('find', 'users', () =>
  users.find({ city: 'Jakarta' })
);

// Full report
const report = obs.report();
console.log(report.collections[0].p99Ms); // P99 per operation

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
│   ├── AggregationPipeline ($match $group $sort $lookup $bucket $facet …)
│   ├── ChangeStreamRegistry (CDC / watch())
│   ├── SecondaryIndexManager     ← single-field + compound indexes
│   │   └── compound key = "val1\x00val2" (NUL separator)
│   ├── FTSIndex (v4.0)           ← inverted index per field, JSON-persisted
│   ├── importFrom / exportTo (v4.0)
│   └── BulkWrite engine
│
├── StorageEngine
│   ├── SegmentManager       ← petabyte-scale multi-file storage
│   │   ├── BloomFilter (v4.0) ← per-segment, FNV-1a double-hash
│   │   └── .seg-0000.ovn, .seg-0001.ovn, …
│   ├── PagedBPlusTree       ← on-disk index, 256 MB buffer pool
│   │   └── PageManager (LRU buffer pool → disk)
│   ├── WAL (group commit)   ← crash recovery, ~10× throughput vs v1
│   │   └── auto-rotation at 256 MB
│   ├── LRUCache<string,Buffer> (byte-aware doc cache, 256 MB)
│   └── MVCCManager          ← Snapshot Isolation + Savepoints (v4.0)
│
├── CryptoLayer (AES-256-GCM per-record)
│   ├── FieldCrypto (per-field encryption)
│   └── Key versioning + rotation
├── SchemaValidator (runtime validation + ReDoS protection)
├── TTLIndex (auto-expiry background worker)
├── RelationManager (cross-collection JOIN / populate)
├── MigrationRunner (versioned schema migrations)
└── Observability (P95/P99 metrics, Prometheus export)
```

### File Layout

```
data/
└── users/                          ← one directory per collection
    ├── users.manifest.json         ← segment list + metadata
    ├── users.seg-0000.ovn          ← segment 0 (max 512 MB)
    ├── users.seg-0001.ovn          ← segment 1 (auto-created)
    ├── users.ovni                  ← on-disk B+ Tree index (page file)
    ├── users.wal                   ← Write-Ahead Log (cleared after flush)
    ├── users.idx.city              ← secondary index for field 'city'
    └── .salt                       ← PBKDF2 salt (if encryption enabled)
```

### Record Format (on-disk)

```
[1 byte]   status   — 0x01 ACTIVE, 0x00 DELETED
[8 bytes]  txId     — MVCC version (BigInt uint64 LE)
[4 bytes]  dataLen  — payload length (uint32 LE)
[N bytes]  data     — JSON payload (or ciphertext if encryption enabled)
[4 bytes]  CRC32    — checksum (covers all previous fields)
```

---

## Security

OvnDB includes defense-in-depth security:

- **Prototype pollution protection** — all keys checked against `__proto__`, `constructor`, `prototype`
- **ReDoS protection** — regex patterns validated before compilation (backreference + quantifier detection)
- **Path traversal prevention** — collection names and backup paths are validated
- **Input sanitization** — document size limits, field depth limits, query filter validation
- **Backup confinement** — backup destination cannot be inside the data directory
- **ID generation** — uses `crypto.randomBytes()` (CSPRNG), not `Math.random()`
- **AES-256-GCM** — authenticated encryption with automatic integrity verification

---

## Performance

Measured on NVMe SSD, Node.js v22, 100K records:

| Operation | Throughput |
|---|---|
| `insertOne` | ~20,000 ops/s |
| `findOne` by `_id` (cache hit) | ~35,000 ops/s |
| `updateOne` | ~15,000 ops/s |
| `deleteOne` | ~22,000 ops/s |
| Full scan (100K docs) | ~120 ms |
| `aggregate` ($match + $group) | ~90 ms |

Run benchmark yourself:
```bash
npm run bench          # 10K records (default)
npm run bench:50k      # 50K records
BENCH_N=500000 npm run bench
```

---

## Running Tests

```bash
npm install

# All tests (unit + integration + security)
npm test

# Unit tests only (fast, pure logic)
npm run test:unit

# Integration tests (uses tmpdir)
npm run test:integration

# Security tests
npm run test:security
```

### Test Coverage

| Test Suite | Modules Covered |
|---|---|
| `crc32.test.ts` | CRC32 checksum utility |
| `lru-cache.test.ts` | Byte-aware LRU cache |
| `filter.test.ts` | Query filter engine, update operators, projection |
| `schema-validator.test.ts` | Schema validation, fluent builder |
| `aggregation.test.ts` | Aggregation pipeline stages |
| `crypto-layer.test.ts` | AES-256-GCM encryption, FieldCrypto |
| `mvcc.test.ts` | MVCC snapshot isolation, conflict detection |
| `id-generator.test.ts` | ID generation, uniqueness, CSPRNG |
| `relation-manager.test.ts` | Cross-collection relations |
| `collection.test.ts` | Full CRUD + **compound index, FTS, import/export** (v4.0) |
| `transaction.test.ts` | Transaction commit/rollback + **savepoints** (v4.0) |
| `security.test.ts` | 25+ security hardening scenarios |

---

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `OVNDB_LOG` | `info` | Log level: `debug` `info` `warn` `error` `silent` |
| `NODE_ENV` | — | Set to `production` for JSON log output |
| `BENCH_N` | `10000` | Number of records for benchmark |

---

## License

[MIT](LICENCE) © 2026 Natz6N
