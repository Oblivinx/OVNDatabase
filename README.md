# OvnDB v1.0

High-performance embedded NoSQL database for Node.js — zero external dependencies.
Built for WhatsApp bots and apps with **100k+ users** and **hundreds of millions of daily operations**.

## Quick Start

```bash
npm install
npm test          # functional tests + benchmark
npm run test:crypto   # crypto + index + relations tests
npm run demo      # WhatsApp bot simulation
npm run test:stress   # 1M record stress test
```

## Features

| Feature | Detail |
|---|---|
| **B+ Tree index** | O(log n) reads, height ≤ 3 for 1M records |
| **WAL crash recovery** | Batched mem buffer, single `writeSync` per flush |
| **LRU Cache** | O(1) get/set, rolling hit-rate window |
| **AES-256-GCM encryption** | Per-record IV, PBKDF2 key derivation |
| **Secondary indexes** | Non-`_id` field lookups with unique constraint |
| **Relations** | `populate()` + `populateMany()` with ID deduplication |
| **MongoDB-style API** | `$eq, $gt, $in, $regex, $and, $or, $not, $inc, $push...` |
| **Auto compaction** | Triggered at 35% fragmentation |
| **Zero dependencies** | Pure Node.js + TypeScript |

## Performance (1M records)

| Operation | Throughput |
|---|---|
| Insert | ~19,000 /s |
| Read by `_id` (cache) | ~32,000 /s |
| Update | ~15,000 /s |
| Delete | ~25,000 /s |
| Full scan (1M) | ~81ms |
| Cold open (1M) | ~768ms |

## Usage

```typescript
import { OvnDB } from 'ovndb';

const db    = await OvnDB.open('./data');
const users = await db.collection<User>('users');

// Insert
const user = await users.insertOne({ _id: phone, phone, name: 'Budi', points: 0 });

// Find
const found = await users.findOne({ _id: phone });           // O(1) cache
const vips  = await users.find({ points: { $gte: 1000 } }, { sort: { points: -1 }, limit: 10 });

// Update
await users.updateOne({ _id: phone }, { $inc: { points: 50 } });
await users.upsertOne({ _id: phone }, { $inc: { points: 10 } }); // insert or update

// Delete
await users.deleteOne({ _id: phone });

await db.close();
```

## CollectionV2 — Encryption + Secondary Index

```typescript
import { OvnDB, cryptoFromPassphrase } from 'ovndb';

const db = await OvnDB.open('./data');

const users = await db.collectionV2<User>('users', {
  crypto:  cryptoFromPassphrase(process.env.DB_KEY!, './data'),
  indexes: [{ field: 'phone', unique: true }],
});

// Transparent encryption on every read/write
// Phone lookup via secondary index O(log n) instead of O(n) full scan
const user = await users.findOne({ phone: '628123456789' });
```

## Relations

```typescript
import { RelationManager } from 'ovndb/relations';

const rel = new RelationManager();
rel.register('users', users);

// Single populate
const msg  = await messages.findOne({ _id: id });
const full = await rel.populate(msg, { userId: 'users' });
// full.userId is now a User object

// Batch populate — deduplicates IDs, much faster
const msgs = await messages.find({ groupId: 'xyz' });
const pop  = await rel.populateMany(msgs, { userId: 'users' });
```

## File Structure

Each collection creates 3 files:

| File | Purpose |
|---|---|
| `{name}.ovn` | Main data (append-only binary records) |
| `{name}.ovni` | B+ Tree index snapshot |
| `{name}.wal` | Write-Ahead Log (cleared after each flush) |
| `{name}.idx.{field}` | Secondary index files (CollectionV2 only) |

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `OVNDB_LOG` | `info` | Log level: `debug`, `info`, `warn`, `error`, `silent` |
| `NODE_ENV` | - | Set to `production` for JSON log output |

## License

MIT
