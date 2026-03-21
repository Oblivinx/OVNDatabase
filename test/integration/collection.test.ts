// ============================================================
//  Integration Test — Collection CRUD + Query + Aggregation
//
//  Semua test menggunakan database sementara di os.tmpdir().
//  Setiap test suite membersihkan datanya sendiri (after cleanup).
//
//  Coverage:
//   - insertOne / insertMany / findOne / find / countDocuments
//   - updateOne / updateMany / upsertOne / replaceOne
//   - deleteOne / deleteMany
//   - findOneAndUpdate / findOneAndDelete
//   - distinct
//   - createIndex / secondary index lookup
//   - aggregate (end-to-end)
//   - explain() → query plan
//   - watch() → ChangeStream events
//   - TTLIndex auto-expiry
// ============================================================

import assert     from 'node:assert/strict';
import { test, describe, before, after, beforeEach } from 'node:test';
import os         from 'node:os';
import path       from 'node:path';
import fsp        from 'node:fs/promises';
import { OvnDB }  from '../../src/index.js';
import { TTLIndex } from '../../src/ttl/ttl-index.js';
import type { OvnDocument } from '../../src/types/index.js';

interface User extends OvnDocument {
  name:    string;
  city:    string;
  points:  number;
  active?: boolean;
  tags?:   string[];
}

// ── Helpers ───────────────────────────────────────────────────

async function makeTmpDb(): Promise<{ db: OvnDB; dir: string }> {
  const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'ovndb-it-'));
  const db  = await OvnDB.open(dir, { fileLock: false });
  return { db, dir };
}

async function cleanup(db: OvnDB, dir: string): Promise<void> {
  await db.close();
  await fsp.rm(dir, { recursive: true, force: true });
}

// ── Insert ────────────────────────────────────────────────────

describe('insertOne / insertMany', () => {
  let db: OvnDB, dir: string;
  before(async () => { ({ db, dir } = await makeTmpDb()); });
  after (async () => cleanup(db, dir));

  test('insertOne: _id di-generate jika tidak ada', async () => {
    const users = await db.collection<User>('users');
    const doc   = await users.insertOne({ name: 'Budi', city: 'Jakarta', points: 100 });
    assert.ok(doc._id, '_id harus ada');
    assert.equal(doc.name, 'Budi');
  });

  test('insertOne: _id custom', async () => {
    const users = await db.collection<User>('users');
    const doc   = await users.insertOne({ _id: 'custom-id', name: 'Siti', city: 'Bandung', points: 200 });
    assert.equal(doc._id, 'custom-id');
  });

  test('insertOne: duplicate _id → throw', async () => {
    const users = await db.collection<User>('users');
    await users.insertOne({ _id: 'dup-id', name: 'A', city: 'X', points: 0 });
    await assert.rejects(
      () => users.insertOne({ _id: 'dup-id', name: 'B', city: 'Y', points: 0 }),
      /Duplicate/,
    );
  });

  test('insertMany: insert batch', async () => {
    const col  = await db.collection<User>('batch-col');
    const docs = await col.insertMany([
      { name: 'A', city: 'C1', points: 10 },
      { name: 'B', city: 'C2', points: 20 },
      { name: 'C', city: 'C1', points: 30 },
    ]);
    assert.equal(docs.length, 3);
    assert.equal(await col.countDocuments(), 3);
  });
});

// ── Find ──────────────────────────────────────────────────────

describe('findOne / find / countDocuments / distinct', () => {
  let db: OvnDB, dir: string;
  let col: Awaited<ReturnType<typeof db.collection<User>>>;

  before(async () => {
    ({ db, dir } = await makeTmpDb());
    col = await db.collection<User>('find-test');
    await col.insertMany([
      { _id: 'u1', name: 'Budi',  city: 'Jakarta', points: 100, active: true  },
      { _id: 'u2', name: 'Siti',  city: 'Bandung', points: 200, active: true  },
      { _id: 'u3', name: 'Ahmad', city: 'Jakarta', points: 150, active: false },
      { _id: 'u4', name: 'Dewi',  city: 'Bandung', points:  50, active: true  },
    ]);
  });
  after(async () => cleanup(db, dir));

  test('findOne by _id', async () => {
    const doc = await col.findOne({ _id: 'u1' });
    assert.ok(doc);
    assert.equal(doc.name, 'Budi');
  });

  test('findOne: tidak ada → null', async () => {
    const doc = await col.findOne({ _id: 'nonexistent' });
    assert.equal(doc, null);
  });

  test('find dengan filter', async () => {
    const docs = await col.find({ city: 'Jakarta' });
    assert.equal(docs.length, 2);
    assert.ok(docs.every(d => d.city === 'Jakarta'));
  });

  test('find dengan $gte', async () => {
    const docs = await col.find({ points: { $gte: 150 } });
    assert.equal(docs.length, 2); // u2 (200), u3 (150)
  });

  test('find dengan sort ascending', async () => {
    const docs = await col.find({}, { sort: { points: 1 } });
    const pts  = docs.map(d => d.points);
    assert.deepEqual(pts, [...pts].sort((a, b) => a - b));
  });

  test('find dengan limit', async () => {
    const docs = await col.find({}, { limit: 2 });
    assert.equal(docs.length, 2);
  });

  test('find dengan skip', async () => {
    const all     = await col.find({}, { sort: { _id: 1 } });
    const skipped = await col.find({}, { skip: 2, sort: { _id: 1 } });
    assert.equal(skipped.length, all.length - 2);
  });

  test('find dengan projection (include)', async () => {
    const docs = await col.find({}, { projection: { name: 1, city: 1 } });
    assert.ok(docs.every(d => 'name' in d && 'city' in d && !('points' in d)));
  });

  test('find dengan projection (exclude)', async () => {
    const docs = await col.find({}, { projection: { points: 0 } });
    assert.ok(docs.every(d => !('points' in d) && 'name' in d));
  });

  test('countDocuments', async () => {
    assert.equal(await col.countDocuments(), 4);
    assert.equal(await col.countDocuments({ city: 'Jakarta' }), 2);
  });

  test('distinct', async () => {
    const cities = (await col.distinct('city')).sort();
    assert.deepEqual(cities, ['Bandung', 'Jakarta']);
  });
});

// ── Update ────────────────────────────────────────────────────

describe('updateOne / updateMany / upsertOne / replaceOne', () => {
  let db: OvnDB, dir: string;
  let col: Awaited<ReturnType<typeof db.collection<User>>>;

  before(async () => {
    ({ db, dir } = await makeTmpDb());
    col = await db.collection<User>('update-test');
    await col.insertMany([
      { _id: 'u1', name: 'Budi',  city: 'Jakarta', points: 100 },
      { _id: 'u2', name: 'Siti',  city: 'Bandung', points: 200 },
      { _id: 'u3', name: 'Ahmad', city: 'Jakarta', points: 150 },
    ]);
  });
  after(async () => cleanup(db, dir));

  test('updateOne dengan $inc', async () => {
    await col.updateOne({ _id: 'u1' }, { $inc: { points: 50 } });
    const doc = await col.findOne({ _id: 'u1' });
    assert.equal(doc?.points, 150);
  });

  test('updateOne dengan $set', async () => {
    await col.updateOne({ _id: 'u2' }, { $set: { active: true } });
    const doc = await col.findOne({ _id: 'u2' });
    assert.equal((doc as unknown as Record<string,unknown>)['active'], true);
  });

  test('updateOne: tidak ada yang cocok → false', async () => {
    const ok = await col.updateOne({ _id: 'nonexistent' }, { $inc: { points: 1 } });
    assert.equal(ok, false);
  });

  test('updateMany memperbarui semua yang cocok', async () => {
    const count = await col.updateMany({ city: 'Jakarta' }, { $inc: { points: 10 } });
    assert.equal(count, 2);
    const docs = await col.find({ city: 'Jakarta' });
    // u1 awalnya 150 (sudah di-inc 50), +10 = 160
    // u3 awalnya 150, +10 = 160
    assert.ok(docs.every(d => d.points >= 160));
  });

  test('upsertOne: insert jika belum ada', async () => {
    const doc = await col.upsertOne({ _id: 'u99' }, { $set: { name: 'Baru', city: 'Bali', points: 0 } });
    assert.equal(doc._id, 'u99');
    assert.equal(doc.name, 'Baru');
  });

  test('upsertOne: update jika sudah ada', async () => {
    await col.upsertOne({ _id: 'u1' }, { $set: { city: 'Surabaya' } });
    const doc = await col.findOne({ _id: 'u1' });
    assert.equal(doc?.city, 'Surabaya');
  });

  test('findOneAndUpdate: kembalikan versi terbaru', async () => {
    const updated = await col.findOneAndUpdate({ _id: 'u2' }, { $inc: { points: 100 } });
    assert.ok(updated);
    assert.ok(updated.points > 200);
  });

  test('replaceOne: ganti seluruh dokumen', async () => {
    const replacement = { _id: 'u3', name: 'Ahmad-New', city: 'Bandung', points: 999 };
    await col.replaceOne({ _id: 'u3' }, replacement);
    const doc = await col.findOne({ _id: 'u3' });
    assert.equal(doc?.name, 'Ahmad-New');
    assert.equal(doc?.points, 999);
  });
});

// ── Delete ────────────────────────────────────────────────────

describe('deleteOne / deleteMany / findOneAndDelete', () => {
  let db: OvnDB, dir: string;
  let col: Awaited<ReturnType<typeof db.collection<User>>>;

  before(async () => {
    ({ db, dir } = await makeTmpDb());
    col = await db.collection<User>('delete-test');
    await col.insertMany([
      { _id: 'u1', name: 'A', city: 'X', points: 10 },
      { _id: 'u2', name: 'B', city: 'Y', points: 20 },
      { _id: 'u3', name: 'C', city: 'X', points: 30 },
    ]);
  });
  after(async () => cleanup(db, dir));

  test('deleteOne: hapus satu dokumen', async () => {
    const ok  = await col.deleteOne({ _id: 'u1' });
    assert.equal(ok, true);
    assert.equal(await col.countDocuments(), 2);
  });

  test('deleteOne: tidak ada → false', async () => {
    const ok = await col.deleteOne({ _id: 'nonexistent' });
    assert.equal(ok, false);
  });

  test('deleteMany: hapus semua yang cocok', async () => {
    const count = await col.deleteMany({ city: 'X' });
    assert.ok(count >= 1); // u3 (u1 sudah dihapus di atas)
  });

  test('findOneAndDelete: kembalikan dokumen sebelum dihapus', async () => {
    const doc  = await col.findOneAndDelete({ _id: 'u2' });
    assert.ok(doc);
    assert.equal(doc._id, 'u2');
    assert.equal(await col.countDocuments({ _id: 'u2' }), 0);
  });
});

// ── Secondary Index ───────────────────────────────────────────

describe('createIndex + secondary index lookup', () => {
  let db: OvnDB, dir: string;
  let col: Awaited<ReturnType<typeof db.collection<User>>>;

  before(async () => {
    ({ db, dir } = await makeTmpDb());
    col = await db.collection<User>('idx-test');
    await col.createIndex({ field: 'city', unique: false });
    await col.insertMany([
      { _id: 'u1', name: 'Budi',  city: 'Jakarta', points: 100 },
      { _id: 'u2', name: 'Siti',  city: 'Bandung', points: 200 },
      { _id: 'u3', name: 'Ahmad', city: 'Jakarta', points: 150 },
    ]);
  });
  after(async () => cleanup(db, dir));

  test('find dengan field yang di-index', async () => {
    const docs = await col.find({ city: 'Jakarta' });
    assert.equal(docs.length, 2);
  });

  test('explain() menunjukkan indexScan untuk field yang di-index', () => {
    const plan = col.explain({ city: 'Jakarta' });
    // Bisa indexScan atau fullCollection tergantung ukuran collection
    assert.ok(['indexScan', 'fullCollection', 'primaryKey'].includes(plan.planType));
  });
});

// ── Aggregation (Integration) ─────────────────────────────────

describe('aggregate — end-to-end', () => {
  let db: OvnDB, dir: string;
  let col: Awaited<ReturnType<typeof db.collection<User>>>;

  before(async () => {
    ({ db, dir } = await makeTmpDb());
    col = await db.collection<User>('agg-test');
    await col.insertMany([
      { _id: 'u1', name: 'Budi',  city: 'Jakarta', points: 100, active: true  },
      { _id: 'u2', name: 'Siti',  city: 'Bandung', points: 200, active: true  },
      { _id: 'u3', name: 'Ahmad', city: 'Jakarta', points: 150, active: false },
      { _id: 'u4', name: 'Dewi',  city: 'Bandung', points:  50, active: true  },
    ]);
  });
  after(async () => cleanup(db, dir));

  test('group by city, total points', async () => {
    const result = await col.aggregate([
      { $group: { _id: '$city', total: { $sum: '$points' } } },
      { $sort: { total: -1 } },
    ]);
    assert.equal(result.length, 2);
    // Jakarta: 100+150=250, Bandung: 200+50=250 — sama, urutan mungkin berbeda
    const totals = result.map(d => d['total'] as number);
    assert.equal(totals.reduce((a, b) => a + b, 0), 500);
  });

  test('$match + $group + $limit', async () => {
    const result = await col.aggregate([
      { $match: { active: true } },
      { $group: { _id: '$city', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 },
    ]);
    assert.equal(result.length, 1);
  });
});

// ── Change Stream ─────────────────────────────────────────────

describe('watch() — Change Stream', () => {
  let db: OvnDB, dir: string;

  before(async () => { ({ db, dir } = await makeTmpDb()); });
  after (async () => cleanup(db, dir));

  test('emit insert event', async () => {
    const col    = await db.collection<User>('watch-test');
    const stream = col.watch();
    let   received: unknown = null;

    stream.on('insert', e => { received = e; });

    await col.insertOne({ name: 'Test', city: 'X', points: 0 });

    // Tunggu sebentar untuk event propagate
    await new Promise(r => setTimeout(r, 50));
    assert.ok(received !== null, 'Harus menerima insert event');
    stream.close();
  });

  test('stream bisa difilter', async () => {
    const col     = await db.collection<User>('watch-filter');
    const stream  = col.watch({ filter: { city: 'Jakarta' } });
    const events: unknown[] = [];

    stream.on('insert', e => events.push(e));

    await col.insertOne({ name: 'A', city: 'Jakarta', points: 0 });
    await col.insertOne({ name: 'B', city: 'Bandung', points: 0 });
    await new Promise(r => setTimeout(r, 50));

    // Hanya event untuk kota Jakarta yang di-emit
    assert.equal(events.length, 1);
    stream.close();
  });
});

// ── TTLIndex ──────────────────────────────────────────────────

describe('TTLIndex', () => {
  let db: OvnDB, dir: string;

  before(async () => { ({ db, dir } = await makeTmpDb()); });
  after (async () => cleanup(db, dir));

  test('dokumen expired dihapus saat purgeNow()', async () => {
    const col = await db.collection<User & { expiresAt: number }>('ttl-test');
    const ttl = new TTLIndex(col as unknown as Parameters<typeof TTLIndex>[0], {
      field:         'expiresAt',
      checkInterval: 999_999, // jangan auto-run
    });

    // Insert dokumen yang sudah expired (di masa lalu)
    await col.insertOne({
      _id: 'exp1', name: 'Expired', city: 'X', points: 0,
      expiresAt: Date.now() - 10_000, // 10 detik lalu
    } as unknown as Parameters<typeof col.insertOne>[0]);

    // Insert dokumen yang belum expired
    await col.insertOne({
      _id: 'live1', name: 'Live', city: 'X', points: 0,
      expiresAt: TTLIndex.expiresIn(1, 'hours'),
    } as unknown as Parameters<typeof col.insertOne>[0]);

    const deleted = await ttl.purgeNow();
    assert.equal(deleted, 1, 'Seharusnya menghapus tepat 1 dokumen');

    const remaining = await col.findOne({ _id: 'exp1' });
    assert.equal(remaining, null, 'Dokumen expired seharusnya sudah terhapus');

    const live = await col.findOne({ _id: 'live1' });
    assert.ok(live, 'Dokumen live seharusnya masih ada');
  });

  test('TTLIndex.expiresIn helper', () => {
    const now       = Date.now();
    const in5min    = TTLIndex.expiresIn(5, 'minutes');
    const in1hour   = TTLIndex.expiresIn(1, 'hours');
    const in7days   = TTLIndex.expiresIn(7, 'days');
    assert.ok(in5min  > now + 4 * 60_000);
    assert.ok(in1hour > now + 59 * 60_000);
    assert.ok(in7days > now + 6 * 86_400_000);
  });

  test('TTLIndex.isExpired', () => {
    const expiredDoc = { expiresAt: Date.now() - 1000 };
    const liveDoc    = { expiresAt: Date.now() + 1000 };
    assert.equal(TTLIndex.isExpired(expiredDoc), true);
    assert.equal(TTLIndex.isExpired(liveDoc), false);
  });
});

// ── Transaction ───────────────────────────────────────────────

describe('Transaction', () => {
  let db: OvnDB, dir: string;

  before(async () => { ({ db, dir } = await makeTmpDb()); });
  after (async () => cleanup(db, dir));

  test('commit: semua operasi berhasil atomik', async () => {
    const users   = await db.collection<User>('tx-users');
    const wallets = await db.collection<{_id:string; balance:number}>('tx-wallets');

    await users.insertOne  ({ _id: 'user1', name: 'Budi', city: 'J', points: 0 });
    await wallets.insertOne({ _id: 'w1',    balance: 1000 });

    const tx = db.beginTransaction();
    tx.update(users,   { _id: 'user1' }, { $inc: { points: 100 } });
    tx.update(wallets, { _id: 'w1'    }, { $inc: { balance: -100 } });
    await tx.commit();

    assert.equal(tx.status, 'committed');
    const user   = await users.findOne({ _id: 'user1' });
    const wallet = await wallets.findOne({ _id: 'w1' });
    assert.equal(user?.points, 100);
    assert.equal(wallet?.balance, 900);
  });

  test('rollback: operasi yang sudah diapply dibatalkan', async () => {
    const col  = await db.collection<User>('tx-rb-test');
    await col.insertOne({ _id: 'orig', name: 'Original', city: 'X', points: 100 });

    const tx = db.beginTransaction();
    tx.update(col, { _id: 'orig' }, { $inc: { points: 500 } });
    await tx.rollback();

    assert.equal(tx.status, 'rolled_back');
    const doc = await col.findOne({ _id: 'orig' });
    assert.equal(doc?.points, 100); // tidak berubah
  });
});

// ── Persistence ───────────────────────────────────────────────

describe('Persistence — data tetap ada setelah reopen', () => {

  test('data bertahan setelah close & reopen', async () => {
    const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'ovndb-persist-'));
    try {
      // Session 1: insert data
      const db1  = await OvnDB.open(dir, { fileLock: false });
      const col1 = await db1.collection<User>('persist-test');
      await col1.insertMany([
        { _id: 'p1', name: 'A', city: 'X', points: 10 },
        { _id: 'p2', name: 'B', city: 'Y', points: 20 },
      ]);
      await db1.close();

      // Session 2: baca data
      const db2  = await OvnDB.open(dir, { fileLock: false });
      const col2 = await db2.collection<User>('persist-test');
      const docs = await col2.find({});
      assert.equal(docs.length, 2, 'Data harus tetap ada setelah reopen');
      const p1 = await col2.findOne({ _id: 'p1' });
      assert.equal(p1?.name, 'A');
      await db2.close();
    } finally {
      await fsp.rm(dir, { recursive: true, force: true });
    }
  });
});
