import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import fsp from 'fs/promises';
import path from 'path';
import os from 'os';
import { OvnDB } from '../../src/index.js';
import type { OvnDocument } from '../../src/types/index.js';

function tmpDir(): string {
  return path.join(os.tmpdir(), 'ovndb-int-test-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8));
}
async function rmDir(dir: string) {
  await fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
}

interface TestDoc extends OvnDocument {
  _id: string;
  name?: string;
  age?: number;
  city?: string;
  active?: boolean;
  tags?: string[];
  points?: number;
  [key: string]: unknown;
}

// ── Database lifecycle ────────────────────────────────────────

describe('OvnDB lifecycle', () => {
  it('opens and closes without error', async () => {
    const dir = tmpDir();
    try {
      const db = await OvnDB.open(dir, { fileLock: false });
      assert.equal(db.isOpen, true);
      assert.equal(db.path, path.resolve(dir));
      await db.close();
      assert.equal(db.isOpen, false);
    } finally {
      await rmDir(dir);
    }
  });

  it('throws on operation after close', async () => {
    const dir = tmpDir();
    try {
      const db = await OvnDB.open(dir, { fileLock: false });
      await db.close();
      await assert.rejects(db.collection('test'), /ditutup/);
    } finally {
      await rmDir(dir);
    }
  });
});

// ── Collection CRUD ───────────────────────────────────────────

describe('Collection — insert', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;
  let col: Awaited<ReturnType<typeof db.collection<TestDoc>>>;

  before(async () => {
    dir = tmpDir();
    db = await OvnDB.open(dir, { fileLock: false });
    col = await db.collection<TestDoc>('test');
  });
  after(async () => { await db.close(); await rmDir(dir); });

  it('insertOne returns doc with _id', async () => {
    const doc = await col.insertOne({ name: 'Alice', age: 30 });
    assert.ok(doc._id);
    assert.equal(doc.name, 'Alice');
    assert.equal(doc.age, 30);
  });

  it('insertOne with custom _id', async () => {
    const doc = await col.insertOne({ _id: 'custom1', name: 'Bob' });
    assert.equal(doc._id, 'custom1');
  });

  it('insertOne rejects duplicate _id', async () => {
    await assert.rejects(
      col.insertOne({ _id: 'custom1', name: 'Dupe' }),
      /Duplicate/,
    );
  });

  it('insertMany returns all docs', async () => {
    const docs = await col.insertMany([
      { name: 'C', age: 20 },
      { name: 'D', age: 25 },
    ]);
    assert.equal(docs.length, 2);
    assert.ok(docs[0]!._id);
    assert.ok(docs[1]!._id);
  });
});

describe('Collection — find', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;
  let col: Awaited<ReturnType<typeof db.collection<TestDoc>>>;

  before(async () => {
    dir = tmpDir();
    db = await OvnDB.open(dir, { fileLock: false });
    col = await db.collection<TestDoc>('test');
    await col.insertMany([
      { _id: 'a1', name: 'Alice', age: 30, city: 'Jakarta', active: true },
      { _id: 'a2', name: 'Bob',   age: 25, city: 'Bandung', active: false },
      { _id: 'a3', name: 'Charlie', age: 35, city: 'Jakarta', active: true },
    ]);
  });
  after(async () => { await db.close(); await rmDir(dir); });

  it('findOne by _id', async () => {
    const doc = await col.findOne({ _id: 'a1' });
    assert.equal(doc?.name, 'Alice');
  });

  it('findOne returns null for missing', async () => {
    const doc = await col.findOne({ _id: 'missing' });
    assert.equal(doc, null);
  });

  it('findById', async () => {
    const doc = await col.findById('a2');
    assert.equal(doc?.name, 'Bob');
  });

  it('find with filter', async () => {
    const docs = await col.find({ city: 'Jakarta' });
    assert.equal(docs.length, 2);
  });

  it('find with $gte', async () => {
    const docs = await col.find({ age: { $gte: 30 } });
    assert.equal(docs.length, 2);
  });

  it('find with sort', async () => {
    const docs = await col.find({}, { sort: { age: 1 } });
    assert.equal(docs[0]!.name, 'Bob');
    assert.equal(docs[2]!.name, 'Charlie');
  });

  it('find with limit', async () => {
    const docs = await col.find({}, { limit: 2 });
    assert.equal(docs.length, 2);
  });

  it('find with skip', async () => {
    const docs = await col.find({}, { sort: { age: 1 }, skip: 1 });
    assert.equal(docs.length, 2);
  });

  it('find with projection', async () => {
    const docs = await col.find({ _id: 'a1' }, { projection: { name: 1 } });
    assert.equal(docs[0]!['name'], 'Alice');
    assert.equal(docs[0]!['age'], undefined);
    assert.ok(docs[0]!._id);
  });

  it('countDocuments', async () => {
    const count = await col.countDocuments({ active: true });
    assert.equal(count, 2);
  });

  it('distinct', async () => {
    const cities = await col.distinct('city');
    assert.ok(cities.includes('Jakarta'));
    assert.ok(cities.includes('Bandung'));
    assert.equal(cities.length, 2);
  });

  it('exists', async () => {
    assert.equal(await col.exists({ _id: 'a1' }), true);
    assert.equal(await col.exists({ _id: 'nonexistent' }), false);
  });

  it('findManyById', async () => {
    const docs = await col.findManyById(['a1', 'a3']);
    assert.equal(docs.length, 2);
  });
});

describe('Collection — update', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;
  let col: Awaited<ReturnType<typeof db.collection<TestDoc>>>;

  before(async () => {
    dir = tmpDir();
    db = await OvnDB.open(dir, { fileLock: false });
    col = await db.collection<TestDoc>('test');
    await col.insertMany([
      { _id: 'u1', name: 'Alice', points: 10, active: true },
      { _id: 'u2', name: 'Bob',   points: 20, active: true },
      { _id: 'u3', name: 'Carol', points: 30, active: false },
    ]);
  });
  after(async () => { await db.close(); await rmDir(dir); });

  it('updateOne with $set', async () => {
    const ok = await col.updateOne({ _id: 'u1' }, { $set: { name: 'Alice Updated' } });
    assert.equal(ok, true);
    const doc = await col.findOne({ _id: 'u1' });
    assert.equal(doc?.name, 'Alice Updated');
  });

  it('updateOne with $inc', async () => {
    await col.updateOne({ _id: 'u1' }, { $inc: { points: 5 } });
    const doc = await col.findOne({ _id: 'u1' });
    assert.equal(doc?.points, 15);
  });

  it('updateOne returns false for no match', async () => {
    const ok = await col.updateOne({ _id: 'missing' }, { $set: { name: 'X' } });
    assert.equal(ok, false);
  });

  it('updateMany', async () => {
    const count = await col.updateMany({ active: true }, { $set: { active: false } });
    assert.equal(count, 2);
    const docs = await col.find({ active: true });
    assert.equal(docs.length, 0);
  });

  it('findOneAndUpdate returns updated doc', async () => {
    const doc = await col.findOneAndUpdate({ _id: 'u1' }, { $set: { name: 'Final' } });
    assert.ok(doc);
  });
});

describe('Collection — upsert', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;
  let col: Awaited<ReturnType<typeof db.collection<TestDoc>>>;

  before(async () => {
    dir = tmpDir();
    db = await OvnDB.open(dir, { fileLock: false });
    col = await db.collection<TestDoc>('test');
  });
  after(async () => { await db.close(); await rmDir(dir); });

  it('upsertOne inserts new doc when no match', async () => {
    const doc = await col.upsertOne({ _id: 'new1' }, { $set: { name: 'NewDoc' } });
    assert.equal(doc.name, 'NewDoc');
    const found = await col.findOne({ _id: 'new1' });
    assert.ok(found);
  });

  it('upsertOne updates existing when match found', async () => {
    await col.insertOne({ _id: 'exist1', name: 'Old' });
    const doc = await col.upsertOne({ _id: 'exist1' }, { $set: { name: 'Updated' } });
    assert.equal(doc.name, 'Updated');
  });
});

describe('Collection — delete', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;
  let col: Awaited<ReturnType<typeof db.collection<TestDoc>>>;

  before(async () => {
    dir = tmpDir();
    db = await OvnDB.open(dir, { fileLock: false });
    col = await db.collection<TestDoc>('test');
    await col.insertMany([
      { _id: 'd1', name: 'X', active: true },
      { _id: 'd2', name: 'Y', active: true },
      { _id: 'd3', name: 'Z', active: false },
    ]);
  });
  after(async () => { await db.close(); await rmDir(dir); });

  it('deleteOne removes matching doc', async () => {
    const ok = await col.deleteOne({ _id: 'd1' });
    assert.equal(ok, true);
    assert.equal(await col.findOne({ _id: 'd1' }), null);
  });

  it('deleteOne returns false for no match', async () => {
    const ok = await col.deleteOne({ _id: 'missing' });
    assert.equal(ok, false);
  });

  it('deleteMany removes matching docs', async () => {
    const count = await col.deleteMany({ active: true });
    assert.ok(count >= 1);
  });

  it('findOneAndDelete returns and removes doc', async () => {
    await col.insertOne({ _id: 'del4', name: 'Delete me' });
    const doc = await col.findOneAndDelete({ _id: 'del4' });
    assert.equal(doc?.name, 'Delete me');
    assert.equal(await col.findOne({ _id: 'del4' }), null);
  });
});

// ── BulkWrite ─────────────────────────────────────────────────

describe('Collection — bulkWrite', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;
  let col: Awaited<ReturnType<typeof db.collection<TestDoc>>>;

  before(async () => {
    dir = tmpDir();
    db = await OvnDB.open(dir, { fileLock: false });
    col = await db.collection<TestDoc>('test');
  });
  after(async () => { await db.close(); await rmDir(dir); });

  it('executes mixed operations', async () => {
    const result = await col.bulkWrite([
      { op: 'insertOne', doc: { _id: 'bw1', name: 'A' } },
      { op: 'insertOne', doc: { _id: 'bw2', name: 'B' } },
      { op: 'updateOne', filter: { _id: 'bw1' }, spec: { $set: { name: 'A Updated' } } },
      { op: 'deleteOne', filter: { _id: 'bw2' } },
    ]);
    assert.equal(result.ops, 4);
    assert.equal(result.insertedCount, 2);
    assert.equal(result.updatedCount, 1);
    assert.equal(result.deletedCount, 1);
    assert.equal(result.errors.length, 0);

    const a = await col.findOne({ _id: 'bw1' });
    assert.equal(a?.name, 'A Updated');
    assert.equal(await col.findOne({ _id: 'bw2' }), null);
  });
});

// ── Aggregation ───────────────────────────────────────────────

describe('Collection — aggregate', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;
  let col: Awaited<ReturnType<typeof db.collection<TestDoc>>>;

  before(async () => {
    dir = tmpDir();
    db = await OvnDB.open(dir, { fileLock: false });
    col = await db.collection<TestDoc>('test');
    await col.insertMany([
      { name: 'Alice', age: 30, city: 'Jakarta' },
      { name: 'Bob',   age: 25, city: 'Bandung' },
      { name: 'Carol', age: 35, city: 'Jakarta' },
    ]);
  });
  after(async () => { await db.close(); await rmDir(dir); });

  it('match → group → sort', async () => {
    const result = await col.aggregate([
      { $match: { age: { $gte: 25 } } },
      { $group: { _id: '$city', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
    ]);
    assert.equal(result.length, 2);
    assert.equal(result[0]!['count'], 2); // Jakarta
    assert.equal(result[1]!['count'], 1); // Bandung
  });

  it('$count', async () => {
    const result = await col.aggregate([
      { $count: 'total' as never },
    ]);
    assert.equal((result[0] as any)['total'], 3);
  });
});

// ── Stats ─────────────────────────────────────────────────────

describe('Collection — stats', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;
  let col: Awaited<ReturnType<typeof db.collection<TestDoc>>>;

  before(async () => {
    dir = tmpDir();
    db = await OvnDB.open(dir, { fileLock: false });
    col = await db.collection<TestDoc>('test');
    await col.insertMany([
      { name: 'A' },
      { name: 'B' },
      { name: 'C' },
    ]);
    await col.flush();
  });
  after(async () => { await db.close(); await rmDir(dir); });

  it('returns stats with correct fields', async () => {
    const s = await col.stats();
    assert.equal(s.collection, 'test');
    assert.ok(s.totalLive >= 3n);
    assert.equal(typeof s.segmentCount, 'number');
    assert.equal(typeof s.totalFileSize, 'number');
    assert.equal(typeof s.fragmentRatio, 'number');
    assert.equal(typeof s.cacheHitRate, 'number');
  });
});

// ── DB-level operations ──────────────────────────────────────

describe('OvnDB — db-level', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;

  before(async () => {
    dir = tmpDir();
    db = await OvnDB.open(dir, { fileLock: false });
  });
  after(async () => { await db.close(); await rmDir(dir); });

  it('listCollections', async () => {
    await db.collection('col_a');
    await db.collection('col_b');
    const list = await db.listCollections();
    assert.ok(list.includes('col_a'));
    assert.ok(list.includes('col_b'));
  });

  it('collectionExists', async () => {
    await db.collection('existing_col');
    assert.equal(await db.collectionExists('existing_col'), true);
    assert.equal(await db.collectionExists('nonexistent'), false);
  });

  it('dropCollection', async () => {
    await db.collection('to_drop');
    await db.dropCollection('to_drop');
    assert.equal(await db.collectionExists('to_drop'), false);
  });

  it('status returns DB status', async () => {
    const s = await db.status();
    assert.equal(s.path, path.resolve(dir));
    assert.equal(typeof s.openedAt, 'number');
    assert.ok(Array.isArray(s.collections));
    assert.equal(typeof s.isHealthy, 'boolean');
  });
});

// ── v4.0: Compound Index ──────────────────────────────────────

describe('Collection — compound index (v4.0)', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;
  let col: Awaited<ReturnType<typeof db.collection<TestDoc>>>;

  before(async () => {
    dir = tmpDir();
    db  = await OvnDB.open(dir, { fileLock: false });
    col = await db.collection<TestDoc>('ci_test');
    // Create compound index on [city, active]
    await col.createIndex({ field: ['city', 'active'], unique: false });
    await col.insertMany([
      { _id: 'ci1', name: 'Alice', city: 'Jakarta', active: true,  points: 100 },
      { _id: 'ci2', name: 'Bob',   city: 'Jakarta', active: false, points: 50  },
      { _id: 'ci3', name: 'Carol', city: 'Bandung', active: true,  points: 80  },
      { _id: 'ci4', name: 'Dave',  city: 'Jakarta', active: true,  points: 200 },
    ]);
  });
  after(async () => { await db.close(); await rmDir(dir); });

  it('find with compound-indexed fields returns correct docs', async () => {
    const docs = await col.find({ city: 'Jakarta', active: true });
    assert.ok(docs.length >= 2, `Expected ≥2 docs, got ${docs.length}`);
    for (const d of docs) {
      assert.equal(d.city, 'Jakarta');
      assert.equal(d.active, true);
    }
  });

  it('compound index excludes non-matching docs', async () => {
    const docs = await col.find({ city: 'Bandung', active: true });
    assert.equal(docs.length, 1);
    assert.equal(docs[0]?.name, 'Carol');
  });

  it('createIndex with string array does not throw', async () => {
    await assert.doesNotReject(
      col.createIndex({ field: ['name', 'city'], unique: false }),
    );
  });
});

// ── v4.0: Full-text Search ────────────────────────────────────

describe('Collection — createTextIndex & $text (v4.0)', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;
  let col: Awaited<ReturnType<typeof db.collection<TestDoc>>>;

  before(async () => {
    dir = tmpDir();
    db  = await OvnDB.open(dir, { fileLock: false });
    col = await db.collection<TestDoc>('fts_test');
    await col.insertMany([
      { _id: 'f1', name: 'Budi Santoso dari Jakarta',  city: 'Jakarta' },
      { _id: 'f2', name: 'Siti Rahayu dari Bandung',   city: 'Bandung' },
      { _id: 'f3', name: 'Budi Rahayu tinggal Bogor',  city: 'Bogor'   },
      { _id: 'f4', name: 'Anton Wijaya Jakarta Selatan', city: 'Jakarta' },
    ]);
    await col.createTextIndex('name');
  });
  after(async () => { await db.close(); await rmDir(dir); });

  it('$text single word returns matching docs', async () => {
    const docs = await col.find({ $text: 'budi' });
    assert.ok(docs.length >= 2, `Expected ≥2 docs for "budi", got ${docs.length}`);
    const names = docs.map(d => d.name!);
    assert.ok(names.some(n => n.includes('Budi')));
  });

  it('$text multi-word returns only docs with ALL words', async () => {
    const docs = await col.find({ $text: 'budi jakarta' });
    assert.equal(docs.length, 1);
    assert.equal(docs[0]?._id, 'f1');
  });

  it('$text with no match returns empty array', async () => {
    const docs = await col.find({ $text: 'surabaya' });
    assert.equal(docs.length, 0);
  });

  it('findOne with $text works', async () => {
    const doc = await col.findOne({ $text: 'siti' });
    assert.ok(doc);
    assert.equal(doc?._id, 'f2');
  });

  it('FTS index updates on insert', async () => {
    await col.insertOne({ _id: 'f5', name: 'Dewi Surabaya', city: 'Surabaya' });
    const docs = await col.find({ $text: 'dewi' });
    assert.ok(docs.length >= 1);
    assert.equal(docs[0]?._id, 'f5');
  });

  it('FTS index updates on delete', async () => {
    await col.deleteOne({ _id: 'f5' });
    const docs = await col.find({ $text: 'dewi' });
    assert.equal(docs.length, 0);
  });
});

// ── v4.0: Import / Export ─────────────────────────────────────

describe('Collection — importFrom / exportTo (v4.0)', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;
  let col: Awaited<ReturnType<typeof db.collection<TestDoc>>>;
  let exportPath: string;

  before(async () => {
    dir        = tmpDir();
    exportPath = path.join(dir, 'export.ndjson');
    db         = await OvnDB.open(dir, { fileLock: false });
    col        = await db.collection<TestDoc>('ie_test');
    await col.insertMany([
      { _id: 'ie1', name: 'Alice', city: 'Jakarta' },
      { _id: 'ie2', name: 'Bob',   city: 'Bandung' },
      { _id: 'ie3', name: 'Carol', city: 'Bogor'   },
    ]);
  });
  after(async () => { await db.close(); await rmDir(dir); });

  it('exportTo NDJSON creates file with correct line count', async () => {
    const count = await col.exportTo(exportPath);
    assert.equal(count, 3);
    const content = await fsp.readFile(exportPath, 'utf8');
    const lines   = content.trim().split('\n').filter(l => l.trim());
    assert.equal(lines.length, 3);
  });

  it('importFrom NDJSON re-imports into new collection', async () => {
    const col2   = await db.collection<TestDoc>('ie_import');
    const result = await col2.importFrom(exportPath);
    assert.equal(result.total, 3);
    assert.equal(result.inserted, 3);
    assert.equal(result.errors.length, 0);
    const docs = await col2.find({});
    assert.equal(docs.length, 3);
  });

  it('exportTo JSON format works', async () => {
    const jsonPath = path.join(dir, 'export.json');
    const count    = await col.exportTo(jsonPath, { format: 'json' });
    assert.equal(count, 3);
    const raw  = await fsp.readFile(jsonPath, 'utf8');
    const docs = JSON.parse(raw);
    assert.ok(Array.isArray(docs));
    assert.equal(docs.length, 3);
  });

  it('importFrom JSON format works', async () => {
    const jsonPath = path.join(dir, 'export.json');
    const col3     = await db.collection<TestDoc>('ie_import_json');
    const result   = await col3.importFrom(jsonPath, { format: 'json' });
    assert.equal(result.inserted, 3);
    const docs = await col3.find({});
    assert.equal(docs.length, 3);
  });

  it('importFrom with continueOnError skips bad docs', async () => {
    // Write NDJSON with one bad line
    const badPath = path.join(dir, 'bad.ndjson');
    await fsp.writeFile(badPath, [
      JSON.stringify({ _id: 'ok1', name: 'Good' }),
      'NOT JSON {{{{',
      JSON.stringify({ _id: 'ok2', name: 'Also Good' }),
    ].join('\n'));
    const col4   = await db.collection<TestDoc>('ie_bad');
    const result = await col4.importFrom(badPath, { continueOnError: true });
    assert.equal(result.errors.length, 1);
    assert.ok(result.inserted >= 2);
  });
});
