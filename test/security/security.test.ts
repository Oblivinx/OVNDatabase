// ============================================================
//  OvnDB — Security Test Suite
//
//  Mencakup semua attack vector yang diidentifikasi dalam audit:
//   1.  Path traversal via nama collection
//   2.  Path traversal via backup destination
//   3.  Prototype pollution via filter field path
//   4.  Prototype pollution via update spec
//   5.  Prototype pollution via insertOne
//   6.  $where operator blocked
//   7.  $regex ReDoS protection
//   8.  $in / $nin array size limits
//   9.  Query filter depth limit
//   10. Document size limit
//   11. Document _id validation
//   12. Aggregation pipeline stage limit
//   13. $lookup collection name validation
//   14. $limit / $skip sanity (negative, NaN)
//   15. WAL key / data length cap
//   16. Crypto version-byte heuristic fix
//   17. CSPRNG ID generation
//   18. Invalid field paths in distinct / createIndex
//   19. Update spec unknown operator rejection
//   20. Safe error messages (no raw _id in output)
// ============================================================

import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import os from 'node:os';
import path from 'node:path';
import fs from 'node:fs';
import fsp from 'node:fs/promises';

// ── Helpers ───────────────────────────────────────────────────

function tmpDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), 'ovndb-sec-'));
}

async function rmDir(p: string): Promise<void> {
  await fsp.rm(p, { recursive: true, force: true });
}

// ── Unit: security.ts ─────────────────────────────────────────

describe('security utilities', () => {

  // ── 1. validateCollectionName ─────────────────────────────

  describe('validateCollectionName', async () => {
    const { validateCollectionName } = await import('../../src/utils/security.js');

    it('accepts valid names', () => {
      assert.doesNotThrow(() => validateCollectionName('users'));
      assert.doesNotThrow(() => validateCollectionName('user_sessions'));
      assert.doesNotThrow(() => validateCollectionName('my-collection'));
      assert.doesNotThrow(() => validateCollectionName('Col123'));
    });

    it('rejects path traversal sequences', () => {
      assert.throws(() => validateCollectionName('../etc'), /tidak valid/);
      assert.throws(() => validateCollectionName('../../root'), /tidak valid/);
      assert.throws(() => validateCollectionName('a/b'), /tidak valid/);
    });

    it('rejects null bytes', () => {
      assert.throws(() => validateCollectionName('col\0name'), /tidak valid/);
    });

    it('rejects colon (Windows device names)', () => {
      assert.throws(() => validateCollectionName('col:name'), /tidak valid/);
    });

    it('rejects empty string', () => {
      assert.throws(() => validateCollectionName(''), /tidak valid/);
    });

    it('rejects name longer than 64 chars', () => {
      assert.throws(() => validateCollectionName('a'.repeat(65)), /tidak valid/);
    });

    it('rejects dot-only names', () => {
      assert.throws(() => validateCollectionName('.'), /tidak valid/);
      assert.throws(() => validateCollectionName('..'), /tidak valid/);
    });

    it('rejects spaces and special chars', () => {
      assert.throws(() => validateCollectionName('my collection'), /tidak valid/);
      assert.throws(() => validateCollectionName('col*'), /tidak valid/);
    });
  });

  // ── 2. isDangerousKey ─────────────────────────────────────

  describe('isDangerousKey', async () => {
    const { isDangerousKey } = await import('../../src/utils/security.js');

    it('flags prototype pollution keys', () => {
      assert.equal(isDangerousKey('__proto__'), true);
      assert.equal(isDangerousKey('constructor'), true);
      assert.equal(isDangerousKey('prototype'), true);
      assert.equal(isDangerousKey('__defineGetter__'), true);
      assert.equal(isDangerousKey('__defineSetter__'), true);
    });

    it('clears normal keys', () => {
      assert.equal(isDangerousKey('name'), false);
      assert.equal(isDangerousKey('_id'), false);
      assert.equal(isDangerousKey('proto'), false); // not __proto__
      assert.equal(isDangerousKey('Constructor'), false); // case-sensitive
    });
  });

  // ── 3. validateFieldPath ──────────────────────────────────

  describe('validateFieldPath', async () => {
    const { validateFieldPath } = await import('../../src/utils/security.js');

    it('accepts valid dot-notation paths', () => {
      assert.doesNotThrow(() => validateFieldPath('name'));
      assert.doesNotThrow(() => validateFieldPath('user.address.city'));
      assert.doesNotThrow(() => validateFieldPath('a.b.c.d'));
    });

    it('rejects __proto__ anywhere in path', () => {
      assert.throws(() => validateFieldPath('__proto__'), /terlarang/);
      assert.throws(() => validateFieldPath('user.__proto__.polluted'), /terlarang/);
      assert.throws(() => validateFieldPath('__proto__.admin'), /terlarang/);
    });

    it('rejects constructor in path', () => {
      assert.throws(() => validateFieldPath('constructor'), /terlarang/);
      assert.throws(() => validateFieldPath('obj.constructor.prototype'), /terlarang/);
    });

    it('rejects empty path', () => {
      assert.throws(() => validateFieldPath(''), /kosong/);
    });

    it('rejects path longer than 256 chars', () => {
      assert.throws(() => validateFieldPath('a'.repeat(257)), /panjang/);
    });

    it('rejects empty segment (double dot)', () => {
      assert.throws(() => validateFieldPath('a..b'), /segment kosong/);
    });
  });

  // ── 4. validateQueryFilter ────────────────────────────────

  describe('validateQueryFilter', async () => {
    const { validateQueryFilter } = await import('../../src/utils/security.js');

    it('accepts valid simple filter', () => {
      assert.doesNotThrow(() => validateQueryFilter({ name: 'Alice', age: 30 }));
      assert.doesNotThrow(() => validateQueryFilter({ role: { $in: ['admin', 'user'] } }));
      assert.doesNotThrow(() => validateQueryFilter({ $and: [{ a: 1 }, { b: 2 }] }));
    });

    it('blocks $where operator', () => {
      assert.throws(() => validateQueryFilter({ $where: () => true }), /\$where/);
      assert.throws(() => validateQueryFilter({ age: { $where: 'x' } }), /\$where/);
    });

    it('blocks __proto__ as filter key', () => {
      assert.throws(() => validateQueryFilter({ '__proto__': { polluted: true } }), /terlarang/);
    });

    it('blocks __proto__ in nested field path', () => {
      assert.throws(() => validateQueryFilter({ 'user.__proto__': 1 }), /terlarang/);
    });

    it('enforces filter depth limit', () => {
      // Build a filter 15 levels deep (limit is 12)
      let deep: Record<string, unknown> = { a: 1 };
      for (let i = 0; i < 15; i++) deep = { $and: [deep] };
      assert.throws(() => validateQueryFilter(deep), /terlalu dalam/);
    });

    it('enforces $in array size limit', () => {
      assert.throws(
        () => validateQueryFilter({ id: { $in: new Array(1001).fill('x') } }),
        /terlalu banyak/
      );
    });

    it('enforces $nin array size limit', () => {
      assert.throws(
        () => validateQueryFilter({ id: { $nin: new Array(1001).fill('x') } }),
        /terlalu banyak/
      );
    });

    it('enforces $regex length limit', () => {
      assert.throws(
        () => validateQueryFilter({ name: { $regex: 'a'.repeat(600) } }),
        /panjang|ReDoS/
      );
    });

    it('enforces key count limit per level', () => {
      const bigFilter: Record<string, number> = {};
      for (let i = 0; i < 70; i++) bigFilter[`field${i}`] = i;
      assert.throws(() => validateQueryFilter(bigFilter), /terlalu banyak key/);
    });
  });

  // ── 5. validateRegex ─────────────────────────────────────

  describe('validateRegex', async () => {
    const { validateRegex } = await import('../../src/utils/security.js');

    it('accepts safe patterns', () => {
      assert.doesNotThrow(() => validateRegex('^hello'));
      assert.doesNotThrow(() => validateRegex('[a-z]+'));
      assert.doesNotThrow(() => validateRegex('\\d{3}-\\d{4}'));
    });

    it('rejects non-string non-RegExp', () => {
      assert.throws(() => validateRegex(42), /harus berupa string/);
      assert.throws(() => validateRegex(null), /harus berupa string/);
    });

    it('rejects pattern too long', () => {
      assert.throws(() => validateRegex('a'.repeat(600)), /panjang/);
    });

    it('blocks ReDoS pattern (.*)*', () => {
      assert.throws(() => validateRegex('(.*)*'), /ReDoS/);
    });

    it('blocks ReDoS pattern (.+)+', () => {
      assert.throws(() => validateRegex('(.+)+'), /ReDoS/);
    });

    it('blocks group-with-quantifier followed by quantifier', () => {
      assert.throws(() => validateRegex('(a+)+'), /ReDoS/);
    });
  });

  // ── 6. validateUpdateSpec ─────────────────────────────────

  describe('validateUpdateSpec', async () => {
    const { validateUpdateSpec } = await import('../../src/utils/security.js');

    it('accepts valid operators', () => {
      assert.doesNotThrow(() => validateUpdateSpec({ $set: { name: 'Bob' } }));
      assert.doesNotThrow(() => validateUpdateSpec({ $inc: { counter: 1 } }));
      assert.doesNotThrow(() => validateUpdateSpec({ $push: { tags: 'new' } }));
    });

    it('rejects unknown operators', () => {
      assert.throws(() => validateUpdateSpec({ $evil: { x: 1 } }), /tidak dikenal/);
    });

    it('blocks __proto__ as field key in $set', () => {
      assert.throws(() => validateUpdateSpec({ $set: { '__proto__': { admin: true } } }), /terlarang/);
    });

    it('blocks prototype in nested field path', () => {
      assert.throws(
        () => validateUpdateSpec({ $set: { 'obj.prototype.x': 1 } }),
        /terlarang/
      );
    });

    it('enforces $push.$each size limit', () => {
      assert.throws(
        () => validateUpdateSpec({ $push: { arr: { $each: new Array(1001).fill('x') } } }),
        /terlalu banyak/
      );
    });
  });

  // ── 7. validateDocumentSize ───────────────────────────────

  describe('validateDocumentSize', async () => {
    const { validateDocumentSize, MAX_DOCUMENT_BYTES } = await import('../../src/utils/security.js');

    it('accepts documents under the limit', () => {
      assert.doesNotThrow(() => validateDocumentSize(Buffer.alloc(1024)));
      assert.doesNotThrow(() => validateDocumentSize(Buffer.alloc(MAX_DOCUMENT_BYTES)));
    });

    it('rejects documents over the limit', () => {
      assert.throws(
        () => validateDocumentSize(Buffer.alloc(MAX_DOCUMENT_BYTES + 1)),
        /melebihi ukuran maksimum/
      );
    });
  });

  // ── 8. validateDocumentId ─────────────────────────────────

  describe('validateDocumentId', async () => {
    const { validateDocumentId } = await import('../../src/utils/security.js');

    it('accepts valid string IDs', () => {
      assert.doesNotThrow(() => validateDocumentId('abc123'));
      assert.doesNotThrow(() => validateDocumentId('user-42'));
    });

    it('rejects non-string', () => {
      assert.throws(() => validateDocumentId(123), /harus berupa string/);
    });

    it('rejects empty string', () => {
      assert.throws(() => validateDocumentId(''), /tidak boleh kosong/);
    });

    it('rejects ID over 128 chars', () => {
      assert.throws(() => validateDocumentId('a'.repeat(129)), /terlalu panjang/);
    });

    it('rejects null bytes', () => {
      assert.throws(() => validateDocumentId('abc\0def'), /null byte/);
    });
  });

  // ── 9. assertPathInside ───────────────────────────────────

  describe('assertPathInside', async () => {
    const { assertPathInside } = await import('../../src/utils/security.js');

    it('accepts path inside base', () => {
      assert.doesNotThrow(() =>
        assertPathInside('/data/db', '/data/db/backups/2025')
      );
    });

    it('rejects path escaping base via ..', () => {
      assert.throws(() =>
        assertPathInside('/data/db', '/data/db/../../etc'),
        /di luar direktori/
      );
    });

    it('rejects sibling directory', () => {
      assert.throws(() =>
        assertPathInside('/data/db', '/data/other'),
        /di luar direktori/
      );
    });

    it('accepts base itself', () => {
      assert.doesNotThrow(() =>
        assertPathInside('/data/db', '/data/db')
      );
    });
  });
});

// ── Unit: filter.ts ───────────────────────────────────────────

describe('filter.ts hardening', () => {

  // ── 10. getFieldValue prototype pollution ─────────────────

  describe('getFieldValue prototype pollution', async () => {
    const { getFieldValue } = await import('../../src/core/query/filter.js');

    it('returns undefined for __proto__ path', () => {
      const doc = { a: 1 };
      assert.equal(getFieldValue(doc as Record<string, unknown>, '__proto__'), undefined);
    });

    it('returns undefined for nested __proto__', () => {
      const doc = { user: { name: 'Alice' } };
      assert.equal(
        getFieldValue(doc as Record<string, unknown>, 'user.__proto__.polluted'),
        undefined
      );
    });

    it('returns undefined for constructor path', () => {
      const doc = { obj: {} };
      assert.equal(
        getFieldValue(doc as Record<string, unknown>, 'obj.constructor'),
        undefined
      );
    });
  });

  // ── 11. setNestedField prototype pollution ────────────────

  describe('setNestedField prototype pollution', async () => {
    const { setNestedField } = await import('../../src/core/query/filter.js');

    it('throws on __proto__ path', () => {
      const obj: Record<string, unknown> = {};
      assert.throws(() => setNestedField(obj, '__proto__.polluted', true), /terlarang/);
    });

    it('throws on prototype path', () => {
      const obj: Record<string, unknown> = {};
      assert.throws(() => setNestedField(obj, 'foo.prototype.x', 1), /terlarang/);
    });

    it('does NOT pollute Object.prototype', () => {
      const obj: Record<string, unknown> = {};
      try {
        setNestedField(obj, '__proto__.polluted', true);
      } catch { /* expected */ }
      // Ensure Object.prototype was not modified
      assert.equal((Object.prototype as Record<string, unknown>)['polluted'], undefined);
    });
  });

  // ── 12. matchFilter $where blocked ────────────────────────

  describe('matchFilter $where', async () => {
    const { matchFilter } = await import('../../src/core/query/filter.js');

    it('throws when $where is used', () => {
      assert.throws(
        () => matchFilter({ age: 30 }, { $where: (() => true) as unknown as never }),
        /\$where/
      );
    });
  });

  // ── 13. $in / $nin size limits in matchFilter ─────────────

  describe('matchFilter $in/$nin size', async () => {
    const { matchFilter } = await import('../../src/core/query/filter.js');

    it('throws when $in array is too large', () => {
      assert.throws(
        () => matchFilter({ id: 'x' }, { id: { $in: new Array(1001).fill('x') } }),
        /terlalu banyak/
      );
    });

    it('throws when $nin array is too large', () => {
      assert.throws(
        () => matchFilter({ id: 'x' }, { id: { $nin: new Array(1001).fill('y') } }),
        /terlalu banyak/
      );
    });
  });

  // ── 14. $regex validation ─────────────────────────────────

  describe('matchFilter $regex validation', async () => {
    const { matchFilter } = await import('../../src/core/query/filter.js');

    it('throws on ReDoS pattern (.*)*', () => {
      assert.throws(
        () => matchFilter({ name: 'test' }, { name: { $regex: '(.*)*' } }),
        /ReDoS/
      );
    });

    it('throws on regex pattern too long', () => {
      assert.throws(
        () => matchFilter({ name: 'test' }, { name: { $regex: 'a'.repeat(600) } }),
        /panjang|ReDoS/
      );
    });

    it('matches normally on safe regex', () => {
      assert.equal(
        matchFilter({ name: 'Alice' }, { name: { $regex: '^Al' } }),
        true
      );
      assert.equal(
        matchFilter({ name: 'Bob' }, { name: { $regex: '^Al' } }),
        false
      );
    });
  });
});

// ── Unit: aggregation.ts ─────────────────────────────────────

describe('aggregation.ts hardening', async () => {
  const { compilePipeline } = await import('../../src/core/query/aggregation.js');

  const noResolver = async (_: string) => [] as Record<string, unknown>[];
  const sampleDocs: Record<string, unknown>[] = [
    { _id: '1', name: 'Alice', age: 30 },
    { _id: '2', name: 'Bob', age: 25 },
    { _id: '3', name: 'Charlie', age: 35 },
  ];

  // ── 15. $lookup collection name ───────────────────────────

  describe('$lookup collection name validation', () => {
    it('rejects path traversal in $lookup.from', async () => {
      const pipeline = [{ $lookup: { from: '../secret', localField: '_id', foreignField: 'userId', as: 'data' } }];
      await assert.rejects(
        compilePipeline(pipeline, noResolver)(sampleDocs),
        /tidak valid/
      );
    });

    it('accepts valid collection name in $lookup.from', async () => {
      let called = '';
      const resolver = async (c: string) => { called = c; return []; };
      const pipeline = [{ $lookup: { from: 'orders', localField: '_id', foreignField: 'userId', as: 'orders' } }];
      await compilePipeline(pipeline, resolver)(sampleDocs);
      assert.equal(called, 'orders');
    });

    it('rejects __proto__ in $lookup.localField', async () => {
      const pipeline = [{ $lookup: { from: 'orders', localField: '__proto__', foreignField: 'id', as: 'r' } }];
      await assert.rejects(
        compilePipeline(pipeline, noResolver)(sampleDocs),
        /terlarang/
      );
    });

    it('rejects __proto__ in $lookup.as', async () => {
      const pipeline = [{ $lookup: { from: 'orders', localField: 'id', foreignField: 'id', as: '__proto__' } }];
      await assert.rejects(
        compilePipeline(pipeline, noResolver)(sampleDocs),
        /terlarang/
      );
    });
  });

  // ── 16. $limit / $skip sanity ─────────────────────────────

  describe('$limit and $skip sanity', () => {
    it('throws on negative $limit', async () => {
      await assert.rejects(
        compilePipeline([{ $limit: -1 }], noResolver)(sampleDocs),
        /positif/
      );
    });

    it('throws on NaN $limit', async () => {
      await assert.rejects(
        compilePipeline([{ $limit: NaN }], noResolver)(sampleDocs),
        /positif/
      );
    });

    it('throws on negative $skip', async () => {
      await assert.rejects(
        compilePipeline([{ $skip: -5 }], noResolver)(sampleDocs),
        /positif/
      );
    });

    it('accepts zero $skip', async () => {
      const result = await compilePipeline([{ $skip: 0 }], noResolver)(sampleDocs);
      assert.equal(result.length, 3);
    });

    it('accepts valid $limit', async () => {
      const result = await compilePipeline([{ $limit: 2 }], noResolver)(sampleDocs);
      assert.equal(result.length, 2);
    });
  });

  // ── 17. $count field name ─────────────────────────────────

  describe('$count field name', () => {
    it('throws on __proto__ as $count field', async () => {
      await assert.rejects(
        compilePipeline([{ $count: '__proto__' as never }], noResolver)(sampleDocs),
        /terlarang/
      );
    });

    it('counts normally with valid field name', async () => {
      const result = await compilePipeline([{ $count: 'total' as never }], noResolver)(sampleDocs);
      assert.deepEqual(result, [{ total: 3 }]);
    });
  });

  // ── 18. $sort key validation ──────────────────────────────

  describe('$sort key validation', () => {
    it('throws on __proto__ as sort key', async () => {
      await assert.rejects(
        compilePipeline([{ $sort: { '__proto__': 1 as (1 | -1) } }], noResolver)(sampleDocs),
        /terlarang/
      );
    });

    it('sorts normally with valid field', async () => {
      const result = await compilePipeline([{ $sort: { age: 1 } }], noResolver)(sampleDocs);
      assert.equal((result[0] as Record<string, unknown>)['name'], 'Bob');
    });
  });
});

// ── Unit: id-generator.ts ─────────────────────────────────────

describe('id-generator CSPRNG', async () => {
  const { generateId, isValidId } = await import('../../src/utils/id-generator.js');

  it('generates 24-hex-char IDs', () => {
    const id = generateId();
    assert.equal(id.length, 24);
    assert.match(id, /^[0-9a-f]{24}$/);
  });

  it('generates unique IDs', () => {
    const ids = new Set<string>();
    for (let i = 0; i < 10_000; i++) ids.add(generateId());
    assert.equal(ids.size, 10_000);
  });

  it('generates monotonically non-decreasing IDs', () => {
    const a = generateId();
    const b = generateId();
    assert.ok(b >= a, `Expected ${b} >= ${a}`);
  });

  it('isValidId accepts generated IDs', () => {
    assert.equal(isValidId(generateId()), true);
  });

  it('isValidId rejects short IDs', () => {
    assert.equal(isValidId('abc'), false);
  });

  it('isValidId rejects uppercase IDs', () => {
    assert.equal(isValidId('A'.repeat(24)), false);
  });

  it('does not use Math.random (source-code check)', async () => {
    const src = await fsp.readFile(
      new URL('../../src/utils/id-generator.ts', import.meta.url),
      'utf8'
    );
    assert.ok(!src.includes('Math.random'), 'id-generator must not use Math.random');
    assert.ok(src.includes('crypto.randomBytes'), 'id-generator must use crypto.randomBytes');
  });
});

// ── Unit: crypto-layer.ts ────────────────────────────────────

describe('CryptoLayer version-byte heuristic fix', async () => {
  const { CryptoLayer } = await import('../../src/crypto/crypto-layer.js');
  const crypto = await import('node:crypto');

  const key = crypto.randomBytes(32);

  it('v3 encrypt → v3 decrypt round-trips correctly', () => {
    const layer = CryptoLayer.fromKey(key, 1);
    const plain = Buffer.from('{"_id":"abc","name":"Alice"}');
    const ct = layer.encrypt(plain);
    const pt = layer.decrypt(ct);
    assert.deepEqual(pt, plain);
  });

  it('v2 ciphertext (28-byte overhead) decrypts correctly via fallback', () => {
    // Simulate v2 ciphertext: [12 IV][16 Tag][N CT]
    const layer = CryptoLayer.fromKey(key, 0);
    const plain = Buffer.from('hello world');
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
    const ct = Buffer.concat([cipher.update(plain), cipher.final()]);
    const tag = cipher.getAuthTag();
    const v2Ciphertext = Buffer.concat([iv, tag, ct]);

    // Layer should fall back to v2 parsing automatically
    const decrypted = layer.decrypt(v2Ciphertext);
    assert.deepEqual(decrypted, plain);
  });

  it('rejects tampered ciphertext (GCM auth tag fails)', () => {
    const layer = CryptoLayer.fromKey(key, 1);
    const ct = layer.encrypt(Buffer.from('secret'));
    // Flip a byte in the ciphertext portion
    ct[ct.length - 1] ^= 0xFF;
    assert.throws(() => layer.decrypt(ct), /corrupt|tampered|key salah/);
  });

  it('throws on ciphertext too short', () => {
    const layer = CryptoLayer.fromKey(key, 0);
    assert.throws(
      () => layer.decrypt(Buffer.alloc(10)),
      /terlalu pendek/
    );
  });

  it('fromKey rejects wrong-length key', () => {
    assert.throws(
      () => CryptoLayer.fromKey(Buffer.alloc(16), 0),
      /32 bytes/
    );
  });

  it('fromKey rejects invalid keyVersion', () => {
    assert.throws(
      () => CryptoLayer.fromKey(key, 300),
      /keyVersion/
    );
  });
});

// ── Integration: OvnDB + Collection ──────────────────────────

describe('OvnDB integration security', async () => {
  const { OvnDB } = await import('../../src/index.js');

  // ── 19. Collection name validation at DB level ─────────────

  describe('collection name validation', () => {
    let dir: string;
    let db: Awaited<ReturnType<typeof OvnDB.open>>;

    before(async () => {
      dir = tmpDir();
      db = await OvnDB.open(dir, { fileLock: false });
    });

    after(async () => {
      await db.close();
      await rmDir(dir);
    });

    it('rejects path traversal in collection()', async () => {
      await assert.rejects(
        db.collection('../secret'),
        /tidak valid/
      );
    });

    it('rejects null byte in collection name', async () => {
      await assert.rejects(
        db.collection('col\0name'),
        /tidak valid/
      );
    });

    it('rejects slash in collection name', async () => {
      await assert.rejects(
        db.collection('col/name'),
        /tidak valid/
      );
    });

    it('accepts valid collection name', async () => {
      const col = await db.collection('valid_col');
      assert.ok(col);
    });

    it('rejects path traversal in dropCollection()', async () => {
      await assert.rejects(
        db.dropCollection('../etc'),
        /tidak valid/
      );
    });
  });

  // ── 20. insertOne validation ──────────────────────────────

  describe('insertOne validation', () => {
    let dir: string;
    let db: Awaited<ReturnType<typeof OvnDB.open>>;
    let col: Awaited<ReturnType<typeof db.collection>>;

    before(async () => {
      dir = tmpDir();
      db = await OvnDB.open(dir, { fileLock: false });
      col = await db.collection('test');
    });

    after(async () => {
      await db.close();
      await rmDir(dir);
    });

    it('rejects document with __proto__ key', async () => {
      await assert.rejects(
        col.insertOne({ '__proto__': { admin: true } } as never),
        /terlarang/
      );
    });

    it('rejects oversized document', async () => {
      const bigDoc = { data: 'x'.repeat(17 * 1024 * 1024) };
      await assert.rejects(
        col.insertOne(bigDoc as never),
        /ukuran maksimum/
      );
    });

    it('rejects null byte in custom _id', async () => {
      await assert.rejects(
        col.insertOne({ _id: 'abc\0def' } as never),
        /null byte/
      );
    });

    it('rejects _id longer than 128 chars', async () => {
      await assert.rejects(
        col.insertOne({ _id: 'a'.repeat(129) } as never),
        /terlalu panjang/
      );
    });

    it('inserts valid document successfully', async () => {
      const doc = await col.insertOne({ name: 'Alice', age: 30 });
      assert.equal(doc.name, 'Alice');
      assert.ok(doc._id);
    });
  });

  // ── 21. find / query filter validation ───────────────────

  describe('find filter validation', () => {
    let dir: string;
    let db: Awaited<ReturnType<typeof OvnDB.open>>;
    let col: Awaited<ReturnType<typeof db.collection>>;

    before(async () => {
      dir = tmpDir();
      db = await OvnDB.open(dir, { fileLock: false });
      col = await db.collection('test');
      await col.insertOne({ name: 'Alice', age: 30 });
    });

    after(async () => {
      await db.close();
      await rmDir(dir);
    });

    it('rejects $where in find filter', async () => {
      await assert.rejects(
        col.find({ $where: (() => true) as never }),
        /\$where/
      );
    });

    it('rejects __proto__ as filter field', async () => {
      await assert.rejects(
        col.find({ '__proto__': { polluted: true } } as never),
        /terlarang/
      );
    });

    it('rejects deeply nested filter', async () => {
      let deep: Record<string, unknown> = { name: 'Alice' };
      for (let i = 0; i < 15; i++) deep = { $and: [deep] };
      await assert.rejects(
        col.find(deep as never),
        /terlalu dalam/
      );
    });

    it('finds documents with valid filter', async () => {
      const docs = await col.find({ age: { $gte: 25 } });
      assert.equal(docs.length, 1);
    });
  });

  // ── 22. updateOne validation ─────────────────────────────

  describe('updateOne validation', () => {
    let dir: string;
    let db: Awaited<ReturnType<typeof OvnDB.open>>;
    let col: Awaited<ReturnType<typeof db.collection>>;

    before(async () => {
      dir = tmpDir();
      db = await OvnDB.open(dir, { fileLock: false });
      col = await db.collection('test');
      await col.insertOne({ _id: 'u1', name: 'Alice' });
    });

    after(async () => {
      await db.close();
      await rmDir(dir);
    });

    it('rejects $set with __proto__ key', async () => {
      await assert.rejects(
        col.updateOne({ _id: 'u1' }, { $set: { '__proto__': { admin: true } } }),
        /terlarang/
      );
    });

    it('rejects unknown operator', async () => {
      await assert.rejects(
        col.updateOne({ _id: 'u1' }, { $evil: { x: 1 } } as never),
        /tidak dikenal/
      );
    });

    it('updates normally with valid spec', async () => {
      const ok = await col.updateOne({ _id: 'u1' }, { $set: { name: 'Bob' } });
      assert.equal(ok, true);
      const doc = await col.findById('u1');
      assert.equal(doc?.name, 'Bob');
    });
  });

  // ── 23. aggregate pipeline stage limit ───────────────────

  describe('aggregate stage limit', () => {
    let dir: string;
    let db: Awaited<ReturnType<typeof OvnDB.open>>;
    let col: Awaited<ReturnType<typeof db.collection>>;

    before(async () => {
      dir = tmpDir();
      db = await OvnDB.open(dir, { fileLock: false });
      col = await db.collection('test');
    });

    after(async () => {
      await db.close();
      await rmDir(dir);
    });

    it('rejects pipeline with too many stages', async () => {
      const pipeline = Array.from({ length: 33 }, () => ({ $match: {} }));
      await assert.rejects(
        col.aggregate(pipeline as never),
        /terlalu banyak stage/
      );
    });

    it('runs valid pipeline normally', async () => {
      await col.insertOne({ value: 42 });
      const result = await col.aggregate([{ $match: { value: 42 } }]);
      assert.equal(result.length, 1);
    });
  });

  // ── 24. backup path confinement ───────────────────────────

  describe('backup path confinement', () => {
    let dir: string;
    let db: Awaited<ReturnType<typeof OvnDB.open>>;

    before(async () => {
      dir = tmpDir();
      db = await OvnDB.open(dir, { fileLock: false });
    });

    after(async () => {
      await db.close();
      await rmDir(dir);
    });

    it('rejects backup destination inside data directory', async () => {
      await assert.rejects(
        db.backup(path.join(dir, 'subdir')),
        /tidak boleh berada di dalam/
      );
    });

    it('allows backup to external directory', async () => {
      const backupDir = tmpDir();
      try {
        // Should not throw
        await db.backup(backupDir);
      } finally {
        await rmDir(backupDir);
      }
    });
  });

  // ── 25. Error messages do not leak raw _id ────────────────

  describe('error message sanitization', () => {
    let dir: string;
    let db: Awaited<ReturnType<typeof OvnDB.open>>;
    let col: Awaited<ReturnType<typeof db.collection>>;

    before(async () => {
      dir = tmpDir();
      db = await OvnDB.open(dir, { fileLock: false });
      col = await db.collection('test');
      await col.insertOne({ _id: 'existing-id', name: 'Alice' });
    });

    after(async () => {
      await db.close();
      await rmDir(dir);
    });

    it('duplicate _id error does not contain the raw _id value', async () => {
      try {
        await col.insertOne({ _id: 'existing-id', name: 'Bob' });
        assert.fail('Should have thrown');
      } catch (err) {
        const msg = (err as Error).message;
        // The raw _id value must NOT appear in the error message
        assert.ok(
          !msg.includes('existing-id'),
          `Error message must not contain raw _id: "${msg}"`
        );
      }
    });
  });
});

// ── Integration: HMAC manifest integrity ─────────────────────

describe('HMAC manifest integrity', async () => {
  const { OvnDB } = await import('../../src/index.js');
  const nodeCrypto = await import('node:crypto');

  const integrityKey = nodeCrypto.randomBytes(32);

  it('opens and closes cleanly with integrityKey', async () => {
    const dir = tmpDir();
    try {
      const db = await OvnDB.open(dir, { fileLock: false, integrityKey });
      const col = await db.collection('items');
      await col.insertOne({ name: 'test' });
      await db.close();
      // Re-open — should pass HMAC verification
      const db2 = await OvnDB.open(dir, { fileLock: false, integrityKey });
      await db2.close();
    } finally {
      await rmDir(dir);
    }
  });

  it('rejects tampered manifest (HMAC mismatch)', async () => {
    const dir = tmpDir();
    try {
      const db = await OvnDB.open(dir, { fileLock: false, integrityKey });
      const col = await db.collection('items');
      await col.insertOne({ name: 'test' });
      await db.close();

      // Tamper with the manifest file directly
      const manifestPath = path.join(dir, 'items', 'items.manifest.json');
      const raw = JSON.parse(await fsp.readFile(manifestPath, 'utf8'));
      raw.totalLive = '9999'; // tamper content
      await fsp.writeFile(manifestPath, JSON.stringify(raw, null, 2));

      // Re-open should throw HMAC mismatch
      await assert.rejects(
        async () => {
          const db2 = await OvnDB.open(dir, { fileLock: false, integrityKey });
          await db2.collection('items'); // triggers open()
          await db2.close();
        },
        /HMAC mismatch|mismatch/i
      );
    } finally {
      await rmDir(dir);
    }
  });

  it('rejects wrong integrityKey', async () => {
    const dir = tmpDir();
    try {
      const db = await OvnDB.open(dir, { fileLock: false, integrityKey });
      const col = await db.collection('items');
      await col.insertOne({ name: 'test' });
      await db.close();

      // Re-open with different key
      const wrongKey = nodeCrypto.randomBytes(32);
      await assert.rejects(
        async () => {
          const db2 = await OvnDB.open(dir, { fileLock: false, integrityKey: wrongKey });
          await db2.collection('items');
          await db2.close();
        },
        /HMAC mismatch|mismatch/i
      );
    } finally {
      await rmDir(dir);
    }
  });

  it('throws on integrityKey that is not 32 bytes', async () => {
    const dir = tmpDir();
    try {
      await assert.rejects(
        async () => { await OvnDB.open(dir, { fileLock: false, integrityKey: nodeCrypto.randomBytes(16) }); },
        /32 byte/
      );
    } finally {
      await rmDir(dir);
    }
  });

  it('manifest written without integrityKey uses sha256: prefix (backward compat)', async () => {
    const dir = tmpDir();
    try {
      const db = await OvnDB.open(dir, { fileLock: false }); // no integrityKey
      const col = await db.collection('items');
      await col.insertOne({ name: 'x' });
      await db.close();

      const manifestPath = path.join(dir, 'items', 'items.manifest.json');
      const raw = JSON.parse(await fsp.readFile(manifestPath, 'utf8'));
      assert.ok(
        typeof raw.checksum === 'string' && raw.checksum.startsWith('sha256:'),
        `Expected sha256: prefix, got: ${raw.checksum}`
      );

      // Re-open without key should work
      const db2 = await OvnDB.open(dir, { fileLock: false });
      await db2.close();
    } finally {
      await rmDir(dir);
    }
  });
});

describe('WAL append length caps', async () => {
  const { WAL } = await import('../../src/core/wal/wal.js');
  const { WalOp } = await import('../../src/types/constants.js');

  it('rejects key longer than 1024 bytes', async () => {
    const dir = tmpDir();
    const wal = new WAL(dir, 'test');
    await wal.open();
    try {
      await assert.rejects(
        wal.append(WalOp.INSERT, 'k'.repeat(1025), Buffer.from('data')),
        /terlalu panjang/
      );
    } finally {
      await wal.close();
      await rmDir(dir);
    }
  });

  it('rejects data larger than 32 MB', async () => {
    const dir = tmpDir();
    const wal = new WAL(dir, 'test');
    await wal.open();
    try {
      await assert.rejects(
        wal.append(WalOp.INSERT, 'key', Buffer.alloc(33 * 1024 * 1024)),
        /terlalu besar/
      );
    } finally {
      await wal.close();
      await rmDir(dir);
    }
  });

  it('accepts normal key and data', async () => {
    const dir = tmpDir();
    const wal = new WAL(dir, 'test');
    await wal.open();
    try {
      await wal.append(WalOp.INSERT, 'normal-key', Buffer.from('{"_id":"x"}'));
    } finally {
      await wal.close();
      await rmDir(dir);
    }
  });
});
