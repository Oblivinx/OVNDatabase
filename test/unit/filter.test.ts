// ============================================================
//  Unit Test — Query Filter & Update Engine
//  Test: semua operator $eq $ne $gt $gte $lt $lte $in $nin
//        $and $or $nor $not $exists $regex $size $all $elemMatch
//        applyUpdate: $set $unset $inc $mul $push $pull $addToSet $min $max $rename
//        applyProjection: include/exclude mode
// ============================================================

import assert from 'node:assert/strict';
import { test, describe } from 'node:test';
import { matchFilter, applyUpdate, applyProjection, getFieldValue } from '../../src/core/query/filter.js';

describe('matchFilter — comparison operators', () => {

  const doc = { _id: 'u1', name: 'Budi', age: 25, score: 100, active: true, email: null };

  test('$eq: kecocokan nilai tepat', () => {
    assert.ok(matchFilter(doc, { age: { $eq: 25 } }));
    assert.ok(!matchFilter(doc, { age: { $eq: 30 } }));
  });

  test('perbandingan langsung (implicit $eq)', () => {
    assert.ok(matchFilter(doc, { name: 'Budi' }));
    assert.ok(!matchFilter(doc, { name: 'Siti' }));
  });

  test('$ne', () => {
    assert.ok(matchFilter(doc, { age: { $ne: 30 } }));
    assert.ok(!matchFilter(doc, { age: { $ne: 25 } }));
  });

  test('$gt / $gte', () => {
    assert.ok(matchFilter(doc, { age: { $gt: 20 } }));
    assert.ok(!matchFilter(doc, { age: { $gt: 25 } }));
    assert.ok(matchFilter(doc, { age: { $gte: 25 } }));
  });

  test('$lt / $lte', () => {
    assert.ok(matchFilter(doc, { age: { $lt: 30 } }));
    assert.ok(!matchFilter(doc, { age: { $lt: 25 } }));
    assert.ok(matchFilter(doc, { age: { $lte: 25 } }));
  });

  test('$in', () => {
    assert.ok(matchFilter(doc, { age: { $in: [20, 25, 30] } }));
    assert.ok(!matchFilter(doc, { age: { $in: [10, 20] } }));
  });

  test('$nin', () => {
    assert.ok(matchFilter(doc, { age: { $nin: [10, 20] } }));
    assert.ok(!matchFilter(doc, { age: { $nin: [25, 30] } }));
  });

  test('$exists: true', () => {
    assert.ok(matchFilter(doc, { name: { $exists: true } }));
    assert.ok(!matchFilter(doc, { phone: { $exists: true } }));
  });

  test('$exists: false', () => {
    assert.ok(matchFilter(doc, { phone: { $exists: false } }));
    assert.ok(!matchFilter(doc, { name: { $exists: false } }));
  });

  test('$regex dengan string', () => {
    assert.ok(matchFilter(doc, { name: { $regex: '^Bu' } }));
    assert.ok(!matchFilter(doc, { name: { $regex: '^Si' } }));
  });

  test('$regex dengan RegExp', () => {
    assert.ok(matchFilter(doc, { name: { $regex: /budi/i } }));
  });
});

describe('matchFilter — logical operators', () => {

  const doc = { _id: 'u1', age: 25, role: 'admin', active: true };

  test('$and: semua kondisi harus terpenuhi', () => {
    assert.ok(matchFilter(doc, { $and: [{ age: { $gte: 18 } }, { role: 'admin' }] }));
    assert.ok(!matchFilter(doc, { $and: [{ age: { $gte: 18 } }, { role: 'user' }] }));
  });

  test('$or: minimal satu kondisi terpenuhi', () => {
    assert.ok(matchFilter(doc, { $or: [{ role: 'user' }, { active: true }] }));
    assert.ok(!matchFilter(doc, { $or: [{ role: 'user' }, { age: 99 }] }));
  });

  test('$nor: tidak ada kondisi yang terpenuhi', () => {
    assert.ok(matchFilter(doc, { $nor: [{ role: 'user' }, { age: 99 }] }));
    assert.ok(!matchFilter(doc, { $nor: [{ role: 'admin' }, { age: 99 }] }));
  });

  test('$not: negasi kondisi', () => {
    assert.ok(matchFilter(doc, { $not: { role: 'user' } }));
    assert.ok(!matchFilter(doc, { $not: { role: 'admin' } }));
  });

  test('filter kosong {} cocok dengan semua dokumen', () => {
    assert.ok(matchFilter(doc, {}));
  });
});

describe('matchFilter — array operators', () => {

  const doc = { _id: 'd1', tags: ['nodejs', 'typescript', 'database'], scores: [80, 90, 95] };

  test('$size: cocok dengan panjang array', () => {
    assert.ok(matchFilter(doc, { tags: { $size: 3 } }));
    assert.ok(!matchFilter(doc, { tags: { $size: 2 } }));
  });

  test('$all: array harus mengandung semua elemen', () => {
    assert.ok(matchFilter(doc, { tags: { $all: ['nodejs', 'typescript'] } }));
    assert.ok(!matchFilter(doc, { tags: { $all: ['nodejs', 'python'] } }));
  });

  test('$elemMatch: elemen array harus cocok dengan kondisi', () => {
    assert.ok(matchFilter(doc, { scores: { $elemMatch: { $gte: 90 } } }));
    assert.ok(!matchFilter(doc, { scores: { $elemMatch: { $gte: 100 } } }));
  });
});

describe('matchFilter — dot notation', () => {

  const doc = { _id: 'u1', address: { city: 'Jakarta', zip: '10110' }, meta: { level: 3 } };

  test('dot notation: field bersarang', () => {
    assert.ok(matchFilter(doc, { 'address.city': 'Jakarta' }));
    assert.ok(!matchFilter(doc, { 'address.city': 'Bandung' }));
  });

  test('dot notation: level lebih dalam', () => {
    assert.ok(matchFilter(doc, { 'meta.level': { $gte: 3 } }));
  });
});

describe('applyUpdate — update operators', () => {

  test('$set: set field', () => {
    const doc    = { _id: 'u1', name: 'Budi', points: 0 };
    const result = applyUpdate(doc, { $set: { name: 'Siti', points: 100 } });
    assert.equal(result['name'], 'Siti');
    assert.equal(result['points'], 100);
  });

  test('$unset: hapus field', () => {
    const doc    = { _id: 'u1', name: 'Budi', temp: 'delete me' };
    const result = applyUpdate(doc, { $unset: { temp: 1 } });
    assert.equal(result['temp'], undefined);
    assert.equal(result['name'], 'Budi'); // field lain tidak berubah
  });

  test('$inc: tambah nilai numerik', () => {
    const doc    = { _id: 'u1', points: 100 };
    const result = applyUpdate(doc, { $inc: { points: 50 } });
    assert.equal(result['points'], 150);
  });

  test('$inc: kurangi dengan nilai negatif', () => {
    const doc    = { _id: 'u1', balance: 1000 };
    const result = applyUpdate(doc, { $inc: { balance: -200 } });
    assert.equal(result['balance'], 800);
  });

  test('$mul: kalikan nilai', () => {
    const doc    = { _id: 'u1', price: 100 };
    const result = applyUpdate(doc, { $mul: { price: 1.1 } });
    assert.ok(Math.abs((result['price'] as number) - 110) < 0.001);
  });

  test('$push: tambah ke array', () => {
    const doc    = { _id: 'u1', tags: ['a', 'b'] };
    const result = applyUpdate(doc, { $push: { tags: 'c' } });
    assert.deepEqual(result['tags'], ['a', 'b', 'c']);
  });

  test('$push dengan $each dan $slice', () => {
    const doc    = { _id: 'u1', scores: [10, 20, 30] };
    const result = applyUpdate(doc, { $push: { scores: { $each: [40, 50], $slice: 3 } } });
    assert.deepEqual(result['scores'], [10, 20, 30]); // slice ke 3 dari kiri
  });

  test('$pull: hapus elemen dari array', () => {
    const doc    = { _id: 'u1', tags: ['a', 'b', 'c'] };
    const result = applyUpdate(doc, { $pull: { tags: 'b' } });
    assert.deepEqual(result['tags'], ['a', 'c']);
  });

  test('$addToSet: tidak duplikat', () => {
    const doc    = { _id: 'u1', tags: ['a', 'b'] };
    const r1     = applyUpdate(doc, { $addToSet: { tags: 'a' } }); // duplikat
    const r2     = applyUpdate(doc, { $addToSet: { tags: 'c' } }); // baru
    assert.deepEqual(r1['tags'], ['a', 'b']);
    assert.deepEqual(r2['tags'], ['a', 'b', 'c']);
  });

  test('$min dan $max', () => {
    const doc = { _id: 'u1', score: 50 };
    assert.equal(applyUpdate(doc, { $min: { score: 30 } })['score'], 30); // 30 < 50 → update
    assert.equal(applyUpdate(doc, { $min: { score: 70 } })['score'], 50); // 70 > 50 → no update
    assert.equal(applyUpdate(doc, { $max: { score: 70 } })['score'], 70); // 70 > 50 → update
    assert.equal(applyUpdate(doc, { $max: { score: 30 } })['score'], 50); // 30 < 50 → no update
  });

  test('$rename: ganti nama field', () => {
    const doc    = { _id: 'u1', oldName: 'value' };
    const result = applyUpdate(doc, { $rename: { oldName: 'newName' } });
    assert.equal(result['newName'], 'value');
    assert.equal(result['oldName'], undefined);
  });

  test('tidak memodifikasi dokumen asli (immutable)', () => {
    const doc    = { _id: 'u1', points: 100 };
    applyUpdate(doc, { $inc: { points: 50 } });
    assert.equal(doc.points, 100); // tidak berubah
  });
});

describe('applyProjection', () => {

  const doc = { _id: 'u1', name: 'Budi', age: 25, password: 'secret', email: 'b@b.com' };

  test('inclusion mode: hanya field yang di-set 1', () => {
    const result = applyProjection(doc, { name: 1, email: 1 });
    assert.ok('name'  in result);
    assert.ok('email' in result);
    assert.ok('_id'   in result); // _id selalu include kecuali eksplisit exclude
    assert.ok(!('age'      in result));
    assert.ok(!('password' in result));
  });

  test('exclusion mode: semua field kecuali yang di-set 0', () => {
    const result = applyProjection(doc, { password: 0 });
    assert.ok('name'  in result);
    assert.ok('age'   in result);
    assert.ok('email' in result);
    assert.ok(!('password' in result));
  });

  test('exclude _id secara eksplisit', () => {
    const result = applyProjection(doc, { name: 1, _id: 0 });
    assert.ok(!('_id' in result));
    assert.ok('name' in result);
  });

  test('projection kosong → kembalikan semua field', () => {
    const result = applyProjection(doc, {});
    assert.deepEqual(result, doc);
  });
});

describe('getFieldValue — dot notation', () => {

  const doc = { a: { b: { c: 42 } }, arr: [1, 2, 3] };

  test('top-level field', () => {
    assert.equal(getFieldValue(doc, 'arr'), doc.arr);
  });

  test('nested field dengan dot notation', () => {
    assert.equal(getFieldValue(doc, 'a.b.c'), 42);
  });

  test('field tidak ada → undefined', () => {
    assert.equal(getFieldValue(doc, 'x.y.z'), undefined);
  });
});
