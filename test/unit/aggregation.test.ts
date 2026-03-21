// ============================================================
//  Unit Test — Aggregation Pipeline
//  Test: $match $project $group $sort $limit $skip $unwind $count $addFields
//        Accumulators: $sum $avg $min $max $push $first $last $addToSet
// ============================================================

import assert from 'node:assert/strict';
import { test, describe } from 'node:test';
import { compilePipeline } from '../../src/core/query/aggregation.js';

// Dataset sampel
const USERS: Record<string, unknown>[] = [
  { _id: 'u1', name: 'Budi',  city: 'Jakarta', age: 25, points: 100, tags: ['nodejs'] },
  { _id: 'u2', name: 'Siti',  city: 'Bandung', age: 30, points: 200, tags: ['python', 'ml'] },
  { _id: 'u3', name: 'Ahmad', city: 'Jakarta', age: 22, points: 150, tags: ['nodejs', 'ts'] },
  { _id: 'u4', name: 'Dewi',  city: 'Bandung', age: 28, points:  50, tags: ['java'] },
  { _id: 'u5', name: 'Rudi',  city: 'Surabaya',age: 35, points: 300, tags: ['nodejs', 'go'] },
];

describe('$match', () => {
  test('filter dokumen yang cocok', async () => {
    const run   = compilePipeline([{ $match: { city: 'Jakarta' } }]);
    const result = await run([...USERS]);
    assert.equal(result.length, 2);
    assert.ok(result.every(d => d['city'] === 'Jakarta'));
  });

  test('$match dengan range', async () => {
    const run   = compilePipeline([{ $match: { age: { $gte: 28 } } }]);
    const result = await run([...USERS]);
    assert.equal(result.length, 3); // Siti(30), Dewi(28), Rudi(35)
  });
});

describe('$project', () => {
  test('inclusion mode', async () => {
    const run   = compilePipeline([{ $project: { name: 1, city: 1 } }]);
    const result = await run([...USERS]);
    assert.ok(result.every(d => 'name' in d && 'city' in d && !('age' in d)));
  });

  test('field reference dengan $fieldName', async () => {
    const run   = compilePipeline([{ $project: { alias: '$name', _id: 0 } }]);
    const result = await run([{ _id: 'u1', name: 'Budi' }]);
    assert.equal(result[0]!['alias'], 'Budi');
    assert.ok(!('_id' in result[0]!));
  });
});

describe('$group', () => {
  test('group by city, hitung $sum: 1', async () => {
    const run   = compilePipeline([{ $group: { _id: '$city', count: { $sum: 1 } } }]);
    const result = await run([...USERS]);
    const jakarta = result.find(d => d['_id'] === 'Jakarta');
    const bandung = result.find(d => d['_id'] === 'Bandung');
    assert.equal(jakarta?.['count'], 2);
    assert.equal(bandung?.['count'], 2);
  });

  test('$avg accumulator', async () => {
    const run   = compilePipeline([{ $group: { _id: '$city', avgAge: { $avg: '$age' } } }]);
    const result = await run([...USERS]);
    const jakarta = result.find(d => d['_id'] === 'Jakarta')!;
    assert.ok(Math.abs((jakarta['avgAge'] as number) - 23.5) < 0.01); // (25+22)/2
  });

  test('$min dan $max', async () => {
    const run   = compilePipeline([{ $group: { _id: null, minPts: { $min: '$points' }, maxPts: { $max: '$points' } } }]);
    const result = await run([...USERS]);
    assert.equal(result[0]!['minPts'], 50);
    assert.equal(result[0]!['maxPts'], 300);
  });

  test('$push: kumpulkan nilai ke array', async () => {
    const run   = compilePipeline([{ $group: { _id: '$city', names: { $push: '$name' } } }]);
    const result = await run([...USERS]);
    const jakarta = result.find(d => d['_id'] === 'Jakarta')!;
    assert.ok(Array.isArray(jakarta['names']));
    assert.ok((jakarta['names'] as string[]).includes('Budi'));
    assert.ok((jakarta['names'] as string[]).includes('Ahmad'));
  });

  test('$addToSet: nilai unik', async () => {
    const docs = [
      { _id: 'a', city: 'A', lang: 'js' },
      { _id: 'b', city: 'A', lang: 'js' },
      { _id: 'c', city: 'A', lang: 'ts' },
    ];
    const run   = compilePipeline([{ $group: { _id: '$city', langs: { $addToSet: '$lang' } } }]);
    const result = await run(docs);
    const langs = (result[0]!['langs'] as string[]).sort();
    assert.deepEqual(langs, ['js', 'ts']);
  });

  test('$first dan $last', async () => {
    const docs  = [{ _id: 'a', g: 'x', v: 1 }, { _id: 'b', g: 'x', v: 2 }, { _id: 'c', g: 'x', v: 3 }];
    const run   = compilePipeline([{ $group: { _id: '$g', first: { $first: '$v' }, last: { $last: '$v' } } }]);
    const result = await run(docs);
    assert.equal(result[0]!['first'], 1);
    assert.equal(result[0]!['last'],  3);
  });
});

describe('$sort', () => {
  test('sort ascending', async () => {
    const run   = compilePipeline([{ $sort: { points: 1 } }]);
    const result = await run([...USERS]);
    const pts   = result.map(d => d['points']);
    assert.deepEqual(pts, [...pts].sort((a, b) => (a as number) - (b as number)));
  });

  test('sort descending', async () => {
    const run   = compilePipeline([{ $sort: { points: -1 } }]);
    const result = await run([...USERS]);
    const pts   = result.map(d => d['points']);
    assert.deepEqual(pts, [...pts].sort((a, b) => (b as number) - (a as number)));
  });
});

describe('$limit dan $skip', () => {
  test('$limit membatasi jumlah dokumen', async () => {
    const run   = compilePipeline([{ $limit: 3 }]);
    const result = await run([...USERS]);
    assert.equal(result.length, 3);
  });

  test('$skip melewati N dokumen', async () => {
    const run   = compilePipeline([{ $skip: 3 }]);
    const result = await run([...USERS]);
    assert.equal(result.length, 2);
  });

  test('$skip + $limit untuk pagination', async () => {
    const run   = compilePipeline([{ $skip: 2 }, { $limit: 2 }]);
    const result = await run([...USERS]);
    assert.equal(result.length, 2);
    assert.equal(result[0]!['_id'], 'u3'); // dokumen ke-3 (0-indexed ke-2)
  });
});

describe('$unwind', () => {
  test('expand array field menjadi satu dokumen per elemen', async () => {
    const run   = compilePipeline([{ $unwind: '$tags' }]);
    const result = await run([...USERS]);
    // u1:1, u2:2, u3:2, u4:1, u5:2 = 8 dokumen
    assert.equal(result.length, 8);
    // Setiap dokumen hasil punya tags berupa scalar, bukan array
    assert.ok(result.every(d => typeof d['tags'] === 'string'));
  });
});

describe('$count', () => {
  test('hitung total dan simpan ke field', async () => {
    const run   = compilePipeline([{ $count: 'total' }]);
    const result = await run([...USERS]);
    assert.equal(result.length, 1);
    assert.equal(result[0]!['total'], 5);
  });
});

describe('$addFields', () => {
  test('tambah field baru', async () => {
    const run   = compilePipeline([{ $addFields: { isVip: { $gte: 200 } } }]);
    const result = await run([{ _id: 'u1', points: 300 }]);
    assert.ok('isVip' in result[0]!);
  });
});

describe('Pipeline kombinasi', () => {
  test('top 3 kota berdasarkan total points', async () => {
    const run = compilePipeline([
      { $group: { _id: '$city', totalPoints: { $sum: '$points' } } },
      { $sort: { totalPoints: -1 } },
      { $limit: 2 },
    ]);
    const result = await run([...USERS]);
    assert.equal(result.length, 2);
    assert.ok((result[0]!['totalPoints'] as number) >= (result[1]!['totalPoints'] as number));
  });

  test('cari user Jakarta, sort by age, ambil 1', async () => {
    const run = compilePipeline([
      { $match: { city: 'Jakarta' } },
      { $sort: { age: -1 } },
      { $limit: 1 },
      { $project: { name: 1, age: 1 } },
    ]);
    const result = await run([...USERS]);
    assert.equal(result.length, 1);
    assert.equal(result[0]!['name'], 'Budi'); // Budi lebih tua (25 vs Ahmad 22)
  });
});
