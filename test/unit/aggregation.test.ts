import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { compilePipeline } from '../../src/core/query/aggregation.js';

const noResolver = async (_: string) => [] as Record<string, unknown>[];

const sampleDocs: Record<string, unknown>[] = [
  { _id: '1', name: 'Alice', age: 30, city: 'Jakarta', tags: ['a', 'b'] },
  { _id: '2', name: 'Bob',   age: 25, city: 'Bandung', tags: ['b', 'c'] },
  { _id: '3', name: 'Charlie', age: 35, city: 'Jakarta', tags: ['a'] },
  { _id: '4', name: 'Diana', age: 28, city: 'Bandung', tags: ['c'] },
];

// ── $match ────────────────────────────────────────────────────

describe('aggregation — $match', () => {
  it('filters documents', async () => {
    const result = await compilePipeline([{ $match: { city: 'Jakarta' } }], noResolver)(sampleDocs);
    assert.equal(result.length, 2);
    assert.ok(result.every(d => d['city'] === 'Jakarta'));
  });

  it('matches with operators', async () => {
    const result = await compilePipeline([{ $match: { age: { $gte: 30 } } }], noResolver)(sampleDocs);
    assert.equal(result.length, 2);
  });
});

// ── $project ──────────────────────────────────────────────────

describe('aggregation — $project', () => {
  it('includes specified fields', async () => {
    const result = await compilePipeline([
      { $project: { name: 1, age: 1 } },
    ], noResolver)(sampleDocs);
    assert.ok(result[0]!['_id']); // _id included by default
    assert.equal(result[0]!['name'], 'Alice');
    assert.equal(result[0]!['city'], undefined);
  });

  it('excludes _id when set to 0', async () => {
    const result = await compilePipeline([
      { $project: { _id: 0, name: 1 } },
    ], noResolver)(sampleDocs);
    assert.equal(result[0]!['_id'], undefined);
  });

  it('supports field references ($field)', async () => {
    const result = await compilePipeline([
      { $project: { location: '$city' } },
    ], noResolver)(sampleDocs);
    assert.equal(result[0]!['location'], 'Jakarta');
  });
});

// ── $group ────────────────────────────────────────────────────

describe('aggregation — $group', () => {
  it('groups by field with $sum', async () => {
    const result = await compilePipeline([
      { $group: { _id: '$city', total: { $sum: '$age' } } },
    ], noResolver)(sampleDocs);
    const jakarta = result.find(d => d['_id'] === 'Jakarta');
    assert.equal(jakarta?.['total'], 65); // 30 + 35
  });

  it('groups with $count', async () => {
    const result = await compilePipeline([
      { $group: { _id: '$city', count: { $count: {} } } },
    ], noResolver)(sampleDocs);
    const bandung = result.find(d => d['_id'] === 'Bandung');
    assert.equal(bandung?.['count'], 2);
  });

  it('groups with $avg', async () => {
    const result = await compilePipeline([
      { $group: { _id: '$city', avgAge: { $avg: '$age' } } },
    ], noResolver)(sampleDocs);
    const jakarta = result.find(d => d['_id'] === 'Jakarta');
    assert.equal(jakarta?.['avgAge'], 32.5);
  });

  it('groups with $min and $max', async () => {
    const result = await compilePipeline([
      { $group: { _id: null, minAge: { $min: '$age' }, maxAge: { $max: '$age' } } },
    ], noResolver)(sampleDocs);
    assert.equal(result[0]?.['minAge'], 25);
    assert.equal(result[0]?.['maxAge'], 35);
  });

  it('groups with $push', async () => {
    const result = await compilePipeline([
      { $group: { _id: '$city', names: { $push: '$name' } } },
    ], noResolver)(sampleDocs);
    const jakarta = result.find(d => d['_id'] === 'Jakarta');
    assert.deepEqual((jakarta?.['names'] as string[]).sort(), ['Alice', 'Charlie']);
  });

  it('groups with $first / $last', async () => {
    const result = await compilePipeline([
      { $sort: { age: 1 } },
      { $group: { _id: null, youngest: { $first: '$name' }, oldest: { $last: '$name' } } },
    ], noResolver)(sampleDocs);
    assert.equal(result[0]?.['youngest'], 'Bob');
    assert.equal(result[0]?.['oldest'], 'Charlie');
  });
});

// ── $sort ─────────────────────────────────────────────────────

describe('aggregation — $sort', () => {
  it('sorts ascending', async () => {
    const result = await compilePipeline([{ $sort: { age: 1 } }], noResolver)(sampleDocs);
    assert.equal(result[0]!['name'], 'Bob');
    assert.equal(result[3]!['name'], 'Charlie');
  });

  it('sorts descending', async () => {
    const result = await compilePipeline([{ $sort: { age: -1 } }], noResolver)(sampleDocs);
    assert.equal(result[0]!['name'], 'Charlie');
  });
});

// ── $limit / $skip ────────────────────────────────────────────

describe('aggregation — $limit / $skip', () => {
  it('limits results', async () => {
    const result = await compilePipeline([{ $limit: 2 }], noResolver)(sampleDocs);
    assert.equal(result.length, 2);
  });

  it('skips results', async () => {
    const result = await compilePipeline([{ $skip: 3 }], noResolver)(sampleDocs);
    assert.equal(result.length, 1);
  });

  it('rejects negative $limit', async () => {
    await assert.rejects(
      compilePipeline([{ $limit: -1 }], noResolver)(sampleDocs),
      /positif/,
    );
  });

  it('rejects negative $skip', async () => {
    await assert.rejects(
      compilePipeline([{ $skip: -1 }], noResolver)(sampleDocs),
      /positif/,
    );
  });
});

// ── $unwind ───────────────────────────────────────────────────

describe('aggregation — $unwind', () => {
  it('unwinds array field', async () => {
    const result = await compilePipeline([{ $unwind: '$tags' }], noResolver)(sampleDocs);
    assert.ok(result.length > sampleDocs.length);
    assert.ok(result.every(d => !Array.isArray(d['tags'])));
  });

  it('preserves null/empty arrays when configured', async () => {
    const docs = [
      { _id: '1', tags: ['a'] },
      { _id: '2', tags: [] },
    ];
    const result = await compilePipeline([
      { $unwind: { path: '$tags', preserveNullAndEmptyArrays: true } as never },
    ], noResolver)(docs);
    assert.ok(result.some(d => d['_id'] === '2'));
  });
});

// ── $count ────────────────────────────────────────────────────

describe('aggregation — $count', () => {
  it('counts documents', async () => {
    const result = await compilePipeline([{ $count: 'total' as never }], noResolver)(sampleDocs);
    assert.deepEqual(result, [{ total: 4 }]);
  });
});

// ── $addFields ────────────────────────────────────────────────

describe('aggregation — $addFields', () => {
  it('adds computed fields', async () => {
    const result = await compilePipeline([
      { $addFields: { fullName: '$name' } },
    ], noResolver)(sampleDocs);
    assert.equal(result[0]!['fullName'], 'Alice');
  });

  it('adds constant fields', async () => {
    const result = await compilePipeline([
      { $addFields: { status: 'active' } },
    ], noResolver)(sampleDocs);
    assert.ok(result.every(d => d['status'] === 'active'));
  });
});

// ── $lookup ───────────────────────────────────────────────────

describe('aggregation — $lookup', () => {
  it('joins with foreign collection', async () => {
    const orders = [
      { _id: 'o1', userId: '1', amount: 100 },
      { _id: 'o2', userId: '1', amount: 200 },
      { _id: 'o3', userId: '2', amount: 50 },
    ];
    const resolver = async (c: string) => {
      if (c === 'orders') return orders;
      return [];
    };
    const result = await compilePipeline([
      { $lookup: { from: 'orders', localField: '_id', foreignField: 'userId', as: 'orders' } },
    ], resolver)(sampleDocs);
    const alice = result.find(d => d['_id'] === '1');
    assert.equal((alice?.['orders'] as unknown[]).length, 2);
  });
});

// ── Expression evaluator ─────────────────────────────────────

describe('aggregation — expressions', () => {
  it('$concat concatenates strings', async () => {
    const result = await compilePipeline([
      { $project: { greeting: { $concat: ['Hello ', '$name'] } } },
    ], noResolver)(sampleDocs);
    assert.equal(result[0]!['greeting'], 'Hello Alice');
  });

  it('$add adds numbers', async () => {
    const result = await compilePipeline([
      { $project: { agePlus10: { $add: ['$age', 10] } } },
    ], noResolver)(sampleDocs);
    assert.equal(result[0]!['agePlus10'], 40);
  });

  it('$subtract subtracts', async () => {
    const result = await compilePipeline([
      { $project: { ageMinus5: { $subtract: ['$age', 5] } } },
    ], noResolver)(sampleDocs);
    assert.equal(result[0]!['ageMinus5'], 25);
  });

  it('$multiply multiplies', async () => {
    const result = await compilePipeline([
      { $project: { ageX2: { $multiply: ['$age', 2] } } },
    ], noResolver)(sampleDocs);
    assert.equal(result[0]!['ageX2'], 60);
  });

  it('$cond conditional', async () => {
    const result = await compilePipeline([
      { $project: { label: { $cond: { if: { age: { $gte: 30 } }, then: 'senior', else: 'junior' } } } },
    ], noResolver)(sampleDocs);
    assert.equal(result[0]!['label'], 'senior');  // Alice age=30
    assert.equal(result[1]!['label'], 'junior');  // Bob age=25
  });

  it('$ifNull provides fallback', async () => {
    const docs = [{ _id: '1', name: 'Alice', nickname: null }];
    const result = await compilePipeline([
      { $project: { display: { $ifNull: ['$nickname', 'N/A'] } } },
    ], noResolver)(docs);
    assert.equal(result[0]!['display'], 'N/A');
  });
});

// ── Multi-stage pipeline ─────────────────────────────────────

describe('aggregation — multi-stage', () => {
  it('match → group → sort → limit', async () => {
    const result = await compilePipeline([
      { $match: { age: { $gte: 25 } } },
      { $group: { _id: '$city', count: { $sum: 1 } } },
      { $sort: { count: -1 } },
      { $limit: 1 },
    ], noResolver)(sampleDocs);
    assert.equal(result.length, 1);
    assert.equal(result[0]!['count'], 2); // Both cities have 2
  });
});
