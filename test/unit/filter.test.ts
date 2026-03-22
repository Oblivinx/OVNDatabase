import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  matchFilter, getFieldValue, setNestedField, getNestedField,
  deleteNestedField, applyUpdate, applyProjection,
} from '../../src/core/query/filter.js';

// ── matchFilter ──────────────────────────────────────────────

describe('matchFilter — comparison operators', () => {
  const doc = { _id: '1', name: 'Alice', age: 30, city: 'Jakarta' };

  it('$eq matches equal value', () => {
    assert.equal(matchFilter(doc, { name: { $eq: 'Alice' } }), true);
    assert.equal(matchFilter(doc, { name: { $eq: 'Bob' } }), false);
  });

  it('$ne matches not-equal value', () => {
    assert.equal(matchFilter(doc, { name: { $ne: 'Bob' } }), true);
    assert.equal(matchFilter(doc, { name: { $ne: 'Alice' } }), false);
  });

  it('$gt / $gte / $lt / $lte', () => {
    assert.equal(matchFilter(doc, { age: { $gt: 25 } }), true);
    assert.equal(matchFilter(doc, { age: { $gt: 30 } }), false);
    assert.equal(matchFilter(doc, { age: { $gte: 30 } }), true);
    assert.equal(matchFilter(doc, { age: { $lt: 35 } }), true);
    assert.equal(matchFilter(doc, { age: { $lt: 30 } }), false);
    assert.equal(matchFilter(doc, { age: { $lte: 30 } }), true);
  });

  it('implicit $eq (scalar match)', () => {
    assert.equal(matchFilter(doc, { name: 'Alice' }), true);
    assert.equal(matchFilter(doc, { name: 'Bob' }), false);
  });
});

describe('matchFilter — $in / $nin', () => {
  const doc = { _id: '1', role: 'admin' };

  it('$in matches when value is in array', () => {
    assert.equal(matchFilter(doc, { role: { $in: ['admin', 'user'] } }), true);
    assert.equal(matchFilter(doc, { role: { $in: ['user', 'guest'] } }), false);
  });

  it('$nin matches when value is NOT in array', () => {
    assert.equal(matchFilter(doc, { role: { $nin: ['user', 'guest'] } }), true);
    assert.equal(matchFilter(doc, { role: { $nin: ['admin', 'user'] } }), false);
  });
});

describe('matchFilter — $exists', () => {
  const doc = { _id: '1', name: 'Alice' };

  it('$exists: true matches existing field', () => {
    assert.equal(matchFilter(doc, { name: { $exists: true } }), true);
    assert.equal(matchFilter(doc, { missing: { $exists: true } }), false);
  });

  it('$exists: false matches missing field', () => {
    assert.equal(matchFilter(doc, { missing: { $exists: false } }), true);
    assert.equal(matchFilter(doc, { name: { $exists: false } }), false);
  });
});

describe('matchFilter — $regex', () => {
  const doc = { _id: '1', name: 'Alice' };

  it('matches with string regex', () => {
    assert.equal(matchFilter(doc, { name: { $regex: '^Al' } }), true);
    assert.equal(matchFilter(doc, { name: { $regex: '^Bo' } }), false);
  });

  it('matches with RegExp object', () => {
    assert.equal(matchFilter(doc, { name: { $regex: /alice/i } }), true);
  });

  it('returns false for non-string field', () => {
    assert.equal(matchFilter({ age: 30 }, { age: { $regex: '30' } }), false);
  });
});

describe('matchFilter — array operators', () => {
  const doc = { _id: '1', tags: ['a', 'b', 'c'] };

  it('$size matches array length', () => {
    assert.equal(matchFilter(doc, { tags: { $size: 3 } }), true);
    assert.equal(matchFilter(doc, { tags: { $size: 2 } }), false);
  });

  it('$all matches when array contains all elements', () => {
    assert.equal(matchFilter(doc, { tags: { $all: ['a', 'b'] } }), true);
    assert.equal(matchFilter(doc, { tags: { $all: ['a', 'x'] } }), false);
  });

  it('$elemMatch matches object elements', () => {
    const items = { _id: '1', orders: [{ amount: 10 }, { amount: 50 }] };
    assert.equal(matchFilter(items, { orders: { $elemMatch: { amount: { $gt: 40 } } } }), true);
    assert.equal(matchFilter(items, { orders: { $elemMatch: { amount: { $gt: 100 } } } }), false);
  });
});

describe('matchFilter — logical operators', () => {
  const doc = { _id: '1', age: 30, active: true };

  it('$and requires all', () => {
    assert.equal(matchFilter(doc, { $and: [{ age: 30 }, { active: true }] }), true);
    assert.equal(matchFilter(doc, { $and: [{ age: 30 }, { active: false }] }), false);
  });

  it('$or requires any', () => {
    assert.equal(matchFilter(doc, { $or: [{ age: 99 }, { active: true }] }), true);
    assert.equal(matchFilter(doc, { $or: [{ age: 99 }, { active: false }] }), false);
  });

  it('$nor rejects all', () => {
    assert.equal(matchFilter(doc, { $nor: [{ age: 99 }, { active: false }] }), true);
    assert.equal(matchFilter(doc, { $nor: [{ age: 30 }, { active: false }] }), false);
  });

  it('$not negates', () => {
    assert.equal(matchFilter(doc, { $not: { age: 99 } }), true);
    assert.equal(matchFilter(doc, { $not: { age: 30 } }), false);
  });
});

// ── getFieldValue ────────────────────────────────────────────

describe('getFieldValue', () => {
  it('reads top-level field', () => {
    assert.equal(getFieldValue({ name: 'Alice' }, 'name'), 'Alice');
  });

  it('reads nested field with dot notation', () => {
    const doc = { user: { address: { city: 'Jakarta' } } };
    assert.equal(getFieldValue(doc, 'user.address.city'), 'Jakarta');
  });

  it('returns undefined for missing field', () => {
    assert.equal(getFieldValue({ a: 1 }, 'b'), undefined);
  });

  it('returns undefined for missing nested field', () => {
    assert.equal(getFieldValue({ a: { b: 1 } }, 'a.c'), undefined);
  });
});

// ── setNestedField / getNestedField / deleteNestedField ──────

describe('setNestedField', () => {
  it('sets top-level field', () => {
    const obj: Record<string, unknown> = {};
    setNestedField(obj, 'name', 'Alice');
    assert.equal(obj['name'], 'Alice');
  });

  it('sets nested field', () => {
    const obj: Record<string, unknown> = {};
    setNestedField(obj, 'a.b.c', 42);
    assert.equal((obj['a'] as any)['b']['c'], 42);
  });
});

describe('deleteNestedField', () => {
  it('deletes top-level field', () => {
    const obj: Record<string, unknown> = { a: 1, b: 2 };
    deleteNestedField(obj, 'a');
    assert.equal(obj['a'], undefined);
    assert.equal(obj['b'], 2);
  });

  it('deletes nested field', () => {
    const obj: Record<string, unknown> = { a: { b: { c: 42 } } };
    deleteNestedField(obj, 'a.b.c');
    assert.equal((obj['a'] as any)['b']['c'], undefined);
  });
});

// ── applyUpdate ──────────────────────────────────────────────

describe('applyUpdate', () => {
  it('$set sets fields', () => {
    const result = applyUpdate({ _id: '1', name: 'Alice' }, { $set: { name: 'Bob' } });
    assert.equal(result['name'], 'Bob');
  });

  it('$unset removes fields', () => {
    const result = applyUpdate({ _id: '1', name: 'Alice', age: 30 }, { $unset: { age: 1 } });
    assert.equal(result['age'], undefined);
  });

  it('$inc increments number', () => {
    const result = applyUpdate({ _id: '1', points: 10 }, { $inc: { points: 5 } });
    assert.equal(result['points'], 15);
  });

  it('$inc creates field if missing', () => {
    const result = applyUpdate({ _id: '1' }, { $inc: { points: 5 } });
    assert.equal(result['points'], 5);
  });

  it('$mul multiplies', () => {
    const result = applyUpdate({ _id: '1', val: 10 }, { $mul: { val: 3 } });
    assert.equal(result['val'], 30);
  });

  it('$push appends to array', () => {
    const result = applyUpdate({ _id: '1', tags: ['a'] }, { $push: { tags: 'b' } });
    assert.deepEqual(result['tags'], ['a', 'b']);
  });

  it('$push creates array if missing', () => {
    const result = applyUpdate({ _id: '1' }, { $push: { tags: 'a' } });
    assert.deepEqual(result['tags'], ['a']);
  });

  it('$push with $each', () => {
    const result = applyUpdate({ _id: '1', tags: ['a'] }, { $push: { tags: { $each: ['b', 'c'] } } });
    assert.deepEqual(result['tags'], ['a', 'b', 'c']);
  });

  it('$pull removes matching elements', () => {
    const result = applyUpdate({ _id: '1', tags: ['a', 'b', 'c'] }, { $pull: { tags: 'b' } });
    assert.deepEqual(result['tags'], ['a', 'c']);
  });

  it('$addToSet adds only if not present', () => {
    const result = applyUpdate({ _id: '1', tags: ['a'] }, { $addToSet: { tags: 'a' } });
    assert.deepEqual(result['tags'], ['a']);
    const result2 = applyUpdate({ _id: '1', tags: ['a'] }, { $addToSet: { tags: 'b' } });
    assert.deepEqual(result2['tags'], ['a', 'b']);
  });

  it('$rename renames field', () => {
    const result = applyUpdate({ _id: '1', name: 'Alice' }, { $rename: { name: 'fullName' } });
    assert.equal(result['fullName'], 'Alice');
    assert.equal(result['name'], undefined);
  });

  it('$min sets lower value', () => {
    const result = applyUpdate({ _id: '1', score: 10 }, { $min: { score: 5 } });
    assert.equal(result['score'], 5);
    const result2 = applyUpdate({ _id: '1', score: 10 }, { $min: { score: 15 } });
    assert.equal(result2['score'], 10);
  });

  it('$max sets higher value', () => {
    const result = applyUpdate({ _id: '1', score: 10 }, { $max: { score: 15 } });
    assert.equal(result['score'], 15);
    const result2 = applyUpdate({ _id: '1', score: 10 }, { $max: { score: 5 } });
    assert.equal(result2['score'], 10);
  });

  it('does not mutate original document', () => {
    const original = { _id: '1', name: 'Alice' };
    const result = applyUpdate(original, { $set: { name: 'Bob' } });
    assert.equal(original['name'], 'Alice');
    assert.equal(result['name'], 'Bob');
  });
});

// ── applyProjection ──────────────────────────────────────────

describe('applyProjection', () => {
  const doc = { _id: '1', name: 'Alice', age: 30, city: 'Jakarta' };

  it('includes specified fields', () => {
    const result = applyProjection(doc, { name: 1, age: 1 });
    assert.deepEqual(result, { _id: '1', name: 'Alice', age: 30 });
  });

  it('excludes specified fields', () => {
    const result = applyProjection(doc, { city: 0 });
    assert.deepEqual(result, { _id: '1', name: 'Alice', age: 30 });
  });

  it('can exclude _id', () => {
    const result = applyProjection(doc, { _id: 0, name: 1 });
    assert.deepEqual(result, { name: 'Alice' });
  });

  it('returns original doc when no projection', () => {
    const result = applyProjection(doc);
    assert.deepEqual(result, doc);
  });
});
