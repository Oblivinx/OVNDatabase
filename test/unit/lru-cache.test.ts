// ============================================================
//  Unit Test — LRU Cache
//  Test: get/set/delete, eviction policy, hit-rate tracking
// ============================================================

import assert from 'node:assert/strict';
import { test, describe, beforeEach } from 'node:test';
import { LRUCache } from '../../src/core/cache/lru-cache.js';

describe('LRUCache', () => {

  let cache: LRUCache<string, number>;

  beforeEach(() => {
    cache = new LRUCache<string, number>(3); // capacity = 3
  });

  // ── Basic ops ─────────────────────────────────────────────

  test('set dan get nilai', () => {
    cache.set('a', 1);
    assert.equal(cache.get('a'), 1);
  });

  test('get key yang tidak ada → undefined', () => {
    assert.equal(cache.get('nonexistent'), undefined);
  });

  test('overwrite nilai yang sudah ada', () => {
    cache.set('a', 1);
    cache.set('a', 99);
    assert.equal(cache.get('a'), 99);
    assert.equal(cache.size, 1); // tidak ada duplikat
  });

  test('delete key', () => {
    cache.set('a', 1);
    const result = cache.delete('a');
    assert.equal(result, true);
    assert.equal(cache.get('a'), undefined);
    assert.equal(cache.size, 0);
  });

  test('delete key yang tidak ada → false', () => {
    assert.equal(cache.delete('missing'), false);
  });

  // ── Eviction (LRU policy) ─────────────────────────────────

  test('evict entry paling lama tidak dipakai (LRU) saat penuh', () => {
    cache.set('a', 1); // [a]
    cache.set('b', 2); // [b, a]
    cache.set('c', 3); // [c, b, a]
    // Akses 'a' → 'a' jadi paling baru: [a, c, b]
    cache.get('a');
    // Insert 'd' → capacity penuh → evict 'b' (LRU)
    cache.set('d', 4); // [d, a, c]
    assert.equal(cache.get('b'), undefined, "'b' seharusnya di-evict");
    assert.equal(cache.get('a'), 1, "'a' seharusnya masih ada");
    assert.equal(cache.get('c'), 3, "'c' seharusnya masih ada");
    assert.equal(cache.get('d'), 4, "'d' seharusnya ada");
  });

  test('insert ke capacity tidak evict apapun', () => {
    cache.set('x', 10);
    cache.set('y', 20);
    cache.set('z', 30);
    assert.equal(cache.size, 3);
    assert.equal(cache.get('x'), 10);
    assert.equal(cache.get('y'), 20);
    assert.equal(cache.get('z'), 30);
  });

  // ── Hit rate ──────────────────────────────────────────────

  test('hit rate 100% jika semua get berhasil', () => {
    cache.set('k', 42);
    cache.get('k');
    cache.get('k');
    cache.get('k');
    assert.equal(cache.hitRate, 1.0);
  });

  test('hit rate 0% jika semua get miss', () => {
    cache.get('miss1');
    cache.get('miss2');
    cache.get('miss3');
    assert.equal(cache.hitRate, 0.0);
  });

  test('hit rate campuran 50%', () => {
    cache.set('k', 1);
    cache.get('k');    // hit
    cache.get('miss'); // miss
    assert.ok(Math.abs(cache.hitRate - 0.5) < 0.01);
  });

  // ── Clear ─────────────────────────────────────────────────

  test('clear menghapus semua entry dan reset stats', () => {
    cache.set('a', 1);
    cache.set('b', 2);
    cache.get('a'); // hit
    cache.clear();
    assert.equal(cache.size, 0);
    assert.equal(cache.hitRate, 1.0); // reset ke default
    assert.equal(cache.get('a'), undefined);
  });

  // ── Entries iterator ──────────────────────────────────────

  test('entries() mengiterasi dari MRU ke LRU', () => {
    cache.set('a', 1);
    cache.set('b', 2);
    cache.set('c', 3);
    // Urutan MRU: c, b, a
    const keys = [...cache.entries()].map(([k]) => k);
    assert.deepEqual(keys, ['c', 'b', 'a']);
  });

  // ── Edge cases ────────────────────────────────────────────

  test('capacity = 1: setiap insert baru evict yang lama', () => {
    const tiny = new LRUCache<string, number>(1);
    tiny.set('a', 1);
    tiny.set('b', 2);
    assert.equal(tiny.get('a'), undefined);
    assert.equal(tiny.get('b'), 2);
  });

  test('throw jika capacity < 1', () => {
    assert.throws(() => new LRUCache(0), /capacity must be/);
    assert.throws(() => new LRUCache(-1), /capacity must be/);
  });

  test('has() benar', () => {
    cache.set('x', 100);
    assert.equal(cache.has('x'), true);
    assert.equal(cache.has('y'), false);
  });
});
