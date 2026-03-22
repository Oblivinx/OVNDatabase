import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LRUCache } from '../../src/core/cache/lru-cache.js';

describe('LRUCache', () => {
  // ── Basic operations ──────────────────────────────────────

  it('set and get', () => {
    const cache = new LRUCache<string, Buffer>(1024 * 1024);
    cache.set('a', Buffer.from('hello'));
    assert.deepEqual(cache.get('a'), Buffer.from('hello'));
  });

  it('returns undefined for missing key', () => {
    const cache = new LRUCache<string, Buffer>(1024 * 1024);
    assert.equal(cache.get('missing'), undefined);
  });

  it('delete removes entry', () => {
    const cache = new LRUCache<string, Buffer>(1024 * 1024);
    cache.set('a', Buffer.from('hello'));
    assert.equal(cache.delete('a'), true);
    assert.equal(cache.get('a'), undefined);
    assert.equal(cache.size, 0);
  });

  it('delete returns false for missing key', () => {
    const cache = new LRUCache<string, Buffer>(1024 * 1024);
    assert.equal(cache.delete('missing'), false);
  });

  it('has returns correct boolean', () => {
    const cache = new LRUCache<string, Buffer>(1024 * 1024);
    cache.set('a', Buffer.from('x'));
    assert.equal(cache.has('a'), true);
    assert.equal(cache.has('b'), false);
  });

  it('clear removes all entries', () => {
    const cache = new LRUCache<string, Buffer>(1024 * 1024);
    cache.set('a', Buffer.from('x'));
    cache.set('b', Buffer.from('y'));
    cache.clear();
    assert.equal(cache.size, 0);
    assert.equal(cache.bytes, 0);
    assert.equal(cache.get('a'), undefined);
  });

  it('overwriting same key updates value', () => {
    const cache = new LRUCache<string, Buffer>(1024 * 1024);
    cache.set('a', Buffer.from('old'));
    cache.set('a', Buffer.from('new'));
    assert.deepEqual(cache.get('a'), Buffer.from('new'));
    assert.equal(cache.size, 1);
  });

  // ── Byte-based eviction ───────────────────────────────────

  it('evicts LRU when bytes exceed limit', () => {
    // Max 100_000 bytes (> 10000 so it's treated as literal bytes)
    const cache = new LRUCache<string, Buffer>(100_000);
    cache.set('a', Buffer.alloc(40_000, 0x01)); // 40K bytes
    cache.set('b', Buffer.alloc(40_000, 0x02)); // 40K bytes → 80K total
    cache.set('c', Buffer.alloc(40_000, 0x03)); // 40K bytes → would be 120K → evict 'a'
    assert.equal(cache.has('a'), false, 'a should be evicted');
    assert.equal(cache.has('b'), true);
    assert.equal(cache.has('c'), true);
  });

  it('promotes recently accessed items', () => {
    const cache = new LRUCache<string, Buffer>(100_000);
    cache.set('a', Buffer.alloc(40_000));
    cache.set('b', Buffer.alloc(40_000));
    cache.get('a'); // promote 'a' → 'b' is now LRU
    cache.set('c', Buffer.alloc(40_000)); // evict 'b'
    assert.equal(cache.has('a'), true, 'a was promoted');
    assert.equal(cache.has('b'), false, 'b was LRU');
    assert.equal(cache.has('c'), true);
  });

  it('tracks bytes correctly', () => {
    const cache = new LRUCache<string, Buffer>(1024 * 1024);
    const buf1 = Buffer.alloc(100);
    const buf2 = Buffer.alloc(200);
    cache.set('a', buf1);
    assert.equal(cache.bytes, 100);
    cache.set('b', buf2);
    assert.equal(cache.bytes, 300);
    cache.delete('a');
    assert.equal(cache.bytes, 200);
    cache.clear();
    assert.equal(cache.bytes, 0);
  });

  // ── Backward compatibility ────────────────────────────────

  it('small maxBytes treated as entry count × 4096', () => {
    // maxBytes <= 10000 → treated as entry count × 4096
    const cache = new LRUCache<string, Buffer>(100); // 100 * 4096 = 409_600
    assert.equal(cache.maxBytes, 100 * 4096);
  });

  it('large maxBytes treated as literal bytes', () => {
    const cache = new LRUCache<string, Buffer>(256 * 1024 * 1024);
    assert.equal(cache.maxBytes, 256 * 1024 * 1024);
  });

  // ── Hit rate ──────────────────────────────────────────────

  it('tracks hit rate', () => {
    const cache = new LRUCache<string, Buffer>(1024 * 1024);
    cache.set('a', Buffer.from('x'));
    cache.get('a');  // hit
    cache.get('b');  // miss
    assert.equal(cache.hitRate, 0.5);
  });

  it('hitRate is 1 when no ops', () => {
    const cache = new LRUCache<string, Buffer>(1024 * 1024);
    assert.equal(cache.hitRate, 1);
  });

  // ── Entries iterator ──────────────────────────────────────

  it('iterates entries in MRU order', () => {
    const cache = new LRUCache<string, Buffer>(1024 * 1024);
    cache.set('a', Buffer.from('1'));
    cache.set('b', Buffer.from('2'));
    cache.set('c', Buffer.from('3'));
    const keys = [...cache.entries()].map(([k]) => k);
    assert.deepEqual(keys, ['c', 'b', 'a']); // MRU first
  });
});
