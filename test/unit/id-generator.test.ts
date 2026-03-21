// ============================================================
//  Unit Test — ID Generator
//  Test: format, uniqueness, sortability, timestamp extraction
// ============================================================

import assert from 'node:assert/strict';
import { test, describe } from 'node:test';
import { generateId, idToTimestamp, isValidId } from '../../src/utils/id-generator.js';

describe('generateId', () => {

  test('menghasilkan string 24 karakter hex', () => {
    const id = generateId();
    assert.equal(typeof id, 'string');
    assert.equal(id.length, 24);
    assert.ok(/^[0-9a-f]{24}$/.test(id), 'harus lowercase hex');
  });

  test('setiap call menghasilkan ID berbeda', () => {
    const ids = new Set(Array.from({ length: 1000 }, () => generateId()));
    assert.equal(ids.size, 1000, 'tidak boleh ada duplikat dalam 1000 call');
  });

  test('ID baru selalu >= ID lama (sortable/monotonic)', () => {
    const ids: string[] = [];
    for (let i = 0; i < 200; i++) ids.push(generateId());
    // Verifikasi: setiap ID >= ID sebelumnya (monotonically non-decreasing)
    for (let i = 1; i < ids.length; i++) {
      assert.ok(ids[i]! >= ids[i - 1]!,
        `ids[${i}]=${ids[i]} harus >= ids[${i-1}]=${ids[i-1]}`);
    }
  });

  test('ID mengandung timestamp saat ini (±2 detik)', () => {
    const before = Date.now();
    const id     = generateId();
    const after  = Date.now();
    const ts     = idToTimestamp(id);
    assert.ok(ts >= before - 2000 && ts <= after + 2000,
      `timestamp ${ts} harus berada antara ${before} dan ${after}`);
  });
});

describe('idToTimestamp', () => {

  test('ekstrak timestamp dari ID dengan benar', () => {
    const before = Date.now();
    const id     = generateId();
    const after  = Date.now();
    const ts     = idToTimestamp(id);
    assert.ok(ts >= before && ts <= after + 10);
  });
});

describe('isValidId', () => {

  test('ID valid dari generateId()', () => {
    assert.ok(isValidId(generateId()));
  });

  test('24 karakter hex → valid', () => {
    assert.ok(isValidId('0123456789abcdef01234567'));
  });

  test('terlalu pendek → tidak valid', () => {
    assert.ok(!isValidId('abc'));
  });

  test('terlalu panjang → tidak valid', () => {
    assert.ok(!isValidId('0123456789abcdef012345678'));
  });

  test('karakter non-hex → tidak valid', () => {
    assert.ok(!isValidId('0123456789ABCDEF01234567')); // uppercase
    assert.ok(!isValidId('0123456789abcdef0123456g')); // 'g' bukan hex
  });

  test('string kosong → tidak valid', () => {
    assert.ok(!isValidId(''));
  });

  test('bukan string → tidak valid', () => {
    assert.ok(!isValidId(null as unknown as string));
    assert.ok(!isValidId(undefined as unknown as string));
    assert.ok(!isValidId(123 as unknown as string));
  });
});
