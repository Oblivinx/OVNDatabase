// ============================================================
//  Unit Test — CRC32
//  Test: deterministic, perubahan kecil mengubah checksum
// ============================================================

import assert from 'node:assert/strict';
import { test, describe } from 'node:test';
import { crc32, writeCrc, readCrc } from '../../src/utils/crc32.js';

describe('crc32', () => {
  test('menghasilkan angka', () => {
    const result = crc32(Buffer.from('hello'));
    assert.equal(typeof result, 'number');
  });

  test('deterministik: input sama → output sama', () => {
    const a = crc32(Buffer.from('OvnDB v2.0'));
    const b = crc32(Buffer.from('OvnDB v2.0'));
    assert.equal(a, b);
  });

  test('sensitif terhadap perubahan kecil', () => {
    const a = crc32(Buffer.from('hello'));
    const b = crc32(Buffer.from('hellO'));
    assert.notEqual(a, b);
  });

  test('buffer kosong tidak crash', () => {
    assert.doesNotThrow(() => crc32(Buffer.alloc(0)));
  });

  test('nilai yang dikenal (CRC32 dari "123456789")', () => {
    // Known CRC32: 0xCBF43926
    const result = crc32(Buffer.from('123456789'));
    assert.equal(result, 0xCBF43926);
  });
});

describe('writeCrc / readCrc', () => {
  test('write dan read kembali nilai yang sama', () => {
    const buf = Buffer.alloc(8);
    const val = 0xDEADBEEF;
    writeCrc(buf, 4, val);
    assert.equal(readCrc(buf, 4), val);
  });

  test('tidak mengubah byte sebelum offset', () => {
    const buf = Buffer.alloc(8);
    buf.writeUInt32LE(0xABCDEF12, 0);
    writeCrc(buf, 4, 0x12345678);
    assert.equal(buf.readUInt32LE(0), 0xABCDEF12); // tidak berubah
  });
});
