import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { crc32, writeCrc, readCrc } from '../../src/utils/crc32.js';

describe('crc32', () => {
  it('produces deterministic output for known input', () => {
    const buf = Buffer.from('hello world');
    const a = crc32(buf);
    const b = crc32(buf);
    assert.equal(a, b);
    assert.equal(typeof a, 'number');
    assert.ok(a > 0);
  });

  it('produces different output for different inputs', () => {
    const a = crc32(Buffer.from('hello'));
    const b = crc32(Buffer.from('world'));
    assert.notEqual(a, b);
  });

  it('handles empty buffer', () => {
    const result = crc32(Buffer.alloc(0));
    assert.equal(typeof result, 'number');
    assert.equal(result, 0); // CRC32 of empty input = 0x00000000
  });

  it('handles large buffer', () => {
    const buf = Buffer.alloc(100_000, 0xAB);
    const result = crc32(buf);
    assert.equal(typeof result, 'number');
    assert.ok(result >= 0);
  });

  it('handles single byte', () => {
    const result = crc32(Buffer.from([0x42]));
    assert.equal(typeof result, 'number');
    assert.ok(result > 0);
  });
});

describe('writeCrc / readCrc round-trip', () => {
  it('writes and reads back the same CRC value', () => {
    const buf = Buffer.alloc(8);
    const value = crc32(Buffer.from('test'));
    writeCrc(buf, 2, value);
    const read = readCrc(buf, 2);
    assert.equal(read, value);
  });

  it('writes at offset 0', () => {
    const buf = Buffer.alloc(4);
    writeCrc(buf, 0, 0xDEADBEEF);
    assert.equal(readCrc(buf, 0), 0xDEADBEEF);
  });

  it('does not corrupt adjacent bytes', () => {
    const buf = Buffer.alloc(12, 0xFF);
    writeCrc(buf, 4, 0x12345678);
    // Bytes before and after should be untouched
    assert.equal(buf[0], 0xFF);
    assert.equal(buf[3], 0xFF);
    assert.equal(buf[8], 0xFF);
    assert.equal(buf[11], 0xFF);
    assert.equal(readCrc(buf, 4), 0x12345678);
  });
});
