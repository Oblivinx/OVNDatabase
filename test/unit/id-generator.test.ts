import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import fsp from 'fs/promises';
import { generateId, idToTimestamp, isValidId } from '../../src/utils/id-generator.js';

describe('generateId', () => {
  it('generates 24-hex-char IDs', () => {
    const id = generateId();
    assert.equal(id.length, 24);
    assert.match(id, /^[0-9a-f]{24}$/);
  });

  it('generates unique IDs (10K)', () => {
    const ids = new Set<string>();
    for (let i = 0; i < 10_000; i++) ids.add(generateId());
    assert.equal(ids.size, 10_000);
  });

  it('generates monotonically non-decreasing IDs', () => {
    const a = generateId();
    const b = generateId();
    assert.ok(b >= a, `Expected ${b} >= ${a}`);
  });

  it('multiple rapid calls remain ordered', () => {
    const ids: string[] = [];
    for (let i = 0; i < 100; i++) ids.push(generateId());
    for (let i = 1; i < ids.length; i++) {
      assert.ok(ids[i]! >= ids[i - 1]!, `IDs not ordered at index ${i}`);
    }
  });
});

describe('idToTimestamp', () => {
  it('extracts approximate timestamp from ID', () => {
    const before = Date.now();
    const id = generateId();
    const after = Date.now();
    const ts = idToTimestamp(id);
    assert.ok(ts >= before - 1, `timestamp ${ts} < before ${before}`);
    assert.ok(ts <= after + 1, `timestamp ${ts} > after ${after}`);
  });

  it('extracts consistent timestamp for same ID', () => {
    const id = generateId();
    assert.equal(idToTimestamp(id), idToTimestamp(id));
  });
});

describe('isValidId', () => {
  it('accepts generated IDs', () => {
    assert.equal(isValidId(generateId()), true);
  });

  it('rejects short IDs', () => {
    assert.equal(isValidId('abc'), false);
  });

  it('rejects uppercase IDs', () => {
    assert.equal(isValidId('A'.repeat(24)), false);
  });

  it('rejects non-hex IDs', () => {
    assert.equal(isValidId('x'.repeat(24)), false);
  });

  it('rejects empty string', () => {
    assert.equal(isValidId(''), false);
  });

  it('rejects non-string', () => {
    assert.equal(isValidId(123 as unknown as string), false);
  });
});

describe('id-generator security', () => {
  it('uses crypto.randomBytes for PROCESS_RAND', async () => {
    const src = await fsp.readFile(
      new URL('../../src/utils/id-generator.ts', import.meta.url),
      'utf8'
    );
    // The PROCESS_RAND line must use crypto.randomBytes, not Math.random
    const processRandLine = src.split('\n').find(l => l.includes('PROCESS_RAND') && l.includes('=') && !l.startsWith('//'));
    assert.ok(processRandLine, 'PROCESS_RAND assignment line must exist');
    assert.ok(processRandLine!.includes('crypto.randomBytes'), 'PROCESS_RAND must use crypto.randomBytes');
    assert.ok(!processRandLine!.includes('Math.random'), 'PROCESS_RAND must not use Math.random');
  });
});
