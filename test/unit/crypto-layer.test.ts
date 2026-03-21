// ============================================================
//  Unit Test — CryptoLayer
//  Test: encrypt/decrypt round-trip, IV uniqueness, key mismatch error,
//        PBKDF2 key derivation, overhead constant
// ============================================================

import assert      from 'node:assert/strict';
import { test, describe, before } from 'node:test';
import os          from 'node:os';
import path        from 'node:path';
import fsp         from 'node:fs/promises';
import { CryptoLayer, cryptoFromPassphrase, CRYPTO_OVERHEAD, IV_SIZE, TAG_SIZE } from '../../src/crypto/crypto-layer.js';

describe('CryptoLayer.fromKey', () => {

  const key = Buffer.alloc(32, 0xAB); // 32 bytes = 256 bit

  test('CRYPTO_OVERHEAD = IV_SIZE + TAG_SIZE', () => {
    assert.equal(CRYPTO_OVERHEAD, IV_SIZE + TAG_SIZE);
    assert.equal(CRYPTO_OVERHEAD, 28);
  });

  test('encrypt/decrypt round-trip', () => {
    const crypto    = CryptoLayer.fromKey(key);
    const plaintext = Buffer.from('{"_id":"u1","name":"Budi","points":100}');
    const ct        = crypto.encrypt(plaintext);
    const dt        = crypto.decrypt(ct);
    assert.deepEqual(dt, plaintext);
  });

  test('ciphertext lebih panjang dari plaintext (overhead)', () => {
    const crypto = CryptoLayer.fromKey(key);
    const pt     = Buffer.from('hello');
    const ct     = crypto.encrypt(pt);
    assert.equal(ct.length, pt.length + CRYPTO_OVERHEAD);
  });

  test('IV unik per enkripsi — ciphertext berbeda untuk plaintext yang sama', () => {
    const crypto = CryptoLayer.fromKey(key);
    const pt     = Buffer.from('sama persis');
    const ct1    = crypto.encrypt(pt);
    const ct2    = crypto.encrypt(pt);
    assert.notDeepEqual(ct1, ct2); // IV berbeda → ciphertext berbeda
    // Tapi keduanya bisa didekripsi ke plaintext yang sama
    assert.deepEqual(crypto.decrypt(ct1), pt);
    assert.deepEqual(crypto.decrypt(ct2), pt);
  });

  test('key salah → DecryptError', () => {
    const crypto1 = CryptoLayer.fromKey(Buffer.alloc(32, 0xAA));
    const crypto2 = CryptoLayer.fromKey(Buffer.alloc(32, 0xBB));
    const ct      = crypto1.encrypt(Buffer.from('secret'));
    assert.throws(() => crypto2.decrypt(ct), /Dekripsi gagal/);
  });

  test('ciphertext corrupt → DecryptError', () => {
    const crypto = CryptoLayer.fromKey(key);
    const ct     = crypto.encrypt(Buffer.from('data'));
    ct[IV_SIZE + 5] ^= 0xFF; // corrupt auth tag
    assert.throws(() => crypto.decrypt(ct), /Dekripsi gagal/);
  });

  test('ciphertext terlalu pendek → error', () => {
    const crypto = CryptoLayer.fromKey(key);
    assert.throws(() => crypto.decrypt(Buffer.alloc(10)), /terlalu pendek/);
  });

  test('key bukan 32 bytes → error', () => {
    assert.throws(() => CryptoLayer.fromKey(Buffer.alloc(16)), /32 bytes/);
  });

  test('buffer besar (64KB) berhasil encrypt/decrypt', () => {
    const crypto = CryptoLayer.fromKey(key);
    const big    = Buffer.alloc(64 * 1024, 0x42);
    const ct     = crypto.encrypt(big);
    const dt     = crypto.decrypt(ct);
    assert.deepEqual(dt, big);
  });
});

describe('cryptoFromPassphrase', () => {

  let tmpDir: string;

  before(async () => {
    tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'ovndb-test-'));
  });

  test('buat key dari passphrase dan bisa encrypt/decrypt', async () => {
    const crypto = await cryptoFromPassphrase('secret-passphrase-123', tmpDir);
    const pt     = Buffer.from('test data');
    const ct     = crypto.encrypt(pt);
    const dt     = crypto.decrypt(ct);
    assert.deepEqual(dt, pt);
  });

  test('salt file dibuat di tmpDir', async () => {
    await cryptoFromPassphrase('another-pass', tmpDir);
    const saltPath = path.join(tmpDir, '.salt');
    const stat     = await fsp.stat(saltPath);
    assert.ok(stat.isFile());
    assert.equal(stat.size, 32); // SALT_LEN = 32
  });

  test('dua call dengan passphrase sama → key sama', async () => {
    const c1 = await cryptoFromPassphrase('consistent-pass', tmpDir);
    const c2 = await cryptoFromPassphrase('consistent-pass', tmpDir);
    const pt  = Buffer.from('data');
    const ct  = c1.encrypt(pt);
    // c2 punya key yang sama (salt sama) → bisa decrypt
    assert.doesNotThrow(() => c2.decrypt(ct));
  });
});
