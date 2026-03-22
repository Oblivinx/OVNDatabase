import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import crypto from 'node:crypto';
import { CryptoLayer, FieldCrypto, CRYPTO_OVERHEAD, CRYPTO_OVERHEAD_V2 } from '../../src/crypto/crypto-layer.js';

describe('CryptoLayer', () => {
  const key = crypto.randomBytes(32);

  // ── Factory ───────────────────────────────────────────────

  it('fromKey accepts valid 32-byte key', () => {
    const layer = CryptoLayer.fromKey(key, 0);
    assert.ok(layer);
  });

  it('fromKey rejects wrong-length key', () => {
    assert.throws(() => CryptoLayer.fromKey(Buffer.alloc(16), 0), /32 bytes/);
  });

  it('fromKey rejects invalid keyVersion', () => {
    assert.throws(() => CryptoLayer.fromKey(key, 300), /keyVersion/);
  });

  it('fromKey accepts keyVersion 0 and 255', () => {
    assert.ok(CryptoLayer.fromKey(key, 0));
    assert.ok(CryptoLayer.fromKey(key, 255));
  });

  // ── Encrypt / Decrypt ─────────────────────────────────────

  it('encrypt → decrypt round-trip', () => {
    const layer = CryptoLayer.fromKey(key, 1);
    const plain = Buffer.from('{"_id":"abc","name":"Alice"}');
    const ct = layer.encrypt(plain);
    const pt = layer.decrypt(ct);
    assert.deepEqual(pt, plain);
  });

  it('encrypted output is larger than input (overhead)', () => {
    const layer = CryptoLayer.fromKey(key, 0);
    const plain = Buffer.from('hello');
    const ct = layer.encrypt(plain);
    assert.ok(ct.length >= plain.length + CRYPTO_OVERHEAD);
  });

  it('each encryption produces different ciphertext (random IV)', () => {
    const layer = CryptoLayer.fromKey(key, 0);
    const plain = Buffer.from('same content');
    const ct1 = layer.encrypt(plain);
    const ct2 = layer.encrypt(plain);
    assert.notDeepEqual(ct1, ct2);
  });

  it('rejects tampered ciphertext', () => {
    const layer = CryptoLayer.fromKey(key, 1);
    const ct = layer.encrypt(Buffer.from('secret'));
    ct[ct.length - 1]! ^= 0xFF;
    assert.throws(() => layer.decrypt(ct), /corrupt|tampered|key salah/);
  });

  it('rejects ciphertext too short', () => {
    const layer = CryptoLayer.fromKey(key, 0);
    assert.throws(() => layer.decrypt(Buffer.alloc(10)), /terlalu pendek/);
  });

  it('decrypts with wrong key → throws', () => {
    const layer1 = CryptoLayer.fromKey(key, 0);
    const layer2 = CryptoLayer.fromKey(crypto.randomBytes(32), 0);
    const ct = layer1.encrypt(Buffer.from('hello'));
    assert.throws(() => layer2.decrypt(ct));
  });

  // ── v2/v3 format compat ───────────────────────────────────

  it('v3 format includes keyVersion byte', () => {
    const layer = CryptoLayer.fromKey(key, 42);
    const ct = layer.encrypt(Buffer.from('test'));
    assert.equal(ct[0], 42);
  });

  it('v2 ciphertext decrypts via fallback', () => {
    // Simulate v2: [12 IV][16 Tag][N CT]
    const layer = CryptoLayer.fromKey(key, 0);
    const plain = Buffer.from('hello world');
    const iv = crypto.randomBytes(12);
    const cipher = crypto.createCipheriv('aes-256-gcm', key, iv);
    const ct = Buffer.concat([cipher.update(plain), cipher.final()]);
    const tag = cipher.getAuthTag();
    const v2Ciphertext = Buffer.concat([iv, tag, ct]);
    const decrypted = layer.decrypt(v2Ciphertext);
    assert.deepEqual(decrypted, plain);
  });

  // ── Utilities ─────────────────────────────────────────────

  it('verify returns true for valid ciphertext', () => {
    const layer = CryptoLayer.fromKey(key, 0);
    const ct = layer.encrypt(Buffer.from('test'));
    assert.equal(layer.verify(ct), true);
  });

  it('verify returns false for tampered ciphertext', () => {
    const layer = CryptoLayer.fromKey(key, 0);
    const ct = layer.encrypt(Buffer.from('test'));
    ct[ct.length - 1]! ^= 0xFF;
    assert.equal(layer.verify(ct), false);
  });

  it('reencrypt works', () => {
    const layer1 = CryptoLayer.fromKey(key, 0);
    const layer2 = CryptoLayer.fromKey(crypto.randomBytes(32), 1);
    const ct1 = layer1.encrypt(Buffer.from('secret'));
    const ct2 = layer1.reencrypt(ct1, layer2);
    const pt = layer2.decrypt(ct2);
    assert.deepEqual(pt, Buffer.from('secret'));
  });

  it('version getter returns keyVersion', () => {
    const layer = CryptoLayer.fromKey(key, 5);
    assert.equal(layer.version, 5);
  });

  it('isEncryptedBuffer detects encrypted data', () => {
    const layer = CryptoLayer.fromKey(key, 0);
    const ct = layer.encrypt(Buffer.from('{"_id":"1"}'));
    assert.equal(CryptoLayer.isEncryptedBuffer(ct), true);
    assert.equal(CryptoLayer.isEncryptedBuffer(Buffer.from('{"_id":"1"}')), false);
  });
});

describe('FieldCrypto', () => {
  const key = crypto.randomBytes(32);
  const layer = CryptoLayer.fromKey(key, 0);
  const fc = new FieldCrypto(layer);

  it('encrypts and decrypts specific fields', () => {
    const doc = { _id: '1', name: 'Alice', ssn: '123-45-6789', age: 30 };
    const encrypted = fc.encryptFields(doc, ['ssn']);
    assert.notEqual(encrypted['ssn'], '123-45-6789'); // should be base64 ciphertext
    assert.equal(encrypted['name'], 'Alice'); // unencrypted field stays
    assert.equal(encrypted['age'], 30);

    const decrypted = fc.decryptFields(encrypted);
    assert.equal(decrypted['ssn'], '123-45-6789');
  });

  it('isEncryptedField returns correct boolean', () => {
    const doc = { _id: '1', name: 'Alice', ssn: '123' };
    const encrypted = fc.encryptFields(doc, ['ssn']);
    assert.equal(fc.isEncryptedField(encrypted, 'ssn'), true);
    assert.equal(fc.isEncryptedField(encrypted, 'name'), false);
  });

  it('handles multiple encrypted fields', () => {
    const doc = { _id: '1', name: 'Alice', ssn: '123', phone: '628123' };
    const encrypted = fc.encryptFields(doc, ['ssn', 'phone']);
    const decrypted = fc.decryptFields(encrypted);
    assert.equal(decrypted['ssn'], '123');
    assert.equal(decrypted['phone'], '628123');
    assert.equal(decrypted['name'], 'Alice');
  });
});
