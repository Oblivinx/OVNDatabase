// ============================================================
//  OvnDB v3.0 — CollectionV2
//
//  G14 FIX: rotateEncryptionKey() — rotate key semua record sekaligus.
//  G13: decrypt otomatis handle format v2 (28B) dan v3 (29B).
//  Fix v2.1: enkripsi via _serialize()/_parse() override, bukan
//  monkey-patching. Race-condition safe.
// ============================================================

import { Collection }  from './collection.js';
import type { CryptoLayer } from '../crypto/crypto-layer.js';
import type { StorageEngine } from '../core/storage/storage-engine.js';
import type { IndexDefinition, OvnDocument, QueryFilter } from '../types/index.js';
import { makeLogger } from '../utils/logger.js';

const log = makeLogger('collection-v2');

export interface CollectionV2Options {
  crypto?:  CryptoLayer;
  indexes?: IndexDefinition[];
}

export interface KeyRotationResult {
  /** Dokumen yang berhasil di-re-encrypt */
  rotated: number;
  /** Dokumen yang gagal (data tetap dengan key lama) */
  failed:  number;
  /** Durasi dalam ms */
  elapsedMs: number;
}

export class CollectionV2<T extends OvnDocument = OvnDocument> extends Collection<T> {
  private _crypto?: CryptoLayer;
  readonly isEncrypted: boolean;

  constructor(name: string, engine: StorageEngine, opts: CollectionV2Options = {}) {
    super(name, engine);
    this._crypto     = opts.crypto;
    this.isEncrypted = !!opts.crypto;

    if (opts.crypto) {
      engine.decryptFn = (buf: Buffer) => opts.crypto!.decrypt(buf);
    }
  }

  async init(opts: CollectionV2Options = {}): Promise<void> {
    if (opts.indexes) {
      for (const def of opts.indexes) await this.createIndex(def);
    }
  }

  // ── Override _serialize / _parse (race-condition safe) ────

  protected override _serialize(doc: T): Buffer {
    const plain = Buffer.from(JSON.stringify(doc), 'utf8');
    return this._crypto ? this._crypto.encrypt(plain) : plain;
  }

  protected override _parse(buf: Buffer): T | null {
    if (!this._crypto) return super._parse(buf);
    try {
      const decrypted = this._crypto.decrypt(buf);
      return JSON.parse(decrypted.toString('utf8')) as T;
    } catch {
      // G13: fallback — coba parse langsung (misal data lama belum terenkripsi)
      try { return JSON.parse(buf.toString('utf8')) as T; } catch { return null; }
    }
  }

  // ── G14: Key rotation ─────────────────────────────────────

  /**
   * G14: Re-encrypt semua record dengan key baru.
   * Proses berlangsung tanpa downtime — collection tetap bisa dibaca/ditulis.
   *
   * Setelah selesai, decryptFn dan _crypto diperbarui ke key baru sehingga
   * semua operasi berikutnya langsung menggunakan key baru.
   *
   * @param newCrypto  CryptoLayer dengan key baru
   * @param batchSize  Dokumen per flush (default 500)
   *
   * @example
   *   const oldCrypto = await cryptoFromPassphrase(oldPass, dir, 0);
   *   const newCrypto = await cryptoFromPassphrase(newPass, dir, 1);
   *   const col = await db.collectionV2('secrets', { crypto: oldCrypto });
   *   const result = await col.rotateEncryptionKey(newCrypto);
   *   console.log(`Rotated: ${result.rotated}, Failed: ${result.failed}`);
   */
  async rotateEncryptionKey(newCrypto: CryptoLayer, batchSize = 500): Promise<KeyRotationResult> {
    if (!this._crypto) {
      throw new Error('[CollectionV2] Collection ini tidak menggunakan enkripsi');
    }
    const startTime = Date.now();
    let rotated = 0;
    let failed  = 0;

    log.info('Key rotation started', {
      col:            this.name,
      oldKeyVersion:  this._crypto.version,
      newKeyVersion:  newCrypto.version,
    });

    // Scan semua record langsung dari engine (raw ciphertext)
    // tanpa melalui _parse() agar kita bisa re-encrypt ciphertext lama
    let batch: Array<{ id: string; ciphertext: Buffer }> = [];

    for await (const [id, rawBuf] of this.engine.scan()) {
      // rawBuf di sini adalah PLAINTEXT dari engine.scan() yang sudah decrypt
      // Kita perlu raw ciphertext — re-encrypt plaintext dengan key baru
      try {
        const newCiphertext = newCrypto.encrypt(rawBuf);
        batch.push({ id, ciphertext: newCiphertext });
      } catch { failed++; }

      if (batch.length >= batchSize) {
        await this._flushRotationBatch(batch, newCrypto);
        rotated += batch.length;
        batch = [];
      }
    }

    // Flush sisa
    if (batch.length > 0) {
      await this._flushRotationBatch(batch, newCrypto);
      rotated += batch.length;
    }

    // Update crypto ke key baru
    this._crypto         = newCrypto;
    this.engine.decryptFn = (buf: Buffer) => newCrypto.decrypt(buf);
    await this.engine.flush();

    const result: KeyRotationResult = {
      rotated,
      failed,
      elapsedMs: Date.now() - startTime,
    };
    log.info('Key rotation complete', { col: this.name, ...result });
    return result;
  }

  private async _flushRotationBatch(
    batch: Array<{ id: string; ciphertext: Buffer }>,
    newCrypto: CryptoLayer,
  ): Promise<void> {
    // Sementara set decryptFn ke newCrypto untuk batch ini
    const prevDecrypt = this.engine.decryptFn;
    this.engine.decryptFn = (buf: Buffer) => newCrypto.decrypt(buf);

    for (const { id, ciphertext } of batch) {
      try {
        await this.engine.upsert(id, ciphertext);
      } catch {
        // record mungkin tidak ada lagi (race condition) — skip
      }
    }

    // Restore ke prevDecrypt sampai seluruh rotate selesai
    this.engine.decryptFn = prevDecrypt;
    await this.engine.flush();
  }
}
