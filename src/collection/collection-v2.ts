// ============================================================
//  OvnDB v2.1 — CollectionV2
//
//  fix: hapus monkey-patching _withEncryption yang tidak thread-safe.
//       Ganti dengan override _serialize() dan _parse() yang bersih.
//
//  Cara kerja enkripsi (baru, lebih aman):
//   INSERT: _serialize(doc) → encrypt(JSON(doc)) → tulis ke engine
//   READ:   baca dari engine → _parse(buf) → decrypt(buf) → JSON.parse
//
//  Keuntungan vs v2.0:
//   - Tidak ada monkey-patching engine methods (race condition safe)
//   - Enkripsi terjadi di layer Collection, bukan di wrapping function
//   - aggregate(), createIndex(), semua path otomatis terenkripsi
//   - CollectionV2 bisa di-extend lagi tanpa kekhawatiran
//
//  update: aggregate() mewarisi fix dari Collection yang sudah pakai
//          engine.scan() dengan decryptFn — tidak perlu override.
// ============================================================

import { Collection }  from './collection.js';
import type { CryptoLayer } from '../crypto/crypto-layer.js';
import type { StorageEngine } from '../core/storage/storage-engine.js';
import type { IndexDefinition, OvnDocument } from '../types/index.js';

export interface CollectionV2Options {
  /** CryptoLayer untuk enkripsi per-record. Gunakan cryptoFromPassphrase() untuk setup mudah. */
  crypto?:  CryptoLayer;
  /** Daftar secondary index yang akan dibuat saat collection dibuka. */
  indexes?: IndexDefinition[];
}

export class CollectionV2<T extends OvnDocument = OvnDocument> extends Collection<T> {
  private readonly crypto?: CryptoLayer;
  /** feat: flag untuk stats/status — apakah collection ini terenkripsi */
  readonly isEncrypted: boolean;

  constructor(name: string, engine: StorageEngine, opts: CollectionV2Options = {}) {
    super(name, engine);
    this.crypto      = opts.crypto;
    this.isEncrypted = !!opts.crypto;

    // Inject decrypt function ke StorageEngine agar:
    // 1. scan() bisa decode _id dari writeBuffer (untuk index rebuild)
    // 2. engine.read() path langsung (bukan lewat _parse) juga tercover
    if (opts.crypto) {
      engine.decryptFn = (buf: Buffer) => opts.crypto!.decrypt(buf);
    }
  }

  /** Inisialisasi: buka semua index yang dikonfigurasi. */
  async init(opts: CollectionV2Options = {}): Promise<void> {
    if (opts.indexes) {
      for (const def of opts.indexes) {
        await this.createIndex(def);
      }
    }
  }

  // ── fix: override _serialize untuk enkripsi ───────────────
  //
  //  Sebelumnya, enkripsi dilakukan lewat monkey-patching sementara
  //  pada engine.insert/update/upsert di dalam _withEncryption().
  //  Pendekatan itu rentan race condition bila ada dua operation
  //  concurrent (misalnya via Promise.all di luar).
  //
  //  Solusi bersih: override _serialize() — dipanggil oleh semua
  //  write method di Collection (insertOne, updateOne, replaceOne).
  //  Enkripsi terjadi tepat sebelum data masuk ke engine buffer.

  protected override _serialize(doc: T): Buffer {
    // fix: enkripsi doc di sini, bukan di wrapper function
    const plain = Buffer.from(JSON.stringify(doc), 'utf8');
    return this.crypto ? this.crypto.encrypt(plain) : plain;
  }

  // ── fix: override _parse untuk dekripsi ──────────────────
  //
  //  Data yang keluar dari engine (writeBuffer atau segment) masih
  //  dalam bentuk ciphertext. _parse() mendekripsi sebelum JSON.parse.
  //
  //  engine.decryptFn juga di-set di constructor sehingga engine.read()
  //  (yang TIDAK melewati _parse) juga mendapat plaintext.
  //  Keduanya harus di-set agar semua read path konsisten.

  protected override _parse(buf: Buffer): T | null {
    if (!this.crypto) return super._parse(buf);
    try {
      const decrypted = this.crypto.decrypt(buf);
      return JSON.parse(decrypted.toString('utf8')) as T;
    } catch {
      // Bisa terjadi jika buf adalah plaintext (data lama sebelum enkripsi diaktifkan)
      // Coba parse langsung sebagai fallback
      try { return JSON.parse(buf.toString('utf8')) as T; } catch { return null; }
    }
  }

  // ── Semua method CRUD mewarisi dari Collection ────────────
  //
  //  Tidak perlu override insertOne, updateOne, deleteOne, find, dll.
  //  Collection sudah memanggil _serialize() dan _parse() di semua
  //  operasi yang relevan, sehingga enkripsi/dekripsi otomatis terjadi.
  //
  //  update: aggregate() di Collection sudah pakai engine.scan() yang
  //  menggunakan engine.decryptFn (di-set di constructor kita).
  //  Tidak ada duplikasi atau bug di path ini lagi.
}
