// ============================================================
//  OvnDB v3.0 — CryptoLayer
//
//  G12 FIX: FieldCrypto — enkripsi per-field, bukan per-record.
//           Memungkinkan query pada field yang tidak terenkripsi
//           sambil tetap menjaga kerahasiaan field sensitif.
//
//  G13 FIX: Key versioning — 1 byte header [keyVersion] sebelum IV.
//           Memungkinkan key rotation bertahap tanpa downtime.
//           Format ciphertext baru: [1 keyVer][12 IV][16 Tag][N CT]
//
//  G14 FIX: rotateEncryptionKey() terintegrasi di CollectionV2.
//           CryptoLayer menyediakan reencrypt() dan rotateKey() helpers.
// ============================================================

import crypto from 'crypto';
import fs     from 'fs';
import fsp    from 'fs/promises';
import path   from 'path';

export const IV_SIZE         = 12;
export const TAG_SIZE        = 16;
// G13: tambah 1 byte untuk key version
export const KEY_VERSION_SIZE = 1;
export const CRYPTO_OVERHEAD  = KEY_VERSION_SIZE + IV_SIZE + TAG_SIZE; // 29 bytes
// backward compat alias (v2.x tidak ada KEY_VERSION_SIZE, tapi parse tetap work)
export const CRYPTO_OVERHEAD_V2 = IV_SIZE + TAG_SIZE; // 28 bytes

const PBKDF2_ITER   = 100_000;
const PBKDF2_DIGEST = 'sha512';
const KEY_LEN       = 32;
const SALT_LEN      = 32;
const ALG           = 'aes-256-gcm' as const;
const MAX_KEY_VERSION = 255;

export class CryptoLayer {
  private readonly key:        Buffer;
  private readonly keyVersion: number;

  private constructor(key: Buffer, keyVersion = 0) {
    this.key        = key;
    this.keyVersion = keyVersion;
  }

  // ── Factory ───────────────────────────────────────────────

  static fromKey(key: Buffer, keyVersion = 0): CryptoLayer {
    if (key.length !== KEY_LEN) throw new Error(`[CryptoLayer] Key harus 32 bytes, got ${key.length}`);
    if (keyVersion < 0 || keyVersion > MAX_KEY_VERSION)
      throw new Error(`[CryptoLayer] keyVersion harus 0-${MAX_KEY_VERSION}`);
    return new CryptoLayer(key, keyVersion);
  }

  static async fromPassphrase(passphrase: string, dataDir: string, keyVersion = 0): Promise<CryptoLayer> {
    const saltPath = path.join(dataDir, '.salt');
    let salt: Buffer;

    if (fs.existsSync(saltPath)) {
      salt = await fsp.readFile(saltPath);
      if (salt.length !== SALT_LEN)
        throw new Error(`[CryptoLayer] Salt file corrupt — expected ${SALT_LEN} bytes, got ${salt.length}`);
    } else {
      salt = crypto.randomBytes(SALT_LEN);
      await fsp.mkdir(dataDir, { recursive: true });
      await fsp.writeFile(saltPath, salt, { mode: 0o600 });
    }

    const key = await new Promise<Buffer>((resolve, reject) =>
      crypto.pbkdf2(passphrase, salt, PBKDF2_ITER, KEY_LEN, PBKDF2_DIGEST,
        (err, dk) => err ? reject(err) : resolve(dk)),
    );
    return new CryptoLayer(key, keyVersion);
  }

  // ── Encrypt / Decrypt ─────────────────────────────────────

  /**
   * G13: Format baru: [1 keyVersion][12 IV][16 Tag][N CT]
   * Total overhead: 29 bytes (vs 28 di v2.x)
   */
  encrypt(plaintext: Buffer): Buffer {
    const iv     = crypto.randomBytes(IV_SIZE);
    const cipher = crypto.createCipheriv(ALG, this.key, iv);
    const ct     = Buffer.concat([cipher.update(plaintext), cipher.final()]);
    const tag    = cipher.getAuthTag();
    // G13: prepend key version byte
    return Buffer.concat([Buffer.from([this.keyVersion]), iv, tag, ct]);
  }

  /**
   * G13: Detect format otomatis (v2 28 bytes overhead vs v3 29 bytes).
   *
   * SECURITY FIX: Heuristic lama `ciphertext[0]! <= MAX_KEY_VERSION` selalu
   * bernilai true karena MAX_KEY_VERSION = 255 dan semua byte adalah <= 255.
   * Akibatnya semua ciphertext v2 (overhead 28 byte) salah-diparse sebagai v3
   * (overhead 29 byte), menghasilkan IV/tag dari offset yang salah → decrypt
   * selalu gagal untuk data yang ditulis versi lama.
   *
   * Fix: format v3 hanya dipakai jika panjang ciphertext > CRYPTO_OVERHEAD_V2
   * (artinya ada setidaknya 1 byte ekstra di depan untuk version byte).
   * Ini adalah kondisi yang perlu DAN cukup untuk membedakan v2 dari v3,
   * karena ciphertext v2 dengan panjang tepat CRYPTO_OVERHEAD_V2 + N byte
   * plaintext tidak akan memiliki byte "ekstra" di posisi 0.
   *
   * Catatan: Untuk plaintext kosong (N=0), v2 = 28 bytes, v3 = 29 bytes.
   * Ambiguitas hanya terjadi pada plaintext 0 byte (sangat jarang untuk JSON doc).
   * Dalam kasus tersebut kita gunakan tryV3 → fallback tryV2.
   */
  decrypt(ciphertext: Buffer): Buffer {
    if (ciphertext.length < CRYPTO_OVERHEAD_V2)
      throw new Error('[CryptoLayer] Ciphertext terlalu pendek');

    // Format v3 tepat 1 byte lebih panjang dari v2 untuk payload yang sama.
    // Kita coba v3 dulu jika panjang memungkinkan, lalu fallback ke v2.
    const canBeV3 = ciphertext.length > CRYPTO_OVERHEAD_V2;

    if (canBeV3) {
      // Coba parse sebagai v3: [1 ver][12 IV][16 Tag][N CT]
      try {
        const iv     = ciphertext.subarray(KEY_VERSION_SIZE, KEY_VERSION_SIZE + IV_SIZE);
        const tag    = ciphertext.subarray(KEY_VERSION_SIZE + IV_SIZE, KEY_VERSION_SIZE + IV_SIZE + TAG_SIZE);
        const ct     = ciphertext.subarray(KEY_VERSION_SIZE + IV_SIZE + TAG_SIZE);
        const dec    = crypto.createDecipheriv(ALG, this.key, iv);
        dec.setAuthTag(tag);
        return Buffer.concat([dec.update(ct), dec.final()]);
      } catch {
        // v3 parse gagal → coba v2 sebagai fallback (data mungkin ditulis versi lama)
      }
    }

    // Parse sebagai v2: [12 IV][16 Tag][N CT]
    const iv  = ciphertext.subarray(0, IV_SIZE);
    const tag = ciphertext.subarray(IV_SIZE, IV_SIZE + TAG_SIZE);
    const ct  = ciphertext.subarray(IV_SIZE + TAG_SIZE);
    const dec = crypto.createDecipheriv(ALG, this.key, iv);
    dec.setAuthTag(tag);
    try {
      return Buffer.concat([dec.update(ct), dec.final()]);
    } catch {
      throw new Error('[CryptoLayer] Dekripsi gagal — data corrupt, tampered, atau key salah');
    }
  }

  /** G13: ekstrak key version dari ciphertext tanpa dekripsi penuh */
  static getKeyVersion(ciphertext: Buffer): number | null {
    if (ciphertext.length < CRYPTO_OVERHEAD) return null;
    const v = ciphertext[0]!;
    return v <= MAX_KEY_VERSION ? v : null;
  }

  get version(): number { return this.keyVersion; }

  // ── Utils ─────────────────────────────────────────────────

  verify(ciphertext: Buffer): boolean {
    try { this.decrypt(ciphertext); return true; } catch { return false; }
  }

  reencrypt(ciphertext: Buffer, newCrypto: CryptoLayer): Buffer {
    return newCrypto.encrypt(this.decrypt(ciphertext));
  }

  rotateKey(ciphertexts: Buffer[], newCrypto: CryptoLayer): Buffer[] {
    return ciphertexts.map(ct => this.reencrypt(ct, newCrypto));
  }

  static isEncryptedBuffer(buf: Buffer): boolean {
    if (buf.length < CRYPTO_OVERHEAD_V2) return false;
    try { JSON.parse(buf.toString('utf8')); return false; } catch { return true; }
  }
}

// ── G12: FieldCrypto — enkripsi per-field ────────────────────

const ENCRYPTED_FIELDS_KEY = '_ef';

export class FieldCrypto {
  constructor(private readonly layer: CryptoLayer) {}

  /**
   * G12: Enkripsi field tertentu dalam dokumen.
   * Field yang terenkripsi disimpan sebagai base64 string.
   * Daftar field yang terenkripsi disimpan di _ef (hidden field).
   */
  encryptFields<T extends Record<string, unknown>>(
    doc: T,
    fields: (keyof T)[],
  ): T & { [ENCRYPTED_FIELDS_KEY]: string[] } {
    const result = { ...doc, [ENCRYPTED_FIELDS_KEY]: fields.map(String) };
    for (const f of fields) {
      const plain = Buffer.from(JSON.stringify(doc[f]), 'utf8');
      (result as Record<string, unknown>)[f as string] =
        this.layer.encrypt(plain).toString('base64');
    }
    return result as T & { [ENCRYPTED_FIELDS_KEY]: string[] };
  }

  /**
   * G12: Dekripsi field yang terenkripsi dalam dokumen.
   * Baca daftar field dari _ef lalu dekripsi satu per satu.
   */
  decryptFields<T extends Record<string, unknown>>(doc: T): T {
    const encryptedFields = ((doc as Record<string, unknown>)[ENCRYPTED_FIELDS_KEY] as string[]) ?? [];
    if (encryptedFields.length === 0) return doc;

    const result = { ...doc };
    for (const f of encryptedFields) {
      const b64 = (doc as Record<string, unknown>)[f];
      if (typeof b64 !== 'string') continue;
      try {
        const ct   = Buffer.from(b64, 'base64');
        const plain = this.layer.decrypt(ct).toString('utf8');
        (result as Record<string, unknown>)[f] = JSON.parse(plain);
      } catch { /* leave as-is jika gagal decrypt */ }
    }
    return result;
  }

  /**
   * G12: Cek apakah field tertentu terenkripsi dalam dokumen.
   */
  isEncryptedField(doc: Record<string, unknown>, field: string): boolean {
    const ef = doc[ENCRYPTED_FIELDS_KEY] as string[] | undefined;
    return Array.isArray(ef) && ef.includes(field);
  }
}

export async function cryptoFromPassphrase(passphrase: string, dataDir: string, keyVersion = 0): Promise<CryptoLayer> {
  return CryptoLayer.fromPassphrase(passphrase, dataDir, keyVersion);
}
