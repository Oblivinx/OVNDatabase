// ============================================================
//  OvnDB v2.1 — CryptoLayer (AES-256-GCM per-record encryption)
//
//  update: added verify(), reencrypt(), rotateKey(),
//          isEncryptedBuffer() helper,
//          stronger salt file protection check
// ============================================================

import crypto from 'crypto';
import fs     from 'fs';
import fsp    from 'fs/promises';
import path   from 'path';

export const IV_SIZE         = 12;   // AES-GCM nonce
export const TAG_SIZE        = 16;   // GCM auth tag
export const CRYPTO_OVERHEAD = IV_SIZE + TAG_SIZE; // 28 bytes per record

const PBKDF2_ITER   = 100_000;
const PBKDF2_DIGEST = 'sha512';
const KEY_LEN       = 32; // 256 bit
const SALT_LEN      = 32; // 256 bit
const ALG           = 'aes-256-gcm' as const;

export class CryptoLayer {
  private readonly key: Buffer;

  private constructor(key: Buffer) {
    this.key = key;
  }

  // ── Factory ───────────────────────────────────────────────

  static fromKey(key: Buffer): CryptoLayer {
    if (key.length !== KEY_LEN)
      throw new Error(`[CryptoLayer] Key harus 32 bytes, got ${key.length}`);
    return new CryptoLayer(key);
  }

  static async fromPassphrase(passphrase: string, dataDir: string): Promise<CryptoLayer> {
    const saltPath = path.join(dataDir, '.salt');
    let salt: Buffer;

    if (fs.existsSync(saltPath)) {
      salt = await fsp.readFile(saltPath);
      // update: validasi ukuran salt file
      if (salt.length !== SALT_LEN)
        throw new Error(`[CryptoLayer] Salt file corrupt — expected ${SALT_LEN} bytes, got ${salt.length}`);
    } else {
      salt = crypto.randomBytes(SALT_LEN);
      await fsp.mkdir(dataDir, { recursive: true });
      await fsp.writeFile(saltPath, salt, { mode: 0o600 });
    }

    const key = await new Promise<Buffer>((resolve, reject) =>
      crypto.pbkdf2(passphrase, salt, PBKDF2_ITER, KEY_LEN, PBKDF2_DIGEST,
        (err, derivedKey) => err ? reject(err) : resolve(derivedKey))
    );
    return new CryptoLayer(key);
  }

  // ── Encrypt / Decrypt ─────────────────────────────────────

  encrypt(plaintext: Buffer): Buffer {
    const iv     = crypto.randomBytes(IV_SIZE);
    const cipher = crypto.createCipheriv(ALG, this.key, iv);
    const ct     = Buffer.concat([cipher.update(plaintext), cipher.final()]);
    const tag    = cipher.getAuthTag();
    return Buffer.concat([iv, tag, ct]);
  }

  decrypt(ciphertext: Buffer): Buffer {
    if (ciphertext.length < CRYPTO_OVERHEAD)
      throw new Error('[CryptoLayer] Ciphertext terlalu pendek — corrupt atau key salah');
    const iv     = ciphertext.subarray(0, IV_SIZE);
    const tag    = ciphertext.subarray(IV_SIZE, IV_SIZE + TAG_SIZE);
    const ct     = ciphertext.subarray(IV_SIZE + TAG_SIZE);
    const decipher = crypto.createDecipheriv(ALG, this.key, iv);
    decipher.setAuthTag(tag);
    try {
      return Buffer.concat([decipher.update(ct), decipher.final()]);
    } catch {
      throw new Error('[CryptoLayer] Dekripsi gagal — data corrupt, tampered, atau key salah');
    }
  }

  /**
   * feat: verify — cek apakah ciphertext valid tanpa mendekripsi payload.
   * Berguna untuk integrity check saat startup atau backup verification.
   * @returns true jika auth tag valid, false jika tampered/corrupt
   */
  verify(ciphertext: Buffer): boolean {
    try {
      this.decrypt(ciphertext);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * feat: reencrypt — dekripsi dengan key lama, enkripsi dengan key baru.
   * Berguna untuk key rotation per-record.
   *
   * @example
   *   const newCrypto = await CryptoLayer.fromPassphrase(newPass, dataDir);
   *   const newCiphertext = oldCrypto.reencrypt(oldCiphertext, newCrypto);
   */
  reencrypt(ciphertext: Buffer, newCrypto: CryptoLayer): Buffer {
    const plaintext = this.decrypt(ciphertext);
    return newCrypto.encrypt(plaintext);
  }

  /**
   * feat: isEncryptedBuffer — heuristik cek apakah buffer kemungkinan ciphertext.
   * Tidak 100% akurat tapi berguna untuk migrasi data lama ke encrypted.
   * Cek: panjang >= CRYPTO_OVERHEAD dan TIDAK bisa di-parse sebagai JSON.
   */
  static isEncryptedBuffer(buf: Buffer): boolean {
    if (buf.length < CRYPTO_OVERHEAD) return false;
    try {
      JSON.parse(buf.toString('utf8'));
      return false; // valid JSON = plaintext
    } catch {
      return true; // bukan valid JSON = kemungkinan ciphertext
    }
  }

  /**
   * feat: rotateKey — re-encrypt semua buffer dalam array dengan key baru.
   * Untuk mass key rotation. Kembalikan array buffer baru.
   *
   * @example
   *   const newBufs = oldCrypto.rotateKey(ciphertexts, newCrypto);
   */
  rotateKey(ciphertexts: Buffer[], newCrypto: CryptoLayer): Buffer[] {
    return ciphertexts.map(ct => this.reencrypt(ct, newCrypto));
  }
}

export async function cryptoFromPassphrase(passphrase: string, dataDir: string): Promise<CryptoLayer> {
  return CryptoLayer.fromPassphrase(passphrase, dataDir);
}
