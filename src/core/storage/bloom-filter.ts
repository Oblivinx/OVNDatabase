// ============================================================
//  OvnDB v4.0 — Bloom Filter (per-segment)
//
//  Probabilistic data structure: sebelum B+ Tree lookup, cek
//  apakah key "mungkin" ada di segment. Jika "pasti tidak ada",
//  skip disk I/O sama sekali. False positive rate ~1–2%.
//
//  Implementasi:
//  - k = 7 hash functions (optimal untuk fpr ~1%)
//  - Bit array (Buffer) — compact, serializable
//  - Hash: FNV-1a variant (fast, no crypto overhead)
//
//  Cara pakai:
//    const bf = new BloomFilter(10_000); // expected 10K items
//    bf.add('mykey');
//    bf.test('mykey');     // true (mungkin ada)
//    bf.test('missing');   // false (pasti tidak ada) atau true (false positive ~1%)
// ============================================================

export class BloomFilter {
  private readonly bits:    Buffer;
  private readonly k:       number;  // jumlah hash functions
  private readonly m:       number;  // jumlah bits

  /**
   * @param capacity    Expected jumlah keys (digunakan untuk size bit array optimal)
   * @param fpr         Target false positive rate (default 0.01 = 1%)
   */
  constructor(capacity: number, fpr = 0.01) {
    // m = -n * ln(p) / (ln(2)^2)
    const m = Math.ceil(-capacity * Math.log(fpr) / (Math.LN2 * Math.LN2));
    // k = (m/n) * ln(2)
    const k = Math.max(1, Math.round((m / capacity) * Math.LN2));

    this.m    = Math.max(64, m);
    this.k    = Math.min(k, 20); // cap at 20 hash functions
    this.bits = Buffer.alloc(Math.ceil(this.m / 8));
  }

  /** Buat BloomFilter dari raw buffer (deserialisasi) */
  static fromBuffer(buf: Buffer<ArrayBuffer>, k: number): BloomFilter {
    const bf = new BloomFilter(1); // dummy constructor
    // Copy into plain Buffer to avoid SharedArrayBuffer type issues
    const copy = Buffer.allocUnsafe(buf.length);
    buf.copy(copy);
    (bf as unknown as { bits: Buffer<ArrayBuffer> }).bits = copy;
    (bf as unknown as { k: number }).k = k;
    (bf as unknown as { m: number }).m = buf.length * 8;
    return bf;
  }

  /** Serialize ke Buffer untuk penyimpanan */
  toBuffer(): Buffer { return Buffer.from(this.bits); }

  get hashCount(): number { return this.k; }
  get bitCount():  number { return this.m; }

  add(key: string): void {
    for (let i = 0; i < this.k; i++) {
      const bit = this._hash(key, i) % this.m;
      this.bits[bit >> 3]! |= (1 << (bit & 7));
    }
  }

  /**
   * Test apakah key MUNGKIN ada.
   * false = pasti tidak ada (tidak perlu disk lookup)
   * true  = mungkin ada (lanjutkan B+ Tree lookup)
   */
  test(key: string): boolean {
    for (let i = 0; i < this.k; i++) {
      const bit = this._hash(key, i) % this.m;
      if (!(this.bits[bit >> 3]! & (1 << (bit & 7)))) return false;
    }
    return true;
  }

  /** Estimasi jumlah elemen (approx) */
  get approximateCount(): number {
    let ones = 0;
    for (const byte of this.bits) {
      let b = byte;
      while (b) { ones += b & 1; b >>= 1; }
    }
    // n_approx = -m/k * ln(1 - X/m) where X = ones
    const ratio = ones / this.m;
    if (ratio >= 1) return this.m; // saturated
    return Math.round(-this.m / this.k * Math.log(1 - ratio));
  }

  // ── Private ───────────────────────────────────────────────

  /**
   * FNV-1a inspired hash with seed for multiple independent hashes.
   * Tidak pakai crypto untuk performa — bloom filter OK dengan non-crypto hash.
   */
  private _hash(key: string, seed: number): number {
    let h = 0x811c9dc5 ^ seed;
    for (let i = 0; i < key.length; i++) {
      h ^= key.charCodeAt(i);
      // FNV prime: 0x01000193
      h = (Math.imul(h, 0x01000193) >>> 0);
    }
    // Double hashing: h1 + i*h2 untuk k independent probes
    let h2 = 0x5bd1e995 ^ seed;
    for (let i = 0; i < key.length; i++) {
      h2 ^= key.charCodeAt(i);
      h2 = (Math.imul(h2, 0x5bd1e995) >>> 0);
    }
    return ((h + seed * h2) >>> 0);
  }
}
