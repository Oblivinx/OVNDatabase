// ============================================================
//  TTLIndex — Time-To-Live auto-expiry for collections
//
//  Kegunaan utama untuk bot WhatsApp:
//   - Session expiry      (TTL: 30 menit idle)
//   - Rate limit windows  (TTL: 1 jam)
//   - OTP / token         (TTL: 5 menit)
//   - Cooldown state      (TTL: 24 jam)
//   - Temporary cache     (TTL: sesuai kebutuhan)
//
//  Cara kerja:
//   - Setiap dokumen punya field TTL (default: 'expiresAt', epoch ms)
//   - Background worker berjalan tiap `checkInterval` ms
//   - Dokumen yang expiresAt < Date.now() dihapus otomatis
//   - Worker bisa dimatikan (stop()) untuk collection yang tidak aktif
//
//  API:
//    // Buat TTL index dengan field 'expiresAt'
//    const ttl = new TTLIndex(collection, { field: 'expiresAt', checkInterval: 60_000 });
//    ttl.start();
//
//    // Insert dokumen dengan TTL 30 menit
//    await collection.insertOne({
//      sessionId: 'abc',
//      data: {...},
//      expiresAt: Date.now() + 30 * 60 * 1000,
//    });
//
//    // Helper: hitung expiresAt
//    const expiresAt = TTLIndex.expiresIn(30, 'minutes');
// ============================================================
import type { Collection }  from '../collection.js';
import type { OvnDocument } from '../types.js';
import { makeLogger }       from '../utils/logger.js';

const log = makeLogger('ttl-index');

export interface TTLIndexOptions {
  /**
   * Nama field yang menyimpan timestamp expiry (epoch ms).
   * Default: 'expiresAt'
   */
  field?: string;

  /**
   * Seberapa sering background worker memeriksa expired docs (ms).
   * Default: 60_000 (1 menit)
   * Minimum: 5_000 (5 detik)
   */
  checkInterval?: number;

  /**
   * Jumlah maksimal dokumen yang dihapus per siklus.
   * Mencegah satu siklus terlalu lama untuk collection besar.
   * Default: 10_000
   */
  batchSize?: number;

  /**
   * Callback dipanggil setelah setiap siklus purge.
   * Berguna untuk monitoring / logging ke sistem eksternal.
   */
  onPurge?: (deleted: number, durationMs: number) => void;
}

export class TTLIndex<T extends OvnDocument = OvnDocument> {
  private readonly _collection:    Collection<T>;
  private readonly _field:         string;
  private readonly _checkInterval: number;
  private readonly _batchSize:     number;
  private readonly _onPurge?:      (deleted: number, durationMs: number) => void;

  private _timer:       ReturnType<typeof setInterval> | null = null;
  private _running      = false;
  private _totalDeleted = 0;
  private _cycleCount   = 0;

  constructor(collection: Collection<T>, opts: TTLIndexOptions = {}) {
    this._collection    = collection;
    this._field         = opts.field         ?? 'expiresAt';
    this._checkInterval = Math.max(opts.checkInterval ?? 60_000, 5_000);
    this._batchSize     = opts.batchSize     ?? 10_000;
    this._onPurge       = opts.onPurge;
  }

  // ── Lifecycle ────────────────────────────────────────────

  /**
   * Mulai background worker.
   * Aman dipanggil berkali-kali (idempotent).
   */
  start(): this {
    if (this._running) return this;
    this._running = true;

    this._timer = setInterval(() => {
      this._purge().catch(err =>
        log.error('TTL purge error', { err: String(err), field: this._field })
      );
    }, this._checkInterval);

    // Unref agar tidak mencegah Node.js exit
    (this._timer as unknown as { unref?: () => void }).unref?.();

    log.info(`TTL index started`, {
      field:         this._field,
      checkInterval: this._checkInterval,
      batchSize:     this._batchSize,
    });

    return this;
  }

  /**
   * Hentikan background worker.
   */
  stop(): void {
    if (!this._running) return;
    this._running = false;

    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }

    log.info(`TTL index stopped`, {
      totalDeleted: this._totalDeleted,
      cycles:       this._cycleCount,
    });
  }

  /**
   * Jalankan purge manual satu kali (tanpa menunggu interval).
   * Berguna untuk testing atau purge on-demand.
   */
  async purgeNow(): Promise<number> {
    return this._purge();
  }

  get isRunning():    boolean { return this._running; }
  get totalDeleted(): number  { return this._totalDeleted; }
  get cycleCount():   number  { return this._cycleCount; }

  // ── Static helpers ───────────────────────────────────────

  /**
   * Hitung timestamp expiresAt dari sekarang.
   *
   * @example
   *   TTLIndex.expiresIn(30, 'minutes')   // 30 menit dari sekarang
   *   TTLIndex.expiresIn(1,  'hours')     // 1 jam dari sekarang
   *   TTLIndex.expiresIn(7,  'days')      // 7 hari dari sekarang
   */
  static expiresIn(amount: number, unit: 'seconds' | 'minutes' | 'hours' | 'days'): number {
    const multipliers = {
      seconds: 1_000,
      minutes: 60_000,
      hours:   3_600_000,
      days:    86_400_000,
    };
    return Date.now() + amount * multipliers[unit];
  }

  /**
   * Cek apakah dokumen sudah expired.
   */
  static isExpired(doc: Record<string, unknown>, field = 'expiresAt'): boolean {
    const val = doc[field];
    if (typeof val !== 'number') return false;
    return Date.now() > val;
  }

  /**
   * Sisa waktu sebelum expired (ms). Negatif jika sudah expired.
   */
  static ttlRemaining(doc: Record<string, unknown>, field = 'expiresAt'): number {
    const val = doc[field];
    if (typeof val !== 'number') return Infinity;
    return val - Date.now();
  }

  // ── Private ──────────────────────────────────────────────

  private async _purge(): Promise<number> {
    const startMs  = Date.now();
    const now      = startMs;
    let   deleted  = 0;

    try {
      // Cari semua dokumen yang sudah expired
      // Menggunakan $lte agar bisa memanfaatkan secondary index jika ada
      const expired = await (this._collection as any).find(
        { [this._field]: { $lte: now } },
        { limit: this._batchSize }
      ) as T[];

      for (const doc of expired) {
        const ok = await this._collection.deleteOne({ _id: doc._id });
        if (ok) deleted++;
      }

      if (deleted > 0) await this._collection.flush();

    } catch (err) {
      log.error('TTL purge gagal', { err: String(err) });
    }

    const durationMs = Date.now() - startMs;
    this._totalDeleted += deleted;
    this._cycleCount++;

    if (deleted > 0) {
      log.info(`TTL purge selesai`, {
        deleted,
        durationMs,
        field:      this._field,
        totalEver:  this._totalDeleted,
      });
    }

    this._onPurge?.(deleted, durationMs);
    return deleted;
  }
}
