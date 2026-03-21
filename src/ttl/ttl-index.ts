// ============================================================
//  OvnDB v2.0 — TTLIndex (Time-To-Live Auto-expiry)
//
//  Secara otomatis menghapus dokumen yang sudah expired.
//  Berguna untuk:
//   - Session (TTL: 30 menit idle)
//   - Rate limit windows (TTL: 1 jam)
//   - OTP / token (TTL: 5 menit)
//   - Cooldown state (TTL: 24 jam)
//   - Cache sementara
//
//  Cara kerja:
//   1. Dokumen punya field TTL (default: 'expiresAt', epoch ms)
//   2. Background worker cek setiap `checkInterval` ms
//   3. Dokumen dengan expiresAt < Date.now() dihapus otomatis
//   4. Worker pakai `.unref()` → tidak mencegah Node.js exit
//
//  Optimasi v2:
//   - Gunakan secondary index pada expiresAt jika ada → O(log n) bukan O(n)
//   - Batch delete per siklus (batchSize) untuk kontrol latency
//   - onPurge callback untuk monitoring/alerting
// ============================================================

import type { Collection }  from '../collection/collection.js';
import type { OvnDocument } from '../types/index.js';
import { makeLogger }       from '../utils/logger.js';

const log = makeLogger('ttl');

export interface TTLIndexOptions {
  /**
   * Nama field yang menyimpan timestamp expiry (epoch ms).
   * @default 'expiresAt'
   */
  field?: string;

  /**
   * Interval pengecekan background worker (ms).
   * Minimum 5.000 ms untuk mencegah terlalu sering scan.
   * @default 60_000 (1 menit)
   */
  checkInterval?: number;

  /**
   * Jumlah dokumen maksimum yang dihapus per siklus.
   * Mencegah satu siklus memakan terlalu banyak CPU.
   * @default 1_000
   */
  batchSize?: number;

  /**
   * Callback setelah setiap siklus purge.
   * Gunakan untuk logging ke monitoring system.
   */
  onPurge?: (deleted: number, durationMs: number, total: number) => void;
}

export class TTLIndex<T extends OvnDocument = OvnDocument> {
  private readonly _col:      Collection<T>;
  private readonly _field:    string;
  private readonly _interval: number;
  private readonly _batch:    number;
  private readonly _onPurge?: (d: number, ms: number, total: number) => void;

  private _timer:    ReturnType<typeof setInterval> | null = null;
  private _running   = false;
  private _totalDel  = 0;
  private _cycles    = 0;

  constructor(collection: Collection<T>, opts: TTLIndexOptions = {}) {
    this._col      = collection;
    this._field    = opts.field         ?? 'expiresAt';
    this._interval = Math.max(opts.checkInterval ?? 60_000, 5_000);
    this._batch    = opts.batchSize     ?? 1_000;
    this._onPurge  = opts.onPurge;
  }

  // ── Lifecycle ─────────────────────────────────────────────

  /**
   * Mulai background worker. Idempotent — aman dipanggil berkali-kali.
   * @returns this (chainable)
   */
  start(): this {
    if (this._running) return this;
    this._running = true;
    this._timer   = setInterval(
      () => this._purge().catch(err => log.error('TTL purge error', { err: String(err) })),
      this._interval,
    );
    // unref() → worker tidak mencegah process exit jika semua pekerjaan selesai
    (this._timer as unknown as { unref?: () => void }).unref?.();
    log.info('TTL index started', { field: this._field, interval: this._interval });
    return this;
  }

  /** Hentikan background worker. */
  stop(): void {
    if (!this._running) return;
    this._running = false;
    if (this._timer) { clearInterval(this._timer); this._timer = null; }
    log.info('TTL index stopped', { totalDeleted: this._totalDel, cycles: this._cycles });
  }

  /**
   * Jalankan satu siklus purge manual tanpa menunggu interval.
   * Berguna untuk testing atau on-demand cleanup.
   * @returns jumlah dokumen yang dihapus
   */
  async purgeNow(): Promise<number> { return this._purge(); }

  get isRunning():    boolean { return this._running; }
  get totalDeleted(): number  { return this._totalDel; }
  get cycleCount():   number  { return this._cycles; }

  // ── Static Helpers ────────────────────────────────────────

  /**
   * Hitung timestamp expiresAt dari sekarang.
   * @example
   *   TTLIndex.expiresIn(30, 'minutes')  // 30 menit dari sekarang
   *   TTLIndex.expiresIn(1,  'hours')    // 1 jam
   *   TTLIndex.expiresIn(7,  'days')     // 7 hari
   */
  static expiresIn(amount: number, unit: 'seconds' | 'minutes' | 'hours' | 'days'): number {
    const ms = { seconds: 1_000, minutes: 60_000, hours: 3_600_000, days: 86_400_000 };
    return Date.now() + amount * ms[unit];
  }

  /** Cek apakah dokumen sudah expired. */
  static isExpired(doc: Record<string, unknown>, field = 'expiresAt'): boolean {
    const val = doc[field];
    return typeof val === 'number' && Date.now() > val;
  }

  /** Sisa waktu sebelum expired (ms). Infinity jika tidak ada TTL. */
  static ttlRemaining(doc: Record<string, unknown>, field = 'expiresAt'): number {
    const val = doc[field];
    return typeof val === 'number' ? val - Date.now() : Infinity;
  }

  /** Format sisa waktu sebagai string human-readable. */
  static formatTTL(doc: Record<string, unknown>, field = 'expiresAt'): string {
    const ms = TTLIndex.ttlRemaining(doc, field);
    if (ms === Infinity) return 'no expiry';
    if (ms <= 0) return 'expired';
    if (ms < 60_000) return `${Math.ceil(ms / 1_000)}s`;
    if (ms < 3_600_000) return `${Math.ceil(ms / 60_000)}m`;
    if (ms < 86_400_000) return `${Math.ceil(ms / 3_600_000)}h`;
    return `${Math.ceil(ms / 86_400_000)}d`;
  }

  // ── Private ───────────────────────────────────────────────

  private async _purge(): Promise<number> {
    const start  = Date.now();
    const now    = start;
    let   deleted = 0;

    try {
      // Gunakan secondary index jika field di-index (lebih efisien)
      const expired = await this._col.find(
        { [this._field]: { $lte: now } } as import('../types/index.js').QueryFilter,
        { limit: this._batch },
      );

      for (const doc of expired) {
        const ok = await this._col.deleteOne({ _id: doc._id });
        if (ok) deleted++;
      }

      if (deleted > 0) await this._col.flush();
    } catch (err) {
      log.error('TTL purge gagal', { err: String(err), field: this._field });
    }

    const dur = Date.now() - start;
    this._totalDel += deleted;
    this._cycles++;

    if (deleted > 0) {
      log.info('TTL purge selesai', {
        deleted, durationMs: dur,
        field: this._field, totalEver: this._totalDel,
      });
    }

    this._onPurge?.(deleted, dur, this._totalDel);
    return deleted;
  }
}
