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
import { validateFieldPath } from '../utils/security.js';
import { makeLogger } from '../utils/logger.js';
const log = makeLogger('ttl');
export class TTLIndex {
    _col;
    _field;
    _interval;
    _batch;
    _onPurge;
    _timer = null;
    _running = false;
    _totalDel = 0;
    _cycles = 0;
    constructor(collection, opts = {}) {
        this._col = collection;
        const fieldName = opts.field ?? 'expiresAt';
        // SECURITY: validasi field path — cegah __proto__ atau path traversal
        // TTLIndex memakai field name langsung sebagai query key; jika tidak divalidasi,
        // opts.field = '__proto__' akan menjadi { ['__proto__']: { $lte: now } }
        // yang berpotensi menyebabkan prototype pollution di matchFilter.
        validateFieldPath(fieldName);
        this._field = fieldName;
        this._interval = Math.max(opts.checkInterval ?? 60_000, 5_000);
        this._batch = Math.min(Math.max(opts.batchSize ?? 1_000, 1), 10_000);
        this._onPurge = opts.onPurge;
    }
    // ── Lifecycle ─────────────────────────────────────────────
    /**
     * Mulai background worker. Idempotent — aman dipanggil berkali-kali.
     * @returns this (chainable)
     */
    start() {
        if (this._running)
            return this;
        this._running = true;
        this._timer = setInterval(() => this._purge().catch(err => log.error('TTL purge error', { err: String(err) })), this._interval);
        // unref() → worker tidak mencegah process exit jika semua pekerjaan selesai
        this._timer.unref?.();
        log.info('TTL index started', { field: this._field, interval: this._interval });
        return this;
    }
    /** Hentikan background worker. */
    stop() {
        if (!this._running)
            return;
        this._running = false;
        if (this._timer) {
            clearInterval(this._timer);
            this._timer = null;
        }
        log.info('TTL index stopped', { totalDeleted: this._totalDel, cycles: this._cycles });
    }
    /**
     * Jalankan satu siklus purge manual tanpa menunggu interval.
     * Berguna untuk testing atau on-demand cleanup.
     * @returns jumlah dokumen yang dihapus
     */
    async purgeNow() { return this._purge(); }
    get isRunning() { return this._running; }
    get totalDeleted() { return this._totalDel; }
    get cycleCount() { return this._cycles; }
    // ── Static Helpers ────────────────────────────────────────
    /**
     * Hitung timestamp expiresAt dari sekarang.
     * @example
     *   TTLIndex.expiresIn(30, 'minutes')  // 30 menit dari sekarang
     *   TTLIndex.expiresIn(1,  'hours')    // 1 jam
     *   TTLIndex.expiresIn(7,  'days')     // 7 hari
     */
    static expiresIn(amount, unit) {
        const ms = { seconds: 1_000, minutes: 60_000, hours: 3_600_000, days: 86_400_000 };
        return Date.now() + amount * ms[unit];
    }
    /** Cek apakah dokumen sudah expired. */
    static isExpired(doc, field = 'expiresAt') {
        const val = doc[field];
        return typeof val === 'number' && Date.now() > val;
    }
    /** Sisa waktu sebelum expired (ms). Infinity jika tidak ada TTL. */
    static ttlRemaining(doc, field = 'expiresAt') {
        const val = doc[field];
        return typeof val === 'number' ? val - Date.now() : Infinity;
    }
    /** Format sisa waktu sebagai string human-readable. */
    static formatTTL(doc, field = 'expiresAt') {
        const ms = TTLIndex.ttlRemaining(doc, field);
        if (ms === Infinity)
            return 'no expiry';
        if (ms <= 0)
            return 'expired';
        if (ms < 60_000)
            return `${Math.ceil(ms / 1_000)}s`;
        if (ms < 3_600_000)
            return `${Math.ceil(ms / 60_000)}m`;
        if (ms < 86_400_000)
            return `${Math.ceil(ms / 3_600_000)}h`;
        return `${Math.ceil(ms / 86_400_000)}d`;
    }
    // ── Private ───────────────────────────────────────────────
    async _purge() {
        const start = Date.now();
        const now = start;
        let deleted = 0;
        try {
            // Gunakan secondary index jika field di-index (lebih efisien)
            const expired = await this._col.find({ [this._field]: { $lte: now } }, { limit: this._batch });
            for (const doc of expired) {
                const ok = await this._col.deleteOne({ _id: doc._id });
                if (ok)
                    deleted++;
            }
            if (deleted > 0)
                await this._col.flush();
        }
        catch (err) {
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
//# sourceMappingURL=ttl-index.js.map