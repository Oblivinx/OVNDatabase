import type { Collection } from '../collection/collection.js';
import type { OvnDocument } from '../types/index.js';
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
export declare class TTLIndex<T extends OvnDocument = OvnDocument> {
    private readonly _col;
    private readonly _field;
    private readonly _interval;
    private readonly _batch;
    private readonly _onPurge?;
    private _timer;
    private _running;
    private _totalDel;
    private _cycles;
    constructor(collection: Collection<T>, opts?: TTLIndexOptions);
    /**
     * Mulai background worker. Idempotent — aman dipanggil berkali-kali.
     * @returns this (chainable)
     */
    start(): this;
    /** Hentikan background worker. */
    stop(): void;
    /**
     * Jalankan satu siklus purge manual tanpa menunggu interval.
     * Berguna untuk testing atau on-demand cleanup.
     * @returns jumlah dokumen yang dihapus
     */
    purgeNow(): Promise<number>;
    get isRunning(): boolean;
    get totalDeleted(): number;
    get cycleCount(): number;
    /**
     * Hitung timestamp expiresAt dari sekarang.
     * @example
     *   TTLIndex.expiresIn(30, 'minutes')  // 30 menit dari sekarang
     *   TTLIndex.expiresIn(1,  'hours')    // 1 jam
     *   TTLIndex.expiresIn(7,  'days')     // 7 hari
     */
    static expiresIn(amount: number, unit: 'seconds' | 'minutes' | 'hours' | 'days'): number;
    /** Cek apakah dokumen sudah expired. */
    static isExpired(doc: Record<string, unknown>, field?: string): boolean;
    /** Sisa waktu sebelum expired (ms). Infinity jika tidak ada TTL. */
    static ttlRemaining(doc: Record<string, unknown>, field?: string): number;
    /** Format sisa waktu sebagai string human-readable. */
    static formatTTL(doc: Record<string, unknown>, field?: string): string;
    private _purge;
}
//# sourceMappingURL=ttl-index.d.ts.map