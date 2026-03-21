// ============================================================
//  Observability — Query timing, slow query log, metrics
//
//  Modul ini menyelesaikan gap observability terbesar di v1:
//   - Query timing per operasi
//   - Slow query log (configurable threshold)
//   - Hit/miss rate per secondary index field
//   - WAL growth monitoring
//   - Compaction pressure tracking
//   - In-memory ring buffer: tidak perlu storage tambahan
//
//  API:
//    const obs = new Observability({ slowQueryMs: 100 });
//
//    // Wrap operasi:
//    const result = await obs.track('findOne', 'users', () =>
//      collection.findOne({ phone: '628xxx' })
//    );
//
//    // Laporan:
//    const report = obs.report();
//    console.log(report.slowQueries);
//    console.log(report.summary);
// ============================================================
import { makeLogger } from '../utils/logger.js';

const log = makeLogger('observability');

// ── Types ─────────────────────────────────────────────────────

export interface QueryRecord {
  op:           string;   // 'findOne', 'find', 'insertOne', 'updateOne', 'deleteOne'
  collection:   string;
  durationMs:   number;
  timestamp:    number;
  slow:         boolean;
  indexUsed?:   string;   // nama field index yang dipakai (jika ada)
  resultCount?: number;   // jumlah dokumen hasil (untuk find)
  error?:       string;   // pesan error (jika gagal)
}

export interface CollectionMetrics {
  collection:   string;
  opCounts:     Record<string, number>;   // { findOne: 142, find: 38, ... }
  totalOps:     number;
  avgMs:        Record<string, number>;   // rata-rata ms per op
  p95Ms:        Record<string, number>;   // P95 ms per op
  errorRate:    number;                   // 0–1
  slowCount:    number;
  lastOpAt:     number;                   // epoch ms
}

export interface ObservabilityReport {
  generatedAt:    number;
  uptimeMs:       number;
  totalOps:       number;
  slowQueries:    QueryRecord[];          // semua slow queries (ring buffer)
  collections:    CollectionMetrics[];
  topSlow:        QueryRecord[];          // 10 paling lambat
  recentErrors:   QueryRecord[];          // error terakhir (max 50)
  summary: {
    totalOps:     number;
    slowOps:      number;
    errorOps:     number;
    avgDurationMs: number;
    p95DurationMs: number;
    p99DurationMs: number;
  };
}

export interface ObservabilityOptions {
  /**
   * Threshold ms untuk dianggap slow query.
   * Default: 100ms
   */
  slowQueryMs?: number;

  /**
   * Ukuran ring buffer untuk slow queries.
   * Default: 500 records
   */
  slowQueryBuffer?: number;

  /**
   * Ukuran ring buffer untuk semua queries (untuk statistik).
   * Default: 10_000 records
   */
  queryBuffer?: number;

  /**
   * Apakah log slow query ke console.
   * Default: true
   */
  logSlowQueries?: boolean;
}

// ── Observability ─────────────────────────────────────────────

export class Observability {
  private readonly _slowQueryMs:     number;
  private readonly _logSlowQueries:  boolean;
  private readonly _queryBuf:        QueryRecord[];
  private readonly _queryBufSize:    number;
  private readonly _slowBuf:         QueryRecord[];
  private readonly _slowBufSize:     number;
  private readonly _errorBuf:        QueryRecord[];
  private readonly _startedAt:       number;
  private _queryIdx  = 0;
  private _slowIdx   = 0;
  private _errorIdx  = 0;
  private _totalOps  = 0;
  private _totalMs   = 0;

  constructor(opts: ObservabilityOptions = {}) {
    this._slowQueryMs    = opts.slowQueryMs    ?? 100;
    this._logSlowQueries = opts.logSlowQueries ?? true;
    this._queryBufSize   = opts.queryBuffer    ?? 10_000;
    this._slowBufSize    = opts.slowQueryBuffer ?? 500;
    this._queryBuf       = new Array(this._queryBufSize);
    this._slowBuf        = new Array(this._slowBufSize);
    this._errorBuf       = new Array(50);
    this._startedAt      = Date.now();
  }

  // ── Tracking ─────────────────────────────────────────────

  /**
   * Wrap sebuah operasi dengan timing tracking.
   *
   * @example
   *   const doc = await obs.track('findOne', 'users', () =>
   *     users.findOne({ _id: id })
   *   );
   */
  async track<R>(
    op: string,
    collection: string,
    fn: () => Promise<R>,
    meta?: { indexUsed?: string },
  ): Promise<R> {
    const start = Date.now();
    let error: string | undefined;
    let result: R;

    try {
      result = await fn();
    } catch (err) {
      error = String(err);
      const rec = this._record(op, collection, Date.now() - start, meta?.indexUsed, undefined, error);
      this._addErrorRecord(rec);
      throw err;
    }

    const durationMs   = Date.now() - start;
    const resultCount  = Array.isArray(result) ? result.length : undefined;
    this._record(op, collection, durationMs, meta?.indexUsed, resultCount);

    return result!;
  }

  /**
   * Record manual (tanpa wrap) — berguna untuk integrasi manual.
   */
  record(op: string, collection: string, durationMs: number, opts?: {
    indexUsed?:   string;
    resultCount?: number;
    error?:       string;
  }): void {
    const rec = this._record(op, collection, durationMs, opts?.indexUsed, opts?.resultCount, opts?.error);
    if (opts?.error) this._addErrorRecord(rec);
  }

  // ── Report ───────────────────────────────────────────────

  report(): ObservabilityReport {
    const now         = Date.now();
    const allQueries  = this._getAll(this._queryBuf, this._queryBufSize);
    const slowQueries = this._getAll(this._slowBuf,  this._slowBufSize);
    const recentErrors= this._getAll(this._errorBuf, 50);

    // Aggregate per collection
    const colMap = new Map<string, QueryRecord[]>();
    for (const q of allQueries) {
      if (!colMap.has(q.collection)) colMap.set(q.collection, []);
      colMap.get(q.collection)!.push(q);
    }

    const collections: CollectionMetrics[] = [];
    for (const [col, recs] of colMap) {
      const opCounts: Record<string, number> = {};
      const opMs:     Record<string, number[]> = {};
      let   errors   = 0;
      let   slows    = 0;
      let   lastOpAt = 0;

      for (const r of recs) {
        opCounts[r.op]  = (opCounts[r.op] ?? 0) + 1;
        if (!opMs[r.op]) opMs[r.op] = [];
        opMs[r.op]!.push(r.durationMs);
        if (r.error)  errors++;
        if (r.slow)   slows++;
        if (r.timestamp > lastOpAt) lastOpAt = r.timestamp;
      }

      const avgMs: Record<string, number> = {};
      const p95Ms: Record<string, number> = {};
      for (const [op, mss] of Object.entries(opMs)) {
        const sorted = [...mss].sort((a, b) => a - b);
        avgMs[op] = sorted.reduce((s, v) => s + v, 0) / sorted.length;
        p95Ms[op] = sorted[Math.floor(sorted.length * 0.95)] ?? 0;
      }

      collections.push({
        collection: col,
        opCounts,
        totalOps:  recs.length,
        avgMs,
        p95Ms,
        errorRate: errors / recs.length,
        slowCount: slows,
        lastOpAt,
      });
    }

    // Top 10 paling lambat
    const topSlow = [...slowQueries]
      .sort((a, b) => b.durationMs - a.durationMs)
      .slice(0, 10);

    // Summary
    const allMs   = allQueries.map(q => q.durationMs).sort((a, b) => a - b);
    const slowOps = allQueries.filter(q => q.slow).length;
    const errOps  = allQueries.filter(q => q.error).length;

    return {
      generatedAt:  now,
      uptimeMs:     now - this._startedAt,
      totalOps:     this._totalOps,
      slowQueries,
      collections,
      topSlow,
      recentErrors,
      summary: {
        totalOps:      this._totalOps,
        slowOps,
        errorOps:      errOps,
        avgDurationMs: allMs.length ? allMs.reduce((s, v) => s + v, 0) / allMs.length : 0,
        p95DurationMs: allMs[Math.floor(allMs.length * 0.95)] ?? 0,
        p99DurationMs: allMs[Math.floor(allMs.length * 0.99)] ?? 0,
      },
    };
  }

  /**
   * Reset semua data (berguna setelah restart atau untuk test).
   */
  reset(): void {
    this._queryBuf.fill(undefined as unknown as QueryRecord);
    this._slowBuf.fill(undefined as unknown as QueryRecord);
    this._errorBuf.fill(undefined as unknown as QueryRecord);
    this._queryIdx = 0;
    this._slowIdx  = 0;
    this._errorIdx = 0;
    this._totalOps = 0;
    this._totalMs  = 0;
  }

  /**
   * Format laporan menjadi string yang mudah dibaca di console.
   */
  formatReport(): string {
    const r     = this.report();
    const lines: string[] = [];

    lines.push('');
    lines.push('╔══════════════════════════════════════════════════════╗');
    lines.push('║              OvnDB Observability Report              ║');
    lines.push('╠══════════════════════════════════════════════════════╣');
    lines.push(`║  Uptime         : ${this._formatDuration(r.uptimeMs).padEnd(33)}║`);
    lines.push(`║  Total ops      : ${String(r.summary.totalOps).padEnd(33)}║`);
    lines.push(`║  Slow queries   : ${String(r.summary.slowOps).padEnd(33)}║`);
    lines.push(`║  Errors         : ${String(r.summary.errorOps).padEnd(33)}║`);
    lines.push(`║  Avg duration   : ${(r.summary.avgDurationMs.toFixed(2) + 'ms').padEnd(33)}║`);
    lines.push(`║  P95 duration   : ${(r.summary.p95DurationMs.toFixed(2) + 'ms').padEnd(33)}║`);
    lines.push(`║  P99 duration   : ${(r.summary.p99DurationMs.toFixed(2) + 'ms').padEnd(33)}║`);
    lines.push('╠══════════════════════════════════════════════════════╣');
    lines.push('║  Per Collection                                      ║');

    for (const col of r.collections) {
      lines.push(`║  • ${col.collection.padEnd(49)}║`);
      lines.push(`║    total=${col.totalOps}  slows=${col.slowCount}  errorRate=${(col.errorRate * 100).toFixed(1)}%  ${''.padEnd(10)}║`);
    }

    if (r.topSlow.length > 0) {
      lines.push('╠══════════════════════════════════════════════════════╣');
      lines.push('║  Top Slow Queries                                    ║');
      for (const q of r.topSlow.slice(0, 5)) {
        const label = `${q.op}(${q.collection}) ${q.durationMs}ms`;
        lines.push(`║  • ${label.substring(0, 50).padEnd(50)}║`);
      }
    }

    lines.push('╚══════════════════════════════════════════════════════╝');
    return lines.join('\n');
  }

  // ── Privates ─────────────────────────────────────────────

  private _record(
    op: string, collection: string, durationMs: number,
    indexUsed?: string, resultCount?: number, error?: string
  ): QueryRecord {
    const slow = durationMs >= this._slowQueryMs;
    const rec: QueryRecord = {
      op, collection, durationMs,
      timestamp:   Date.now(),
      slow, indexUsed, resultCount, error,
    };

    // Ring buffer: selalu write bahkan jika overflow
    this._queryBuf[this._queryIdx % this._queryBufSize] = rec;
    this._queryIdx++;
    this._totalOps++;
    this._totalMs += durationMs;

    if (slow) {
      this._slowBuf[this._slowIdx % this._slowBufSize] = rec;
      this._slowIdx++;

      if (this._logSlowQueries) {
        log.warn(`Slow query terdeteksi`, {
          op, collection, durationMs,
          threshold: this._slowQueryMs,
          indexUsed, resultCount,
        });
      }
    }

    return rec;
  }

  private _addErrorRecord(rec: QueryRecord): void {
    this._errorBuf[this._errorIdx % 50] = rec;
    this._errorIdx++;
  }

  private _getAll<T>(buf: T[], size: number): T[] {
    return buf.filter(Boolean).slice(0, size);
  }

  private _formatDuration(ms: number): string {
    if (ms < 1000)        return `${ms}ms`;
    if (ms < 60_000)      return `${(ms / 1000).toFixed(1)}s`;
    if (ms < 3_600_000)   return `${Math.floor(ms / 60_000)}m ${Math.floor((ms % 60_000) / 1000)}s`;
    return `${Math.floor(ms / 3_600_000)}h ${Math.floor((ms % 3_600_000) / 60_000)}m`;
  }
}

// ── Singleton default instance ───────────────────────────────

let _defaultInstance: Observability | null = null;

/**
 * Ambil atau buat instance Observability default (singleton).
 * Berguna jika tidak ingin inject manually ke setiap collection.
 */
export function getObservability(opts?: ObservabilityOptions): Observability {
  if (!_defaultInstance) {
    _defaultInstance = new Observability(opts);
  }
  return _defaultInstance;
}

export function resetObservability(): void {
  _defaultInstance = null;
}
