// ============================================================
//  OvnDB v2.0 — Observability
//
//  Monitoring lengkap untuk production:
//   - Per-operasi timing (insert, update, delete, find, aggregate)
//   - Slow query log (query yang melebihi threshold)
//   - Latency percentiles: P50, P90, P95, P99
//   - Error rate per collection
//   - Cache hit rate
//   - Operasi per detik (throughput)
//
//  Singleton pattern: getObservability() mengembalikan instance global.
//  Enable lewat OvnDBOptions.observability atau environment variable.
//
//  Integrasi monitoring:
//   - Export ke Prometheus: obs.toPrometheus()
//   - Export ke JSON: obs.report()
//   - Reset stats: obs.reset()
// ============================================================

import { makeLogger } from '../utils/logger.js';

const log = makeLogger('observability');

export interface ObservabilityOptions {
  /**
   * Query dengan duration > threshold (ms) akan di-log sebagai slow query.
   * @default 100
   */
  slowQueryThresholdMs?: number;

  /**
   * Jumlah slow query terbaru yang disimpan dalam memory.
   * @default 100
   */
  slowQueryLogSize?: number;

  /**
   * Jika true, log setiap operasi ke console/stderr.
   * Hanya untuk debugging — jangan aktifkan di production.
   * @default false
   */
  verbose?: boolean;
}

export interface OperationRecord {
  op:         string;       // 'insert' | 'find' | 'update' | 'delete' | 'aggregate'
  collection: string;
  durationMs: number;
  docsScanned?: number;
  docsReturned?: number;
  timestamp:  number;
  planType?:  string;
  error?:     string;
}

export interface CollectionMetrics {
  collection:   string;
  opCounts:     Record<string, number>;
  totalOps:     number;
  totalErrors:  number;
  avgDurationMs: Record<string, number>;
  p50Ms:        Record<string, number>;
  p95Ms:        Record<string, number>;
  p99Ms:        Record<string, number>;
  cacheHitRate: number;
  opsPerSecond: number;
}

export interface ObservabilityReport {
  generatedAt:   string;
  uptimeSeconds: number;
  totalOps:      number;
  totalErrors:   number;
  slowQueries:   OperationRecord[];
  collections:   CollectionMetrics[];
}

// ── Histogram untuk latency percentile ───────────────────────

class Histogram {
  private readonly samples: number[] = [];
  private readonly MAX = 10_000; // rolling window

  record(durationMs: number): void {
    if (this.samples.length >= this.MAX) this.samples.shift();
    this.samples.push(durationMs);
  }

  percentile(p: number): number {
    if (this.samples.length === 0) return 0;
    const sorted = [...this.samples].sort((a, b) => a - b);
    const idx    = Math.ceil(p / 100 * sorted.length) - 1;
    return sorted[Math.max(0, idx)] ?? 0;
  }

  get avg(): number {
    if (this.samples.length === 0) return 0;
    return this.samples.reduce((s, v) => s + v, 0) / this.samples.length;
  }

  get count(): number { return this.samples.length; }
}

// ── Observability Singleton ───────────────────────────────────

let _instance: Observability | null = null;

export class Observability {
  private readonly opts:      Required<ObservabilityOptions>;
  private readonly startTime: number;
  private readonly histograms: Map<string, Map<string, Histogram>> = new Map(); // col → op → hist
  private readonly opCounts:   Map<string, Map<string, number>>    = new Map();
  private readonly errCounts:  Map<string, number>                 = new Map();
  private readonly slowLog:    OperationRecord[] = [];
  private _totalOps   = 0;
  private _totalErrors = 0;

  constructor(opts: ObservabilityOptions = {}) {
    this.opts = {
      slowQueryThresholdMs: opts.slowQueryThresholdMs ?? 100,
      slowQueryLogSize:     opts.slowQueryLogSize     ?? 100,
      verbose:              opts.verbose              ?? false,
    };
    this.startTime = Date.now();
  }

  // ── Recording API ─────────────────────────────────────────

  /**
   * Record hasil satu operasi.
   * Dipanggil oleh Collection.wrapOp() setelah setiap operasi.
   */
  record(rec: OperationRecord): void {
    const { op, collection, durationMs, error } = rec;
    this._totalOps++;
    if (error) {
      this._totalErrors++;
      this.errCounts.set(collection, (this.errCounts.get(collection) ?? 0) + 1);
    }

    // Update histogram
    if (!this.histograms.has(collection)) this.histograms.set(collection, new Map());
    const colHist = this.histograms.get(collection)!;
    if (!colHist.has(op)) colHist.set(op, new Histogram());
    colHist.get(op)!.record(durationMs);

    // Update op count
    if (!this.opCounts.has(collection)) this.opCounts.set(collection, new Map());
    const colOps = this.opCounts.get(collection)!;
    colOps.set(op, (colOps.get(op) ?? 0) + 1);

    // Slow query log
    if (durationMs >= this.opts.slowQueryThresholdMs) {
      this.slowLog.push(rec);
      if (this.slowLog.length > this.opts.slowQueryLogSize) this.slowLog.shift();
      log.warn('Slow query', { op, collection, durationMs, planType: rec.planType });
    }

    if (this.opts.verbose) {
      log.debug(`op`, { op, collection, durationMs });
    }
  }

  /**
   * Helper: wrap async function dengan timing otomatis.
   * @example
   *   const result = await obs.measure('find', 'users', () => col.find(filter));
   */
  async measure<T>(
    op: string,
    collection: string,
    fn: () => Promise<T>,
    extra?: Partial<OperationRecord>,
  ): Promise<T> {
    const start = Date.now();
    let error: string | undefined;
    try {
      return await fn();
    } catch (err) {
      error = String(err);
      throw err;
    } finally {
      this.record({ op, collection, durationMs: Date.now() - start, timestamp: Date.now(), error, ...extra });
    }
  }

  // ── Report ────────────────────────────────────────────────

  /** Buat report lengkap semua metrics. */
  report(): ObservabilityReport {
    const uptimeSeconds = (Date.now() - this.startTime) / 1_000;
    const collections: CollectionMetrics[] = [];

    for (const [col, hist] of this.histograms) {
      const opCountMap = this.opCounts.get(col) ?? new Map<string, number>();
      const totalColOps = [...opCountMap.values()].reduce((s, v) => s + v, 0);
      const metrics: CollectionMetrics = {
        collection:   col,
        opCounts:     Object.fromEntries(opCountMap),
        totalOps:     totalColOps,
        totalErrors:  this.errCounts.get(col) ?? 0,
        avgDurationMs: {},
        p50Ms:        {},
        p95Ms:        {},
        p99Ms:        {},
        cacheHitRate: 0, // diisi dari engine stats jika perlu
        opsPerSecond: Math.round(totalColOps / Math.max(uptimeSeconds, 1)),
      };
      for (const [op, h] of hist) {
        metrics.avgDurationMs[op] = Math.round(h.avg);
        metrics.p50Ms[op]         = Math.round(h.percentile(50));
        metrics.p95Ms[op]         = Math.round(h.percentile(95));
        metrics.p99Ms[op]         = Math.round(h.percentile(99));
      }
      collections.push(metrics);
    }

    return {
      generatedAt:   new Date().toISOString(),
      uptimeSeconds: Math.round(uptimeSeconds),
      totalOps:      this._totalOps,
      totalErrors:   this._totalErrors,
      slowQueries:   [...this.slowLog],
      collections,
    };
  }

  /**
   * Export metrics dalam format Prometheus text exposition.
   * Mount di /metrics endpoint untuk Prometheus scraping.
   */
  toPrometheus(): string {
    const rep   = this.report();
    const lines: string[] = [
      '# HELP ovndb_total_ops Total operations processed',
      '# TYPE ovndb_total_ops counter',
      `ovndb_total_ops ${rep.totalOps}`,
      '# HELP ovndb_total_errors Total operations with errors',
      '# TYPE ovndb_total_errors counter',
      `ovndb_total_errors ${rep.totalErrors}`,
      '# HELP ovndb_uptime_seconds Database uptime in seconds',
      '# TYPE ovndb_uptime_seconds gauge',
      `ovndb_uptime_seconds ${rep.uptimeSeconds}`,
    ];
    for (const col of rep.collections) {
      const label = `collection="${col.collection}"`;
      for (const [op, count] of Object.entries(col.opCounts)) {
        lines.push(`ovndb_op_count{${label},op="${op}"} ${count}`);
      }
      for (const [op, p99] of Object.entries(col.p99Ms)) {
        lines.push(`ovndb_p99_ms{${label},op="${op}"} ${p99}`);
      }
    }
    return lines.join('\n');
  }

  /** Reset semua metrics. */
  reset(): void {
    this.histograms.clear();
    this.opCounts.clear();
    this.errCounts.clear();
    this.slowLog.length = 0;
    this._totalOps = 0;
    this._totalErrors = 0;
  }

  get totalOps():    number { return this._totalOps; }
  get totalErrors(): number { return this._totalErrors; }
}

// ── Singleton helpers ─────────────────────────────────────────

export function getObservability(opts?: ObservabilityOptions): Observability {
  if (!_instance) _instance = new Observability(opts);
  return _instance;
}

export function resetObservability(): void {
  _instance?.reset();
}
