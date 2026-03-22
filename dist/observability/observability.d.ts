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
    op: string;
    collection: string;
    durationMs: number;
    docsScanned?: number;
    docsReturned?: number;
    timestamp: number;
    planType?: string;
    error?: string;
}
export interface CollectionMetrics {
    collection: string;
    opCounts: Record<string, number>;
    totalOps: number;
    totalErrors: number;
    avgDurationMs: Record<string, number>;
    p50Ms: Record<string, number>;
    p95Ms: Record<string, number>;
    p99Ms: Record<string, number>;
    cacheHitRate: number;
    opsPerSecond: number;
}
export interface ObservabilityReport {
    generatedAt: string;
    uptimeSeconds: number;
    totalOps: number;
    totalErrors: number;
    slowQueries: OperationRecord[];
    collections: CollectionMetrics[];
}
export declare class Observability {
    private readonly opts;
    private readonly startTime;
    private readonly histograms;
    private readonly opCounts;
    private readonly errCounts;
    private readonly slowLog;
    private _totalOps;
    private _totalErrors;
    constructor(opts?: ObservabilityOptions);
    /**
     * Record hasil satu operasi.
     * Dipanggil oleh Collection.wrapOp() setelah setiap operasi.
     */
    record(rec: OperationRecord): void;
    /**
     * Helper: wrap async function dengan timing otomatis.
     * @example
     *   const result = await obs.measure('find', 'users', () => col.find(filter));
     */
    measure<T>(op: string, collection: string, fn: () => Promise<T>, extra?: Partial<OperationRecord>): Promise<T>;
    /** Buat report lengkap semua metrics. */
    report(): ObservabilityReport;
    /**
     * Export metrics dalam format Prometheus text exposition.
     * Mount di /metrics endpoint untuk Prometheus scraping.
     */
    toPrometheus(): string;
    /** Reset semua metrics. */
    reset(): void;
    get totalOps(): number;
    get totalErrors(): number;
}
export declare function getObservability(opts?: ObservabilityOptions): Observability;
export declare function resetObservability(): void;
//# sourceMappingURL=observability.d.ts.map