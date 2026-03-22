import type { QueryFilter, QueryOptions, QueryPlan, IndexDefinition } from '../../types/index.js';
export interface ExecutionStats {
    /** Jumlah dokumen yang dikembalikan */
    nReturned: number;
    /** Jumlah dokumen yang di-scan (termasuk yang tidak cocok filter) */
    totalDocsScanned: number;
    /** Jumlah index key yang diakses */
    totalKeysScanned: number;
    /** Waktu eksekusi dalam ms */
    executionTimeMs: number;
    /** Plan yang digunakan */
    planType: string;
    /** Apakah index dipakai */
    indexUsed: string | null;
    /** Berapa dokumen yang di-reject oleh filter (scanned - returned) */
    nRejected: number;
}
export interface QueryExplanation extends QueryPlan {
    executionStats?: ExecutionStats;
}
export declare class QueryPlanner {
    private readonly indexes;
    private readonly estimatedTotal;
    constructor(indexes: Map<string, IndexDefinition>, estimatedTotal: () => bigint);
    plan(filter: QueryFilter, options?: QueryOptions): QueryPlan;
    /**
     * G19: Buat ExecutionStats tracker untuk dipakai di Collection.find().
     * Dikembalikan sebagai object yang di-mutasi selama eksekusi.
     */
    createStatsTracker(): ExecutionStats;
    private _estimateSelectivity;
    shouldUseIndex(plan: QueryPlan, total: number): boolean;
}
//# sourceMappingURL=planner.d.ts.map