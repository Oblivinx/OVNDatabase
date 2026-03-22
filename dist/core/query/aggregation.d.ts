import type { AggregationStage } from '../../types/index.js';
export type AggFn = (docs: Record<string, unknown>[]) => Record<string, unknown>[];
/**
 * Compile pipeline (array of stages) menjadi satu fungsi yang bisa
 * dijalankan terhadap array dokumen.
 *
 * @param pipeline   Array of aggregation stages
 * @param lookupResolver  Fungsi untuk resolve $lookup (collection name → docs)
 */
export declare function compilePipeline(pipeline: AggregationStage[], lookupResolver?: (collection: string) => Promise<Record<string, unknown>[]>): (docs: Record<string, unknown>[]) => Promise<Record<string, unknown>[]>;
//# sourceMappingURL=aggregation.d.ts.map