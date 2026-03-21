// ============================================================
//  OvnDB v2.0 — Query Planner (Cost-Based Optimizer)
//
//  v1 tidak memiliki planner — setiap find() langsung full scan
//  kecuali ada secondary index yang cocok (dan itu pun tanpa
//  cost estimation).
//
//  v2 Query Planner:
//   1. Analisis filter: ekstrak field yang di-filter
//   2. Cek apakah ada index untuk field tersebut
//   3. Estimasi cost masing-masing plan
//   4. Pilih plan dengan cost terendah
//
//  Cost model (simplified):
//   - PrimaryKey lookup: cost = 1 (O(log n) dengan tree, ~O(1) dengan cache)
//   - Index scan: cost = log(n) + k (k = dokumen yang match)
//   - Full collection scan: cost = n (semua dokumen)
//
//  Dalam mode explain(), kembalikan plan tanpa eksekusi.
// ============================================================

import type {
  QueryFilter, QueryOptions, QueryPlan, IndexDefinition,
} from '../../types/index.js';
import { getFieldValue } from './filter.js';

export class QueryPlanner {
  private readonly indexes: Map<string, IndexDefinition>;
  private readonly estimatedTotal: () => bigint;

  constructor(
    indexes: Map<string, IndexDefinition>,
    estimatedTotal: () => bigint,
  ) {
    this.indexes       = indexes;
    this.estimatedTotal = estimatedTotal;
  }

  /**
   * Analisis filter dan kembalikan plan terbaik.
   * Plan dipilih berdasarkan cost estimation.
   */
  plan(filter: QueryFilter, options?: QueryOptions): QueryPlan {
    const total = Number(this.estimatedTotal());

    // Force index jika hint diberikan
    if (options?.hint && this.indexes.has(options.hint)) {
      return {
        planType:      'indexScan',
        indexField:    options.hint,
        estimatedDocs: Math.ceil(total * 0.1),
        estimatedCost: Math.log2(total) + Math.ceil(total * 0.1),
      };
    }

    // Cek primary key lookup (_id)
    const idValue = filter['_id'];
    if (idValue !== undefined && typeof idValue !== 'object') {
      return {
        planType:      'primaryKey',
        estimatedDocs: 1,
        estimatedCost: 1,
      };
    }

    // Cek $or dengan _id — masih bisa pakai primary key
    if (filter['$or']) {
      const orClauses = filter['$or'] as QueryFilter[];
      if (orClauses.every(c => '_id' in c)) {
        return {
          planType:      'primaryKey',
          estimatedDocs: orClauses.length,
          estimatedCost: orClauses.length,
        };
      }
    }

    // Cari index yang paling selektif untuk filter yang ada
    let bestPlan: QueryPlan | null = null;

    for (const [field, def] of this.indexes) {
      const condition = filter[field];
      if (condition === undefined) continue;

      // Estimasi berapa banyak dokumen yang akan di-return oleh index ini
      const selectivity = def.unique
        ? 1 / total       // unique index sangat selektif
        : this._estimateSelectivity(condition, total);

      const estimatedDocs = Math.max(1, Math.ceil(total * selectivity));
      const cost = Math.log2(Math.max(total, 2)) + estimatedDocs;

      if (!bestPlan || cost < bestPlan.estimatedCost) {
        bestPlan = {
          planType:           'indexScan',
          indexField:         field,
          estimatedDocs,
          estimatedCost:      cost,
          indexCardinality:   def.unique ? total : Math.ceil(total * 0.5),
        };
      }
    }

    if (bestPlan) return bestPlan;

    // Fallback: full collection scan
    return {
      planType:      'fullCollection',
      estimatedDocs: total,
      estimatedCost: total,
    };
  }

  /**
   * Estimasi selectivity kondisi filter (berapa fraction dokumen yang cocok).
   * 0.0 = sangat selektif (sedikit dokumen cocok)
   * 1.0 = tidak selektif (semua dokumen cocok)
   */
  private _estimateSelectivity(condition: unknown, total: number): number {
    if (condition === null || typeof condition !== 'object') return 1 / Math.max(total, 1);

    const ops = condition as Record<string, unknown>;
    if ('$eq' in ops)   return 1 / Math.max(total * 0.01, 1); // ~1%
    if ('$in' in ops)   return (ops.$in as unknown[]).length / Math.max(total, 1);
    if ('$gt' in ops || '$gte' in ops || '$lt' in ops || '$lte' in ops)
      return 0.3; // range: estimasi 30% cocok
    if ('$regex' in ops) return 0.1; // regex: estimasi 10% cocok
    if ('$exists' in ops) return ops.$exists ? 0.8 : 0.2;
    return 0.5; // default
  }

  /**
   * Cek apakah plan ini bisa digunakan untuk filter yang diberikan
   * (beberapa plan lebih efisien untuk filter tertentu).
   */
  shouldUseIndex(plan: QueryPlan, total: number): boolean {
    if (plan.planType === 'primaryKey') return true;
    if (plan.planType === 'indexScan') {
      // Gunakan index hanya jika lebih murah dari full scan
      return plan.estimatedCost < total * 0.8;
    }
    return false;
  }
}
