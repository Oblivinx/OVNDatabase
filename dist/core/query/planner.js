// ============================================================
//  OvnDB v3.0 — Query Planner + Execution Stats (G19)
//
//  G19 FIX: ExecutionStats — track nReturned, totalDocsScanned,
//  totalKeysScanned, executionTimeMs saat query dieksekusi.
//  Collection.findWithStats() mengembalikan docs + stats sekaligus.
// ============================================================
export class QueryPlanner {
    indexes;
    estimatedTotal;
    constructor(indexes, estimatedTotal) {
        this.indexes = indexes;
        this.estimatedTotal = estimatedTotal;
    }
    plan(filter, options) {
        const total = Number(this.estimatedTotal());
        if (options?.hint && this.indexes.has(options.hint)) {
            return {
                planType: 'indexScan',
                indexField: options.hint,
                estimatedDocs: Math.ceil(total * 0.1),
                estimatedCost: Math.log2(total) + Math.ceil(total * 0.1),
            };
        }
        const idValue = filter['_id'];
        if (idValue !== undefined && typeof idValue !== 'object') {
            return { planType: 'primaryKey', estimatedDocs: 1, estimatedCost: 1 };
        }
        if (filter['$or']) {
            const orClauses = filter['$or'];
            if (orClauses.every(c => '_id' in c)) {
                return {
                    planType: 'primaryKey',
                    estimatedDocs: orClauses.length,
                    estimatedCost: orClauses.length,
                };
            }
        }
        let bestPlan = null;
        for (const [field, def] of this.indexes) {
            const condition = filter[field];
            if (condition === undefined)
                continue;
            const selectivity = def.unique ? 1 / total : this._estimateSelectivity(condition, total);
            const estimatedDocs = Math.max(1, Math.ceil(total * selectivity));
            const cost = Math.log2(Math.max(total, 2)) + estimatedDocs;
            if (!bestPlan || cost < bestPlan.estimatedCost) {
                bestPlan = {
                    planType: 'indexScan',
                    indexField: field,
                    estimatedDocs,
                    estimatedCost: cost,
                    indexCardinality: def.unique ? total : Math.ceil(total * 0.5),
                };
            }
        }
        return bestPlan ?? { planType: 'fullCollection', estimatedDocs: total, estimatedCost: total };
    }
    /**
     * G19: Buat ExecutionStats tracker untuk dipakai di Collection.find().
     * Dikembalikan sebagai object yang di-mutasi selama eksekusi.
     */
    createStatsTracker() {
        return {
            nReturned: 0,
            totalDocsScanned: 0,
            totalKeysScanned: 0,
            executionTimeMs: 0,
            planType: 'fullCollection',
            indexUsed: null,
            nRejected: 0,
        };
    }
    _estimateSelectivity(condition, total) {
        if (condition === null || typeof condition !== 'object')
            return 1 / Math.max(total, 1);
        const ops = condition;
        if ('$eq' in ops)
            return 1 / Math.max(total * 0.01, 1);
        if ('$in' in ops)
            return ops.$in.length / Math.max(total, 1);
        if ('$gt' in ops || '$gte' in ops || '$lt' in ops || '$lte' in ops)
            return 0.3;
        if ('$regex' in ops)
            return 0.1;
        if ('$exists' in ops)
            return ops.$exists ? 0.8 : 0.2;
        return 0.5;
    }
    shouldUseIndex(plan, total) {
        if (plan.planType === 'primaryKey')
            return true;
        if (plan.planType === 'indexScan')
            return plan.estimatedCost < total * 0.8;
        return false;
    }
}
//# sourceMappingURL=planner.js.map