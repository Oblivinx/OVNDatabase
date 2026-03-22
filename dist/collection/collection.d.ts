import { type ExecutionStats } from '../core/query/planner.js';
import { ChangeStreamRegistry } from './change-stream.js';
import { SecondaryIndexManager } from '../core/index/secondary-index.js';
import { FTSIndex } from '../core/index/fts-index.js';
import type { StorageEngine } from '../core/storage/storage-engine.js';
import type { OvnDocument, QueryFilter, QueryOptions, UpdateSpec, AggregationStage, OvnStats, IndexDefinition, QueryPlan, BulkWriteOp, BulkWriteResult, ExportOptions, ImportOptions, ImportResult } from '../types/index.js';
export declare class Collection<T extends OvnDocument = OvnDocument> {
    readonly name: string;
    protected readonly engine: StorageEngine;
    protected readonly indexes: Map<string, IndexDefinition>;
    protected readonly idxMgr: SecondaryIndexManager;
    protected readonly streams: ChangeStreamRegistry<T>;
    private readonly planner;
    /** v4.0: FTS indexes keyed by field name */
    protected readonly ftsIndexes: Map<string, FTSIndex>;
    constructor(name: string, engine: StorageEngine);
    createIndex(def: IndexDefinition): Promise<void>;
    dropIndex(field: string | string[]): Promise<void>;
    /**
     * v4.0: Create a full-text search index on a field.
     * After creating, use { $text: 'query words' } in find()/findOne().
     *
     * @example
     *   await users.createTextIndex('name');
     *   const results = await users.find({ $text: 'budi jakarta' });
     */
    createTextIndex(field: string): Promise<void>;
    insertOne(doc: Omit<T, '_id'> & {
        _id?: string;
    }): Promise<T>;
    insertMany(docs: Array<Omit<T, '_id'> & {
        _id?: string;
    }>): Promise<T[]>;
    findOne(filter: QueryFilter, options?: Pick<QueryOptions, 'projection'>): Promise<T | null>;
    findById(id: string): Promise<T | null>;
    findManyById(ids: string[]): Promise<T[]>;
    find(filter?: QueryFilter, options?: QueryOptions): Promise<T[]>;
    /**
     * G19: find() + ExecutionStats — eksekusi query dan kembalikan stats lengkap.
     * Berguna untuk debugging query performance.
     *
     * @example
     *   const { docs, stats } = await users.findWithStats({ role: 'admin' });
     *   console.log(`Scanned: ${stats.totalDocsScanned}, Returned: ${stats.nReturned}, ${stats.executionTimeMs}ms`);
     */
    findWithStats(filter?: QueryFilter, options?: QueryOptions): Promise<{
        docs: T[];
        stats: ExecutionStats;
    }>;
    countDocuments(filter?: QueryFilter): Promise<number>;
    distinct(field: string, filter?: QueryFilter): Promise<unknown[]>;
    exists(filter: QueryFilter): Promise<boolean>;
    updateOne(filter: QueryFilter, spec: UpdateSpec): Promise<boolean>;
    updateMany(filter: QueryFilter, spec: UpdateSpec): Promise<number>;
    upsertOne(filter: QueryFilter, spec: UpdateSpec): Promise<T>;
    replaceOne(filter: QueryFilter, replacement: T): Promise<boolean>;
    deleteOne(filter: QueryFilter): Promise<boolean>;
    deleteMany(filter?: QueryFilter): Promise<number>;
    truncate(): Promise<void>;
    findOneAndUpdate(filter: QueryFilter, spec: UpdateSpec): Promise<T | null>;
    findOneAndDelete(filter: QueryFilter): Promise<T | null>;
    findOneAndReplace(filter: QueryFilter, replacement: Omit<T, '_id'>): Promise<T | null>;
    bulkWrite(ops: BulkWriteOp<T>[], options?: {
        ordered?: boolean;
    }): Promise<BulkWriteResult>;
    aggregate(pipeline: AggregationStage[], lookupResolver?: (colName: string) => Promise<Record<string, unknown>[]>): Promise<Record<string, unknown>[]>;
    explain(filter: QueryFilter, options?: QueryOptions): QueryPlan;
    watch(opts?: import('./change-stream.js').WatchOptions): import('./change-stream.js').ChangeStream<T>;
    compact(): Promise<void>;
    flush(): Promise<void>;
    stats(): Promise<OvnStats>;
    beginBulkLoad(): void;
    endBulkLoad(): Promise<void>;
    /**
     * v4.0: Export collection to a file.
     * Default format: NDJSON (streaming, one doc per line).
     *
     * @example
     *   await col.exportTo('./backup.ndjson');
     *   await col.exportTo('./backup.json', { format: 'json' });
     */
    exportTo(filePath: string, opts?: ExportOptions): Promise<number>;
    /**
     * v4.0: Import documents from a file into this collection.
     * Default format: NDJSON (auto-detected from extension).
     *
     * @example
     *   const result = await col.importFrom('./backup.ndjson');
     *   console.log(`Imported ${result.inserted} docs`);
     */
    importFrom(filePath: string, opts?: ImportOptions): Promise<ImportResult>;
    protected _scanWithPlan(filter: QueryFilter, options?: QueryOptions, stats?: ExecutionStats): AsyncGenerator<T>;
    /** v4.0: Find a compound index definition that covers all filter fields */
    private _findCompoundIndexFor;
    protected _parse(buf: Buffer): T | null;
    protected _serialize(doc: T): Buffer;
    private _emitChange;
}
//# sourceMappingURL=collection.d.ts.map