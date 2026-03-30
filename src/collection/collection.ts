// ============================================================
//  OvnDB v4.0 — Collection
//
//  v4.0 NEW:
//  - createTextIndex(field) — full-text search index
//  - find({ $text: 'query' }) — FTS query operator
//  - Compound index support in _scanWithPlan()
//  - importFrom() / exportTo() — NDJSON and JSON
//
//  From v3.0:
//  - insertOne/updateOne/deleteOne: storage dulu, index kemudian
//  - updateMany/deleteMany: streaming — tidak load semua ke RAM
// ============================================================

import { generateId }       from '../utils/id-generator.js';
import { matchFilter, applyUpdate, applyProjection } from '../core/query/filter.js';
import { QueryPlanner, type ExecutionStats }  from '../core/query/planner.js';
import { compilePipeline }  from '../core/query/aggregation.js';
import { ChangeStreamRegistry } from './change-stream.js';
import { SecondaryIndexManager } from '../core/index/secondary-index.js';
import { FTSIndex }          from '../core/index/fts-index.js';
import { exportTo as _exportTo, importFrom as _importFrom } from './import-export.js';
import type { StorageEngine }  from '../core/storage/storage-engine.js';
import {
  validateQueryFilter, validateUpdateSpec, validateDocumentSize,
  validateDocumentKeys, validateDocumentId, isDangerousKey, MAX_PIPELINE_STAGES,
} from '../utils/security.js';
import type {
  OvnDocument, QueryFilter, QueryOptions, UpdateSpec,
  AggregationStage, OvnStats, IndexDefinition, QueryPlan,
  ChangeEvent, BulkWriteOp, BulkWriteResult,
  ExportOptions, ImportOptions, ImportResult,
} from '../types/index.js';

export class Collection<T extends OvnDocument = OvnDocument> {
  readonly name:    string;
  protected readonly engine:   StorageEngine;
  protected readonly indexes:  Map<string, IndexDefinition> = new Map();
  protected readonly idxMgr:  SecondaryIndexManager;
  protected readonly streams: ChangeStreamRegistry<T>;
  private   readonly planner: QueryPlanner;
  /** v4.0: FTS indexes keyed by field name */
  protected readonly ftsIndexes: Map<string, FTSIndex> = new Map();

  constructor(name: string, engine: StorageEngine) {
    this.name    = name;
    this.engine  = engine;
    this.idxMgr  = new SecondaryIndexManager(engine.dirPath, name);
    this.streams = new ChangeStreamRegistry<T>();
    this.planner = new QueryPlanner(this.indexes, () => this.engine.segments.totalLive);
    engine.setSecondaryIndex(this.idxMgr);
  }

  // ── Index Management ─────────────────────────────────────

  async createIndex(def: IndexDefinition): Promise<void> {
    // Support compound index: normalize field to string key
    const fieldKey = Array.isArray(def.field) ? def.field.join('__') : def.field;
    if (this.indexes.has(fieldKey)) return;
    this.indexes.set(fieldKey, def);
    this.idxMgr.addIndex(def);
    await this.idxMgr.open();
    const allDocs: Record<string, unknown>[] = [];
    for await (const [, buf] of this.engine.scan()) {
      const doc = this._parse(buf);
      if (doc) allDocs.push(doc as Record<string, unknown>);
    }
    if (allDocs.length > 0) this.idxMgr.rebuildFromDocs(allDocs);
    await this.idxMgr.save();
  }

  async dropIndex(field: string | string[]): Promise<void> {
    const key = Array.isArray(field) ? field.join('__') : field;
    this.indexes.delete(key);
  }

  /**
   * v4.0: Create a full-text search index on a field.
   * After creating, use { $text: 'query words' } in find()/findOne().
   *
   * @example
   *   await users.createTextIndex('name');
   *   const results = await users.find({ $text: 'budi jakarta' });
   */
  async createTextIndex(field: string): Promise<void> {
    if (this.ftsIndexes.has(field)) return;
    const idx = new FTSIndex(this.engine.dirPath, this.name, field);
    await idx.open();
    // Build from existing docs
    for await (const [, buf] of this.engine.scan()) {
      const doc = this._parse(buf);
      if (!doc) continue;
      const val = (doc as Record<string, unknown>)[field];
      if (typeof val === 'string') idx.index((doc as Record<string, unknown>)['_id'] as string, val);
    }
    await idx.save();
    this.ftsIndexes.set(field, idx);
  }

  // ── Insert ────────────────────────────────────────────────

  async insertOne(doc: Omit<T, '_id'> & { _id?: string }): Promise<T> {
    // SECURITY: validate doc BEFORE spread so __proto__ injection is still detectable
    // { ...doc } would normalize the prototype, losing the __proto__ pollution
    validateDocumentKeys(doc);
    const full = { ...doc, _id: doc._id ?? generateId() } as T;
    // SECURITY: validasi _id jika disupply user
    validateDocumentId(full._id);
    const buf  = this._serialize(full);
    // SECURITY: batasi ukuran dokumen sebelum masuk storage
    validateDocumentSize(buf);
    // FIX: storage DULU — jika insert gagal (misal duplicate _id), index tidak tersentuh
    await this.engine.insert(full._id, buf);
    // Index KEMUDIAN — jika gagal, rollback storage insert agar tetap konsisten
    try {
      this.idxMgr.onInsert(full as unknown as Record<string, unknown>);
      // v4.0: update FTS indexes
      for (const [field, ftsIdx] of this.ftsIndexes) {
        const val = (full as Record<string, unknown>)[field];
        if (typeof val === 'string') ftsIdx.index(full._id, val);
      }
    } catch (idxErr) {
      try { await this.engine.delete(full._id); } catch { /* best-effort rollback */ }
      throw idxErr;
    }
    this._emitChange({ operationType: 'insert', documentKey: { _id: full._id }, fullDocument: full, timestamp: Date.now(), txId: 0n });
    return full;
  }

  async insertMany(docs: Array<Omit<T, '_id'> & { _id?: string }>): Promise<T[]> {
    const results: T[] = [];
    for (const doc of docs) results.push(await this.insertOne(doc));
    await this.flush();
    return results;
  }

  // ── Find ──────────────────────────────────────────────────

  async findOne(filter: QueryFilter, options?: Pick<QueryOptions, 'projection'>): Promise<T | null> {
    // SECURITY: validasi filter sebelum eksekusi
    validateQueryFilter(filter);
    if (filter['_id'] !== undefined && typeof filter['_id'] !== 'object') {
      const buf = await this.engine.read(filter['_id'] as string);
      if (!buf) return null;
      const doc = this._parse(buf);
      return doc ? applyProjection(doc, options?.projection) as T : null;
    }
    for await (const doc of this._scanWithPlan(filter, { limit: 1, ...options })) {
      return applyProjection(doc, options?.projection) as T;
    }
    return null;
  }

  async findById(id: string): Promise<T | null> {
    const buf = await this.engine.read(id);
    if (!buf) return null;
    return this._parse(buf);
  }

  async findManyById(ids: string[]): Promise<T[]> {
    const unique = [...new Set(ids)];
    const results: T[] = [];
    await Promise.all(unique.map(async id => {
      const doc = await this.findById(id);
      if (doc) results.push(doc);
    }));
    return results;
  }

  async find(filter: QueryFilter = {}, options: QueryOptions = {}): Promise<T[]> {
    // SECURITY: validasi filter sebelum eksekusi
    validateQueryFilter(filter);
    if (options.explain) return [this.explain(filter, options) as unknown as T];

    const docs: T[] = [];
    for await (const doc of this._scanWithPlan(filter, options)) {
      docs.push(applyProjection(doc, options.projection) as T);
    }

    if (options.sort) {
      const entries = Object.entries(options.sort);
      docs.sort((a, b) => {
        for (const [field, dir] of entries) {
          const av = (a as Record<string, unknown>)[field];
          const bv = (b as Record<string, unknown>)[field];
          const cmp = av === bv ? 0 : av! < bv! ? -1 : 1;
          if (cmp !== 0) return cmp * dir;
        }
        return 0;
      });
    }
    return docs;
  }

  /**
   * G19: find() + ExecutionStats — eksekusi query dan kembalikan stats lengkap.
   * Berguna untuk debugging query performance.
   *
   * @example
   *   const { docs, stats } = await users.findWithStats({ role: 'admin' });
   *   console.log(`Scanned: ${stats.totalDocsScanned}, Returned: ${stats.nReturned}, ${stats.executionTimeMs}ms`);
   */
  async findWithStats(
    filter: QueryFilter = {},
    options: QueryOptions = {},
  ): Promise<{ docs: T[]; stats: ExecutionStats }> {
    const stats   = this.planner.createStatsTracker();
    const plan    = this.planner.plan(filter, options);
    const t0      = performance.now();

    stats.planType  = plan.planType;
    stats.indexUsed = plan.indexField ?? null;

    const docs: T[] = [];
    for await (const doc of this._scanWithPlan(filter, options, stats)) {
      docs.push(applyProjection(doc, options.projection) as T);
    }

    if (options.sort) {
      const entries = Object.entries(options.sort);
      docs.sort((a, b) => {
        for (const [field, dir] of entries) {
          const av = (a as Record<string, unknown>)[field];
          const bv = (b as Record<string, unknown>)[field];
          const cmp = av === bv ? 0 : av! < bv! ? -1 : 1;
          if (cmp !== 0) return cmp * dir;
        }
        return 0;
      });
    }

    stats.nReturned      = docs.length;
    stats.nRejected      = stats.totalDocsScanned - docs.length;
    stats.executionTimeMs = Math.round((performance.now() - t0) * 100) / 100;
    return { docs, stats };
  }

  async countDocuments(filter: QueryFilter = {}): Promise<number> {
    // SECURITY: validasi filter sebelum eksekusi
    validateQueryFilter(filter);
    let count = 0;
    for await (const _ of this._scanWithPlan(filter)) count++;
    return count;
  }

  async distinct(field: string, filter: QueryFilter = {}): Promise<unknown[]> {
    // SECURITY: validasi field path dan filter
    if (typeof field !== 'string' || field.length === 0)
      throw new Error('[OvnDB] distinct: field harus string non-kosong');
    for (const part of field.split('.')) {
      if (isDangerousKey(part))
        throw new Error(`[OvnDB] distinct: field path mengandung kunci terlarang: "${part}"`);
    }
    validateQueryFilter(filter);
    const values = new Set<string>();
    for (const doc of await this.find(filter)) {
      const val = (doc as Record<string, unknown>)[field];
      if (val !== undefined) values.add(JSON.stringify(val));
    }
    return [...values].map(v => JSON.parse(v));
  }

  async exists(filter: QueryFilter): Promise<boolean> {
    for await (const _ of this._scanWithPlan(filter, { limit: 1 })) return true;
    return false;
  }

  // ── Update ────────────────────────────────────────────────

  async updateOne(filter: QueryFilter, spec: UpdateSpec): Promise<boolean> {
    // SECURITY: validasi filter dan update spec sebelum eksekusi
    validateQueryFilter(filter);
    validateUpdateSpec(spec as Record<string, unknown>);
    const doc = await this.findOne(filter);
    if (!doc) return false;
    const before  = { ...doc };
    const updated = applyUpdate(doc as unknown as Record<string, unknown>, spec);
    updated['_id'] = doc._id;
    const buf = this._serialize(updated as T);
    // FIX: storage DULU — jika update gagal, index tidak tersentuh
    await this.engine.update(doc._id, buf);
    this.idxMgr.onUpdate(before as unknown as Record<string, unknown>, updated);
    // v4.0: update FTS indexes
    for (const [field, ftsIdx] of this.ftsIndexes) {
      const newVal = (updated as Record<string, unknown>)[field];
      if (typeof newVal === 'string') ftsIdx.index(doc._id, newVal);
      else if (ftsIdx.hasDoc(doc._id)) ftsIdx.remove(doc._id);
    }
    this._emitChange({
      operationType: 'update', documentKey: { _id: doc._id }, fullDocument: updated as T,
      updateDescription: { updatedFields: spec.$set ?? {}, removedFields: Object.keys(spec.$unset ?? {}) },
      timestamp: Date.now(), txId: 0n,
    });
    return true;
  }

  async updateMany(filter: QueryFilter, spec: UpdateSpec): Promise<number> {
    // SECURITY: validasi filter dan spec — updateOne akan validasi lagi per-doc tapi
    // lebih efisien validasi sekali di sini sebelum scan
    validateQueryFilter(filter);
    validateUpdateSpec(spec as Record<string, unknown>);
    // FIX: streaming scan — tidak load semua matching docs ke RAM
    // Kumpulkan _id dulu (lightweight), baru update satu per satu
    const ids: string[] = [];
    for await (const doc of this._scanWithPlan(filter)) ids.push(doc._id);
    let count = 0;
    for (const id of ids) { if (await this.updateOne({ _id: id }, spec)) count++; }
    return count;
  }

  async upsertOne(filter: QueryFilter, spec: UpdateSpec): Promise<T> {
    validateQueryFilter(filter);
    validateUpdateSpec(spec as Record<string, unknown>);
    const existing = await this.findOne(filter);
    if (!existing) {
      const insertFields = {
        ...(spec.$set ?? {}), ...(spec.$setOnInsert ?? {}),
        _id: (filter['_id'] as string) ?? generateId(),
      };
      return this.insertOne(insertFields as Omit<T, '_id'> & { _id?: string });
    }
    const updateSpec: UpdateSpec = { ...spec };
    delete updateSpec.$setOnInsert;
    await this.updateOne({ _id: existing._id }, updateSpec);
    return (await this.findOne({ _id: existing._id }))!;
  }

  async replaceOne(filter: QueryFilter, replacement: T): Promise<boolean> {
    validateQueryFilter(filter);
    // SECURITY: validasi replacement sebagai dokumen baru
    validateDocumentKeys(replacement);
    const doc = await this.findOne(filter);
    if (!doc) return false;
    const withId = { ...replacement, _id: doc._id };
    const buf    = this._serialize(withId as T);
    // FIX: storage DULU — jika update gagal, index tidak tersentuh
    await this.engine.update(doc._id, buf);
    this.idxMgr.onUpdate(doc as unknown as Record<string, unknown>, withId as unknown as Record<string, unknown>);
    // v4.0: update FTS indexes
    for (const [field, ftsIdx] of this.ftsIndexes) {
      const newVal = (withId as Record<string, unknown>)[field];
      if (typeof newVal === 'string') ftsIdx.index(doc._id, newVal);
      else if (ftsIdx.hasDoc(doc._id)) ftsIdx.remove(doc._id);
    }
    this._emitChange({
      operationType: 'update', documentKey: { _id: doc._id }, fullDocument: withId as T,
      updateDescription: { updatedFields: {}, removedFields: [] },
      timestamp: Date.now(), txId: 0n,
    });
    return true;
  }

  // ── Delete ────────────────────────────────────────────────

  async deleteOne(filter: QueryFilter): Promise<boolean> {
    validateQueryFilter(filter);
    const doc = await this.findOne(filter);
    if (!doc) return false;
    // FIX: storage DULU — jika delete gagal, index tidak tersentuh
    await this.engine.delete(doc._id);
    this.idxMgr.onDelete(doc as unknown as Record<string, unknown>);
    // v4.0: remove from FTS indexes
    for (const ftsIdx of this.ftsIndexes.values()) ftsIdx.remove(doc._id);
    this._emitChange({ operationType: 'delete', documentKey: { _id: doc._id }, timestamp: Date.now(), txId: 0n });
    return true;
  }

  async deleteMany(filter: QueryFilter = {}): Promise<number> {
    validateQueryFilter(filter);
    // FIX: kumpulkan _id dulu via streaming, baru delete — hindari load semua doc ke RAM
    const ids: string[] = [];
    for await (const doc of this._scanWithPlan(filter)) ids.push(doc._id);
    let count = 0;
    for (const id of ids) { if (await this.deleteOne({ _id: id })) count++; }
    return count;
  }

  async truncate(): Promise<void> {
    await this.engine.deleteAll();
    this.idxMgr.clearAll();
    this._emitChange({ operationType: 'drop', documentKey: { _id: '*' }, timestamp: Date.now(), txId: 0n });
  }

  // ── Atomic ops ────────────────────────────────────────────

  async findOneAndUpdate(filter: QueryFilter, spec: UpdateSpec): Promise<T | null> {
    const doc = await this.findOne(filter);
    if (!doc) return null;
    await this.updateOne({ _id: doc._id }, spec);
    return this.findOne({ _id: doc._id });
  }

  async findOneAndDelete(filter: QueryFilter): Promise<T | null> {
    const doc = await this.findOne(filter);
    if (!doc) return null;
    await this.deleteOne({ _id: doc._id });
    return doc;
  }

  async findOneAndReplace(filter: QueryFilter, replacement: Omit<T, '_id'>): Promise<T | null> {
    const doc = await this.findOne(filter);
    if (!doc) return null;
    const newDoc = { ...replacement, _id: doc._id } as T;
    await this.replaceOne({ _id: doc._id }, newDoc);
    return newDoc;
  }

  // ── BulkWrite ─────────────────────────────────────────────

  async bulkWrite(ops: BulkWriteOp<T>[], options: { ordered?: boolean } = {}): Promise<BulkWriteResult> {
    const ordered = options.ordered !== false;
    const result: BulkWriteResult = {
      ops: ops.length, insertedCount: 0, updatedCount: 0,
      deletedCount: 0, upsertedCount: 0, replacedCount: 0,
      insertedIds: [], errors: [],
    };

    for (let i = 0; i < ops.length; i++) {
      const op = ops[i]!;
      try {
        if (op.op === 'insertOne') {
          const ins = await this.insertOne(op.doc);
          result.insertedIds.push(ins._id); result.insertedCount++;
        } else if (op.op === 'updateOne') {
          if (await this.updateOne(op.filter, op.spec)) result.updatedCount++;
        } else if (op.op === 'updateMany') {
          result.updatedCount += await this.updateMany(op.filter, op.spec);
        } else if (op.op === 'deleteOne') {
          if (await this.deleteOne(op.filter)) result.deletedCount++;
        } else if (op.op === 'deleteMany') {
          result.deletedCount += await this.deleteMany(op.filter);
        } else if (op.op === 'upsertOne') {
          const before = await this.exists(op.filter);
          await this.upsertOne(op.filter, op.spec);
          if (!before) { result.upsertedCount++; result.insertedCount++; }
          else result.updatedCount++;
        } else if (op.op === 'replaceOne') {
          if (await this.replaceOne(op.filter, op.replacement as T)) result.replacedCount++;
        }
      } catch (err) {
        const errMsg = err instanceof Error ? err.message : String(err);
        result.errors.push({ index: i, error: errMsg });
        if (ordered) throw new Error(`[OvnDB] bulkWrite aborted at op[${i}] (${op.op}): ${errMsg}`);
      }
    }
    await this.flush();
    return result;
  }

  // ── Aggregation ───────────────────────────────────────────

  async aggregate(
    pipeline: AggregationStage[],
    lookupResolver?: (colName: string) => Promise<Record<string, unknown>[]>,
  ): Promise<Record<string, unknown>[]> {
    // SECURITY: batasi jumlah stage pipeline untuk mencegah DoS
    if (!Array.isArray(pipeline))
      throw new Error('[OvnDB] aggregate: pipeline harus array');
    if (pipeline.length > MAX_PIPELINE_STAGES)
      throw new Error(`[OvnDB] aggregate: pipeline terlalu banyak stage (maks ${MAX_PIPELINE_STAGES})`);
    const allDocs: Record<string, unknown>[] = [];
    for await (const [, buf] of this.engine.scan()) {
      const doc = this._parse(buf);
      if (doc) allDocs.push(doc as Record<string, unknown>);
    }
    const resolver = lookupResolver ?? (async (_: string) => []);
    return compilePipeline(pipeline, resolver)(allDocs);
  }

  // ── Explain ───────────────────────────────────────────────

  explain(filter: QueryFilter, options?: QueryOptions): QueryPlan {
    return this.planner.plan(filter, options);
  }

  // ── Change Stream ─────────────────────────────────────────

  watch(opts?: import('./change-stream.js').WatchOptions): import('./change-stream.js').ChangeStream<T> {
    return this.streams.create(opts);
  }

  // ── Maintenance ───────────────────────────────────────────

  async compact(): Promise<void>       { await this.engine.forceCompact(); }
  async flush(): Promise<void>         { await this.engine.flush(); }
  async stats(): Promise<OvnStats>     { return this.engine.stats(this.name); }
  beginBulkLoad(): void                { this.engine.beginBulkLoad(); }
  async endBulkLoad(): Promise<void>   { await this.engine.endBulkLoad(); }

  // ── Import / Export (v4.0) ───────────────────────────────

  /**
   * v4.0: Export collection to a file.
   * Default format: NDJSON (streaming, one doc per line).
   *
   * @example
   *   await col.exportTo('./backup.ndjson');
   *   await col.exportTo('./backup.json', { format: 'json' });
   */
  async exportTo(filePath: string, opts?: ExportOptions): Promise<number> {
    return _exportTo(this, filePath, opts);
  }

  /**
   * v4.0: Import documents from a file into this collection.
   * Default format: NDJSON (auto-detected from extension).
   *
   * @example
   *   const result = await col.importFrom('./backup.ndjson');
   *   console.log(`Imported ${result.inserted} docs`);
   */
  async importFrom(filePath: string, opts?: ImportOptions): Promise<ImportResult> {
    return _importFrom(this, filePath, opts);
  }

  // ── Internal ──────────────────────────────────────────────

  protected async *_scanWithPlan(
    filter: QueryFilter,
    options?: QueryOptions,
    stats?: ExecutionStats,
  ): AsyncGenerator<T> {
    const plan    = this.planner.plan(filter, options);
    const afterId = options?.after;
    let   skip    = options?.skip ?? 0;
    let   limit   = options?.limit ?? Infinity;
    let   emitted = 0;
    let   skipped = 0;

    if (stats) { stats.planType = plan.planType; stats.indexUsed = plan.indexField ?? null; }

    // v4.0: $text query — use FTS index if available
    if (filter['$text'] && typeof filter['$text'] === 'string') {
      // Find which FTS index to use (first registered)
      for (const [field, ftsIdx] of this.ftsIndexes) {
        const matchingIds = ftsIdx.search(filter['$text']);
        if (stats) { stats.planType = 'indexScan'; stats.indexUsed = `fts:${field}`; }
        const remainingFilter = { ...filter };
        delete remainingFilter['$text'];
        for (const id of matchingIds) {
          if (emitted >= limit) break;
          const buf = await this.engine.read(id);
          if (stats) stats.totalDocsScanned++;
          if (!buf) continue;
          const doc = this._parse(buf);
          if (!doc) continue;
          if (Object.keys(remainingFilter).length > 0 && !matchFilter(doc as unknown as Record<string, unknown>, remainingFilter)) continue;
          if (afterId && id <= afterId) continue;
          if (skipped < skip) { skipped++; continue; }
          if (stats) stats.nReturned++;
          yield doc;
          emitted++;
        }
        return;
      }
    }

    if (plan.planType === 'primaryKey' && filter['_id'] !== undefined) {
      const buf = await this.engine.read(filter['_id'] as string);
      if (stats) { stats.totalDocsScanned++; stats.totalKeysScanned++; }
      if (buf) {
        const doc = this._parse(buf);
        if (doc && matchFilter(doc as unknown as Record<string, unknown>, filter)) {
          if (stats) stats.nReturned++;
          yield doc;
        }
      }
      return;
    }

    if (plan.planType === 'indexScan' && plan.indexField) {
      const condition = filter[plan.indexField];
      let ids: string[] | null = null;

      // v4.0: check for compound index first
      const compoundDef = this._findCompoundIndexFor(filter);
      if (compoundDef && Array.isArray(compoundDef.field)) {
        const fields = compoundDef.field;
        ids = this.idxMgr.lookupCompound(fields, filter as Record<string, unknown>);
        if (stats) { stats.planType = 'indexScan'; stats.indexUsed = fields.join('__'); }
      } else if (condition !== null && condition !== undefined && typeof condition !== 'object') {
        ids = this.idxMgr.lookup(plan.indexField, condition);
      } else if (condition !== null && condition !== undefined && typeof condition === 'object') {
        const ops = condition as Record<string, unknown>;
        if ('$eq' in ops) ids = this.idxMgr.lookup(plan.indexField, ops.$eq);
        else if ('$gte' in ops || '$lte' in ops) {
          ids = this.idxMgr.lookupRange(
            plan.indexField,
            ops.$gte !== undefined ? String(ops.$gte) : undefined,
            ops.$lte !== undefined ? String(ops.$lte) : undefined,
          );
        }
      }

      if (ids !== null) {
        if (stats) stats.totalKeysScanned += ids.length;
        for (const id of ids) {
          if (emitted >= limit) break;
          const buf = await this.engine.read(id);
          if (stats) stats.totalDocsScanned++;
          if (!buf) continue;
          const doc = this._parse(buf);
          if (!doc || !matchFilter(doc as unknown as Record<string, unknown>, filter)) continue;
          if (afterId && id <= afterId) continue;
          if (skipped < skip) { skipped++; continue; }
          if (stats) stats.nReturned++;
          yield doc;
          emitted++;
        }
        return;
      }
    }

    // Full collection scan
    for await (const [id, buf] of this.engine.scan()) {
      if (emitted >= limit) break;
      if (afterId && id <= afterId) continue;
      if (stats) stats.totalDocsScanned++;
      const doc = this._parse(buf);
      if (!doc || !matchFilter(doc as unknown as Record<string, unknown>, filter)) continue;
      if (skipped < skip) { skipped++; continue; }
      if (stats) stats.nReturned++;
      yield doc;
      emitted++;
    }
  }

  /** v4.0: Find a compound index definition that covers all filter fields */
  private _findCompoundIndexFor(filter: QueryFilter): IndexDefinition | null {
    for (const def of this.idxMgr.getIndexDefs()) {
      if (!Array.isArray(def.field)) continue;
      const fields = def.field;
      // Check if ALL compound fields are present as exact-match conditions in filter
      if (fields.every(f => {
        const val = filter[f];
        return val !== undefined && val !== null && typeof val !== 'object';
      })) {
        return def;
      }
    }
    return null;
  }

  protected _parse(buf: Buffer): T | null {
    try {
      return JSON.parse(buf.toString('utf8'), (_, v) => {
        if (v && typeof v === 'object' && typeof v.$bigint === 'string') return BigInt(v.$bigint);
        return v;
      }) as T;
    } catch { return null; }
  }

  protected _serialize(doc: T): Buffer {
    return Buffer.from(JSON.stringify(doc, (_, v) => typeof v === 'bigint' ? { $bigint: v.toString() } : v), 'utf8');
  }

  private _emitChange(event: ChangeEvent<T>): void {
    if (this.streams.count > 0) this.streams.emit(event);
  }
}
