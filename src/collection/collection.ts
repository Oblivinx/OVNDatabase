// ============================================================
//  OvnDB v2.1 — Collection
//
//  update: added truncate(), exists(), findById(), findManyById(),
//          bulkWrite(), compact(), improved upsertOne ($setOnInsert),
//          fixed aggregate() scan path, _serialize() override hook,
//          better error context, ordered/unordered bulkWrite
// ============================================================

import { generateId }       from '../utils/id-generator.js';
import { matchFilter, applyUpdate, applyProjection } from '../core/query/filter.js';
import { QueryPlanner }     from '../core/query/planner.js';
import { compilePipeline }  from '../core/query/aggregation.js';
import { ChangeStreamRegistry } from './change-stream.js';
import { SecondaryIndexManager } from '../core/index/secondary-index.js';
import type { StorageEngine }  from '../core/storage/storage-engine.js';
import type {
  OvnDocument, QueryFilter, QueryOptions, UpdateSpec,
  AggregationStage, OvnStats, IndexDefinition, QueryPlan,
  ChangeEvent, BulkWriteOp, BulkWriteResult,
} from '../types/index.js';

export class Collection<T extends OvnDocument = OvnDocument> {
  readonly name:    string;
  protected readonly engine:   StorageEngine;
  protected readonly indexes:  Map<string, IndexDefinition> = new Map();
  protected readonly idxMgr:  SecondaryIndexManager;
  protected readonly streams: ChangeStreamRegistry<T>;
  private   readonly planner: QueryPlanner;

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
    if (this.indexes.has(def.field)) return;
    this.indexes.set(def.field, def);
    this.idxMgr.addIndex(def);
    await this.idxMgr.open();
    // rebuild index dari data yang sudah ada
    // update: scan dengan decryptFn agar encrypted collections work
    const allDocs: Record<string, unknown>[] = [];
    for await (const [, buf] of this.engine.scan()) {
      try { allDocs.push(this._parse(buf) as Record<string, unknown> ?? {}); } catch { /* skip */ }
    }
    if (allDocs.length > 0) this.idxMgr.rebuildFromDocs(allDocs);
    await this.idxMgr.save();
  }

  async dropIndex(field: string): Promise<void> {
    this.indexes.delete(field);
  }

  // ── Insert ────────────────────────────────────────────────

  async insertOne(doc: Omit<T, '_id'> & { _id?: string }): Promise<T> {
    const full = { ...doc, _id: doc._id ?? generateId() } as T;
    const buf  = this._serialize(full);
    this.idxMgr.onInsert(full as unknown as Record<string, unknown>);
    await this.engine.insert(full._id, buf);
    this._emitChange({ operationType: 'insert', documentKey: { _id: full._id }, fullDocument: full, timestamp: Date.now(), txId: 0n });
    return full;
  }

  async insertMany(docs: Array<Omit<T, '_id'> & { _id?: string }>): Promise<T[]> {
    const results: T[] = [];
    for (const doc of docs) {
      results.push(await this.insertOne(doc));
    }
    await this.flush();
    return results;
  }

  // ── Find ──────────────────────────────────────────────────

  async findOne(filter: QueryFilter, options?: Pick<QueryOptions, 'projection'>): Promise<T | null> {
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

  /** feat: shortcut for findOne by _id */
  async findById(id: string): Promise<T | null> {
    const buf = await this.engine.read(id);
    if (!buf) return null;
    return this._parse(buf);
  }

  /** feat: batch read multiple _ids in one call — deduplicates & skips missing */
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
    if (options.explain) {
      const plan = this.explain(filter, options);
      return [plan as unknown as T];
    }

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

  async countDocuments(filter: QueryFilter = {}): Promise<number> {
    let count = 0;
    for await (const _ of this._scanWithPlan(filter)) count++;
    return count;
  }

  async distinct(field: string, filter: QueryFilter = {}): Promise<unknown[]> {
    const values = new Set<string>();
    const docs   = await this.find(filter);
    for (const doc of docs) {
      const val = (doc as Record<string, unknown>)[field];
      if (val !== undefined) values.add(JSON.stringify(val));
    }
    return [...values].map(v => JSON.parse(v));
  }

  /** feat: quick existence check without loading full document */
  async exists(filter: QueryFilter): Promise<boolean> {
    for await (const _ of this._scanWithPlan(filter, { limit: 1 })) return true;
    return false;
  }

  // ── Update ────────────────────────────────────────────────

  async updateOne(filter: QueryFilter, spec: UpdateSpec): Promise<boolean> {
    const doc = await this.findOne(filter);
    if (!doc) return false;
    const before  = { ...doc };
    const updated = applyUpdate(doc as unknown as Record<string, unknown>, spec);
    updated['_id'] = doc._id;
    const buf = this._serialize(updated as T);
    this.idxMgr.onUpdate(before as unknown as Record<string, unknown>, updated);
    await this.engine.update(doc._id, buf);
    this._emitChange({
      operationType: 'update',
      documentKey:   { _id: doc._id },
      fullDocument:  updated as T,
      updateDescription: { updatedFields: spec.$set ?? {}, removedFields: Object.keys(spec.$unset ?? {}) },
      timestamp: Date.now(), txId: 0n,
    });
    return true;
  }

  async updateMany(filter: QueryFilter, spec: UpdateSpec): Promise<number> {
    const docs = await this.find(filter);
    let count  = 0;
    for (const doc of docs) {
      if (await this.updateOne({ _id: doc._id }, spec)) count++;
    }
    return count;
  }

  /**
   * Upsert: insert jika tidak ada, update jika ada.
   * update: mendukung $setOnInsert — hanya di-apply saat dokumen baru dibuat
   */
  async upsertOne(filter: QueryFilter, spec: UpdateSpec): Promise<T> {
    const existing = await this.findOne(filter);
    if (!existing) {
      // fix: gabungkan $set dan $setOnInsert saat insert
      const insertFields = {
        ...(spec.$set ?? {}),
        ...(spec.$setOnInsert ?? {}),
        _id: (filter['_id'] as string) ?? generateId(),
      };
      return this.insertOne(insertFields as Omit<T, '_id'> & { _id?: string });
    }
    // update: strip $setOnInsert saat update (tidak dipakai)
    const updateSpec: UpdateSpec = { ...spec };
    delete updateSpec.$setOnInsert;
    await this.updateOne({ _id: existing._id }, updateSpec);
    return (await this.findOne({ _id: existing._id }))!;
  }

  async replaceOne(filter: QueryFilter, replacement: T): Promise<boolean> {
    const doc = await this.findOne(filter);
    if (!doc) return false;
    const withId = { ...replacement, _id: doc._id };
    const buf    = this._serialize(withId as T);
    this.idxMgr.onUpdate(doc as unknown as Record<string, unknown>, withId as unknown as Record<string, unknown>);
    await this.engine.update(doc._id, buf);
    return true;
  }

  // ── Delete ────────────────────────────────────────────────

  async deleteOne(filter: QueryFilter): Promise<boolean> {
    const doc = await this.findOne(filter);
    if (!doc) return false;
    this.idxMgr.onDelete(doc as unknown as Record<string, unknown>);
    await this.engine.delete(doc._id);
    this._emitChange({ operationType: 'delete', documentKey: { _id: doc._id }, timestamp: Date.now(), txId: 0n });
    return true;
  }

  async deleteMany(filter: QueryFilter = {}): Promise<number> {
    const docs = await this.find(filter);
    let count  = 0;
    for (const doc of docs) {
      if (await this.deleteOne({ _id: doc._id })) count++;
    }
    return count;
  }

  /**
   * feat: hapus SEMUA dokumen dalam collection (jauh lebih efisien dari deleteMany({}))
   * Menggunakan engine.deleteAll() untuk skip per-doc index overhead.
   */
  async truncate(): Promise<void> {
    await this.engine.deleteAll();
    this.idxMgr.clearAll();
    this._emitChange({ operationType: 'drop', documentKey: { _id: '*' }, timestamp: Date.now(), txId: 0n });
  }

  // ── Atomic ops ────────────────────────────────────────────

  async findOneAndUpdate(filter: QueryFilter, spec: UpdateSpec): Promise<T | null> {
    const updated = await this.updateOne(filter, spec);
    if (!updated) return null;
    return this.findOne(filter);
  }

  async findOneAndDelete(filter: QueryFilter): Promise<T | null> {
    const doc = await this.findOne(filter);
    if (!doc) return null;
    await this.deleteOne({ _id: doc._id });
    return doc;
  }

  /**
   * feat: findOneAndReplace — atomic find, replace, return new doc
   */
  async findOneAndReplace(filter: QueryFilter, replacement: Omit<T, '_id'>): Promise<T | null> {
    const doc = await this.findOne(filter);
    if (!doc) return null;
    const newDoc = { ...replacement, _id: doc._id } as T;
    await this.replaceOne({ _id: doc._id }, newDoc);
    return newDoc;
  }

  // ── BulkWrite ─────────────────────────────────────────────

  /**
   * feat: bulkWrite — batch operasi mixed dalam satu call.
   * Mendukung insertOne, updateOne, updateMany, deleteOne, deleteMany, upsertOne, replaceOne.
   *
   * @param ops     Array operasi yang akan dieksekusi
   * @param ordered Jika true (default), hentikan saat ada error.
   *                Jika false, lanjutkan dan kumpulkan errors.
   *
   * @example
   *   await col.bulkWrite([
   *     { op: 'insertOne', doc: { name: 'Budi' } },
   *     { op: 'updateOne', filter: { status: 'draft' }, spec: { $set: { status: 'active' } } },
   *     { op: 'deleteMany', filter: { expired: true } },
   *   ]);
   */
  async bulkWrite(
    ops: BulkWriteOp<T>[],
    options: { ordered?: boolean } = {},
  ): Promise<BulkWriteResult> {
    const ordered = options.ordered !== false; // default true
    const result: BulkWriteResult = {
      ops: ops.length,
      insertedCount: 0,
      updatedCount:  0,
      deletedCount:  0,
      upsertedCount: 0,
      replacedCount: 0,
      insertedIds:   [],
      errors:        [],
    };

    for (let i = 0; i < ops.length; i++) {
      const op = ops[i]!;
      try {
        if (op.op === 'insertOne') {
          const inserted = await this.insertOne(op.doc);
          result.insertedIds.push(inserted._id);
          result.insertedCount++;
        } else if (op.op === 'updateOne') {
          const ok = await this.updateOne(op.filter, op.spec);
          if (ok) result.updatedCount++;
        } else if (op.op === 'updateMany') {
          const n = await this.updateMany(op.filter, op.spec);
          result.updatedCount += n;
        } else if (op.op === 'deleteOne') {
          const ok = await this.deleteOne(op.filter);
          if (ok) result.deletedCount++;
        } else if (op.op === 'deleteMany') {
          const n = await this.deleteMany(op.filter);
          result.deletedCount += n;
        } else if (op.op === 'upsertOne') {
          const before = await this.exists(op.filter);
          await this.upsertOne(op.filter, op.spec);
          if (!before) { result.upsertedCount++; result.insertedCount++; }
          else result.updatedCount++;
        } else if (op.op === 'replaceOne') {
          const ok = await this.replaceOne(op.filter, op.replacement as T);
          if (ok) result.replacedCount++;
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

  /**
   * update: aggregate sekarang menggunakan _scanWithPlan agar enkripsi
   * dan semua overrides berfungsi. lookupResolver bisa di-inject untuk
   * cross-collection $lookup.
   */
  async aggregate(
    pipeline: AggregationStage[],
    lookupResolver?: (colName: string) => Promise<Record<string, unknown>[]>,
  ): Promise<Record<string, unknown>[]> {
    // fix: gunakan engine.scan() yang sudah ada decryptFn-nya
    const allDocs: Record<string, unknown>[] = [];
    for await (const [, buf] of this.engine.scan()) {
      const doc = this._parse(buf);
      if (doc) allDocs.push(doc as Record<string, unknown>);
    }

    const resolver = lookupResolver ?? (async (_: string) => []);
    const exec = compilePipeline(pipeline, resolver);
    return exec(allDocs);
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

  /** feat: trigger manual compaction untuk collection ini */
  async compact(): Promise<void> {
    await this.engine.forceCompact();
  }

  // ── Utilities ─────────────────────────────────────────────

  async flush(): Promise<void>       { await this.engine.flush(); }
  async stats(): Promise<OvnStats>   { return this.engine.stats(this.name); }

  beginBulkLoad(): void              { this.engine.beginBulkLoad(); }
  async endBulkLoad(): Promise<void> { await this.engine.endBulkLoad(); }

  // ── Internal ──────────────────────────────────────────────

  protected async *_scanWithPlan(filter: QueryFilter, options?: QueryOptions): AsyncGenerator<T> {
    const plan = this.planner.plan(filter, options);

    const afterId = options?.after;
    let   skip    = options?.skip ?? 0;
    let   limit   = options?.limit ?? Infinity;
    let   emitted = 0;
    let   skipped = 0;

    if (plan.planType === 'primaryKey' && filter['_id'] !== undefined) {
      const buf = await this.engine.read(filter['_id'] as string);
      if (buf) {
        const doc = this._parse(buf);
        if (doc && matchFilter(doc as unknown as Record<string, unknown>, filter)) {
          yield doc;
        }
      }
      return;
    }

    if (plan.planType === 'indexScan' && plan.indexField) {
      const condition = filter[plan.indexField!];
      let ids: string[] | null = null;

      if (condition !== null && typeof condition !== 'object') {
        ids = this.idxMgr.lookup(plan.indexField!, condition);
      } else if (condition !== null && typeof condition === 'object') {
        const ops = condition as Record<string, unknown>;
        if ('$eq' in ops) ids = this.idxMgr.lookup(plan.indexField!, ops.$eq);
        else if ('$gte' in ops || '$lte' in ops) {
          ids = this.idxMgr.lookupRange(
            plan.indexField!,
            ops.$gte !== undefined ? String(ops.$gte) : undefined,
            ops.$lte !== undefined ? String(ops.$lte) : undefined,
          );
        }
      }

      if (ids !== null) {
        for (const id of ids) {
          if (emitted >= limit) break;
          const buf = await this.engine.read(id);
          if (!buf) continue;
          const doc = this._parse(buf);
          if (!doc || !matchFilter(doc as unknown as Record<string, unknown>, filter)) continue;
          if (afterId && id <= afterId) continue;
          if (skipped < skip) { skipped++; continue; }
          yield doc;
          emitted++;
        }
        return;
      }
    }

    // Full collection scan (fallback)
    for await (const [id, buf] of this.engine.scan()) {
      if (emitted >= limit) break;
      if (afterId && id <= afterId) continue;
      const doc = this._parse(buf);
      if (!doc || !matchFilter(doc as unknown as Record<string, unknown>, filter)) continue;
      if (skipped < skip) { skipped++; continue; }
      yield doc;
      emitted++;
    }
  }

  /**
   * update: _parse adalah override point untuk CollectionV2 (dekripsi).
   * Selalu gunakan _parse, bukan JSON.parse langsung.
   */
  protected _parse(buf: Buffer): T | null {
    try { return JSON.parse(buf.toString('utf8')) as T; }
    catch { return null; }
  }

  /**
   * feat: _serialize adalah override point untuk CollectionV2 (enkripsi).
   * Semua write ops menggunakan _serialize agar subclass bisa intercept.
   */
  protected _serialize(doc: T): Buffer {
    return Buffer.from(JSON.stringify(doc), 'utf8');
  }

  private _emitChange(event: ChangeEvent<T>): void {
    if (this.streams.count > 0) this.streams.emit(event);
  }
}
