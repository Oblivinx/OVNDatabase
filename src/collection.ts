// ============================================================
//  Collection<T> — MongoDB-style query API
// ============================================================
import { generateId }    from './utils/id-generator.js';
import { StorageEngine } from './core/storage-engine.js';
import type { OvnDocument, QueryFilter, QueryOptions, UpdateSpec, OvnStats, Scalar } from './types.js';

// ── TopK min-heap — O(n log k) sort+limit without loading all N docs ─────────
class TopKHeap<T extends OvnDocument> {
  private readonly heap: T[] = [];
  private readonly k:    number;
  private readonly sort: Record<string, 1 | -1>;

  constructor(k: number, sort: Record<string, 1 | -1>) { this.k = k; this.sort = sort; }

  push(doc: T): void {
    if (this.heap.length < this.k) {
      this.heap.push(doc);
      this._siftUp(this.heap.length - 1);
    } else if (this.heap.length > 0 && this._beats(doc, this.heap[0]!)) {
      this.heap[0] = doc;
      this._siftDown(0);
    }
  }

  toSortedArray(): T[] {
    // Sort using _cmpDocs directly — dir is already encoded in the comparator
    return [...this.heap].sort((a, b) => this._cmpDocs(a, b));
  }

  private _beats(a: T, b: T): boolean { return this._cmpDocs(a, b) > 0; }
  private _cmpDocs(a: T, b: T): number {
    for (const [f, dir] of Object.entries(this.sort)) {
      const c = this._cmp(this._get(a, f), this._get(b, f)) * dir;
      if (c !== 0) return c;
    }
    return 0;
  }
  private _cmp(a: unknown, b: unknown): number {
    if (typeof a === 'number' && typeof b === 'number') return a - b;
    const sa = String(a), sb = String(b);
    return sa < sb ? -1 : sa > sb ? 1 : 0;
  }
  private _get(doc: T, path: string): unknown {
    return path.split('.').reduce<unknown>((o, k) =>
      o !== null && typeof o === 'object' ? (o as Record<string,unknown>)[k] : undefined, doc);
  }
  private _siftUp(i: number): void {
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (this._beats(this.heap[p]!, this.heap[i]!)) break;
      [this.heap[i], this.heap[p]] = [this.heap[p]!, this.heap[i]!];
      i = p;
    }
  }
  private _siftDown(i: number): void {
    const n = this.heap.length;
    while (true) {
      let min = i;
      const l = 2*i+1, r = 2*i+2;
      if (l < n && !this._beats(this.heap[l]!, this.heap[min]!)) min = l;
      if (r < n && !this._beats(this.heap[r]!, this.heap[min]!)) min = r;
      if (min === i) break;
      [this.heap[i], this.heap[min]] = [this.heap[min]!, this.heap[i]!];
      i = min;
    }
  }
}

// ── Collection ────────────────────────────────────────────────────────────────
export class Collection<T extends OvnDocument = OvnDocument> {
  constructor(
    protected readonly _colName:   string,
    protected readonly _engine: StorageEngine,
  ) {}

  // ── Insert ─────────────────────────────────────────────────

  async insertOne(doc: Omit<T, '_id'> & { _id?: string }): Promise<T> {
    const _id  = doc._id ?? generateId();
    const full = { ...doc, _id } as T;
    await this._engine.insert(_id, Buffer.from(JSON.stringify(full), 'utf8'));
    return full;
  }

  async insertMany(docs: Array<Omit<T, '_id'> & { _id?: string }>): Promise<T[]> {
    const results: T[] = [];
    for (const doc of docs) {
      const _id  = doc._id ?? generateId();
      const full = { ...doc, _id } as T;
      await this._engine.upsert(_id, Buffer.from(JSON.stringify(full), 'utf8'));
      results.push(full);
    }
    return results;
  }

  // ── Find ───────────────────────────────────────────────────

  async findOne(filter: QueryFilter): Promise<T | null> {
    if (this._isIdOnlyFilter(filter)) {
      const raw = await this._engine.read(filter['_id'] as string);
      if (!raw) return null;
      return JSON.parse(raw.toString('utf8')) as T;
    }
    for await (const doc of this._scan(filter)) return doc;
    return null;
  }

  async find(filter: QueryFilter = {}, opts: QueryOptions = {}): Promise<T[]> {
    const { skip = 0, limit, sort, projection } = opts;

    if (sort && limit) {
      const heap = new TopKHeap<T>(limit, sort);
      for await (const doc of this._scan(filter)) heap.push(doc);
      let res = heap.toSortedArray().slice(skip);
      if (projection) res = res.map(d => this._project(d, projection) as T);
      return res;
    }

    let results: T[] = [];
    let skipped = 0;
    for await (const doc of this._scan(filter)) {
      if (skipped < skip) { skipped++; continue; }
      results.push(doc);
      if (!sort && limit && results.length >= limit) break;
    }
    if (sort) { results = this._sortDocs(results, sort); if (limit) results = results.slice(0, limit); }
    if (projection) results = results.map(d => this._project(d, projection) as T);
    return results;
  }

  async count(filter: QueryFilter = {}): Promise<number> {
    let n = 0;
    for await (const _ of this._scan(filter)) n++;
    return n;
  }

  async exists(filter: QueryFilter): Promise<boolean> {
    return (await this.findOne(filter)) !== null;
  }

  // ── Update ─────────────────────────────────────────────────

  async updateOne(filter: QueryFilter, update: UpdateSpec): Promise<boolean> {
    const doc = await this.findOne(filter);
    if (!doc) return false;
    const updated = this._applyUpdate(doc, update);
    await this._engine.update(doc._id, Buffer.from(JSON.stringify(updated), 'utf8'));
    return true;
  }

  async updateMany(filter: QueryFilter, update: UpdateSpec): Promise<number> {
    const docs = await this.find(filter);
    for (const doc of docs) {
      const updated = this._applyUpdate(doc, update);
      await this._engine.update(doc._id, Buffer.from(JSON.stringify(updated), 'utf8'));
    }
    return docs.length;
  }

  async upsertOne(filter: QueryFilter, update: UpdateSpec | Partial<T>): Promise<T> {
    const isOp     = Object.keys(update).some(k => k.startsWith('$'));
    const existing = await this.findOne(filter);
    if (existing) {
      const spec: UpdateSpec = isOp ? (update as UpdateSpec) : { $set: update as Record<string, unknown> };
      await this.updateOne({ _id: existing._id }, spec);
      return (await this.findOne({ _id: existing._id }))!;
    }
    const setFields = isOp ? ((update as UpdateSpec).$set ?? {}) : (update as Record<string, unknown>);
    return this.insertOne({ ...filter, ...setFields } as Omit<T, '_id'> & { _id?: string });
  }

  // ── Delete ─────────────────────────────────────────────────

  async deleteOne(filter: QueryFilter): Promise<boolean> {
    const doc = await this.findOne(filter);
    if (!doc) return false;
    return this._engine.delete(doc._id);
  }

  async deleteMany(filter: QueryFilter): Promise<number> {
    const docs = await this.find(filter);
    for (const doc of docs) await this._engine.delete(doc._id);
    return docs.length;
  }

  /**
   * Replace seluruh dokumen dengan dokumen baru.
   * _id harus sama. Digunakan terutama oleh Transaction rollback.
   */
  async replaceOne(filter: QueryFilter, replacement: T): Promise<boolean> {
    const existing = await this.findOne(filter);
    if (!existing) return false;
    const full = { ...replacement, _id: existing._id } as T;
    await this._engine.update(existing._id, Buffer.from(JSON.stringify(full), 'utf8'));
    return true;
  }

  // ── Maintenance ────────────────────────────────────────────

  async compact(): Promise<void>            { return this._engine.compact(); }
  async flush():   Promise<void>            { return this._engine.flush(); }
  async stats():   Promise<OvnStats>        { return this._engine.stats(this._colName); }

  // ── Internal ──────────────────────────────────────────────

  protected async *_scan(filter: QueryFilter): AsyncIterableIterator<T> {
    for await (const [, raw] of this._engine.scan()) {
      const doc = JSON.parse(raw.toString('utf8')) as T;
      if (this._matches(doc, filter)) yield doc;
    }
  }

  protected _isIdOnlyFilter(f: QueryFilter): boolean {
    const keys = Object.keys(f);
    return keys.length === 1 && keys[0] === '_id' && typeof f['_id'] === 'string';
  }

  _matches(doc: Record<string, unknown>, filter: QueryFilter): boolean {
    for (const [key, condition] of Object.entries(filter)) {
      if (key === '$and') {
        if (!(condition as QueryFilter[]).every(f => this._matches(doc, f))) return false;
        continue;
      }
      if (key === '$or') {
        if (!(condition as QueryFilter[]).some(f => this._matches(doc, f))) return false;
        continue;
      }
      if (key === '$not') {
        if (this._matches(doc, condition as QueryFilter)) return false;
        continue;
      }
      const fieldVal = this._getField(doc, key);
      if (condition !== null && typeof condition === 'object' && !Array.isArray(condition)) {
        if (!this._evalOps(fieldVal, condition as Record<string, unknown>)) return false;
      } else {
        if (!this._eq(fieldVal, condition)) return false;
      }
    }
    return true;
  }

  private _evalOps(val: unknown, ops: Record<string, unknown>): boolean {
    for (const [op, operand] of Object.entries(ops)) {
      switch (op) {
        case '$eq':     if (!this._eq(val, operand))                            return false; break;
        case '$ne':     if  (this._eq(val, operand))                            return false; break;
        case '$gt':     if (!(this._cmp(val, operand as Scalar) >  0))          return false; break;
        case '$gte':    if (!(this._cmp(val, operand as Scalar) >= 0))          return false; break;
        case '$lt':     if (!(this._cmp(val, operand as Scalar) <  0))          return false; break;
        case '$lte':    if (!(this._cmp(val, operand as Scalar) <= 0))          return false; break;
        case '$in':     if (!(operand as unknown[]).some(v => this._eq(val, v))) return false; break;
        case '$nin':    if  ((operand as unknown[]).some(v => this._eq(val, v))) return false; break;
        case '$exists': if  (Boolean(operand) !== (val !== undefined))           return false; break;
        case '$regex': {
          const re = operand instanceof RegExp ? operand : new RegExp(operand as string);
          if (typeof val !== 'string' || !re.test(val)) return false;
          break;
        }
      }
    }
    return true;
  }

  private _eq(a: unknown, b: unknown): boolean {
    if (a === b) return true;
    if (typeof a !== typeof b) return false;
    if (Array.isArray(a) && Array.isArray(b))
      return a.length === b.length && a.every((v, i) => this._eq(v, b[i]));
    return false;
  }

  private _cmp(a: unknown, b: Scalar): number {
    if (typeof a === 'number' && typeof b === 'number') return a - b;
    const sa = String(a), sb = String(b);
    return sa < sb ? -1 : sa > sb ? 1 : 0;
  }

  protected _getField(doc: Record<string, unknown>, path: string): unknown {
    return path.split('.').reduce<unknown>((obj, k) =>
      obj !== null && typeof obj === 'object' ? (obj as Record<string,unknown>)[k] : undefined, doc);
  }

  protected _applyUpdate(doc: T, spec: UpdateSpec): T {
    const out = Object.assign(Object.create(null), doc) as Record<string, unknown>;
    if (spec.$set)    for (const [k, v] of Object.entries(spec.$set))   this._setField(out, k, v);
    if (spec.$unset)  for (const k of Object.keys(spec.$unset))          this._unsetField(out, k);
    if (spec.$inc)    for (const [k, n] of Object.entries(spec.$inc))    this._setField(out, k, ((this._getField(out, k) as number) ?? 0) + n);
    if (spec.$push)   for (const [k, v] of Object.entries(spec.$push))   { const a = ([...(this._getField(out, k) as unknown[] ?? [])]); a.push(v); this._setField(out, k, a); }
    if (spec.$pull)   for (const [k, v] of Object.entries(spec.$pull))   this._setField(out, k, ((this._getField(out, k) as unknown[]) ?? []).filter(item => !this._eq(item, v)));
    if (spec.$rename) for (const [ok, nk] of Object.entries(spec.$rename)) { this._setField(out, nk, this._getField(out, ok)); this._unsetField(out, ok); }
    return out as T;
  }

  private _setField(obj: Record<string, unknown>, path: string, val: unknown): void {
    const parts = path.split('.');
    let cur = obj;
    for (let i = 0; i < parts.length - 1; i++) {
      if (typeof cur[parts[i]!] !== 'object' || cur[parts[i]!] === null) cur[parts[i]!] = {};
      cur = cur[parts[i]!] as Record<string, unknown>;
    }
    cur[parts[parts.length - 1]!] = val;
  }

  private _unsetField(obj: Record<string, unknown>, path: string): void {
    const parts = path.split('.');
    let cur = obj;
    for (let i = 0; i < parts.length - 1; i++) {
      if (typeof cur[parts[i]!] !== 'object') return;
      cur = cur[parts[i]!] as Record<string, unknown>;
    }
    delete cur[parts[parts.length - 1]!];
  }

  protected _sortDocs(docs: T[], sort: Record<string, 1 | -1>): T[] {
    return [...docs].sort((a, b) => {
      for (const [field, dir] of Object.entries(sort)) {
        const c = this._cmp(this._getField(a as Record<string,unknown>, field),
                            this._getField(b as Record<string,unknown>, field) as Scalar);
        if (c !== 0) return c * dir;
      }
      return 0;
    });
  }

  protected _project(doc: T, proj: Record<string, 0 | 1>): Partial<T> {
    const hasInclusion = Object.values(proj).some(v => v === 1);
    const out: Record<string, unknown> = hasInclusion ? { _id: doc._id } : { ...doc };
    for (const [k, v] of Object.entries(proj)) {
      if (k === '_id') { if (v === 0) delete out._id; continue; }
      if (v === 1) out[k] = this._getField(doc as Record<string,unknown>, k);
      else         delete out[k];
    }
    return out as Partial<T>;
  }
}