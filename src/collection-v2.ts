// ============================================================
//  CollectionV2<T> — Drop-in replacement for Collection<T>
//  Adds: optional AES-256-GCM encryption + secondary indexes
//
//  Usage:
//    const users = new CollectionV2('users', engine, dataDir, {
//      crypto:  cryptoFromPassphrase(process.env.KEY!, dataDir),
//      indexes: [{ field: 'phone', unique: true }],
//    });
//    await users.init();
// ============================================================
import { Collection }            from './collection.js';
import { StorageEngine }         from './core/storage-engine.js';
import { SecondaryIndexManager, type IndexDefinition } from './core/secondary-index.js';
import type { CryptoLayer }      from './crypto/crypto-layer.js';
import { generateId }            from './utils/id-generator.js';
import { makeLogger }            from './utils/logger.js';
import type { OvnDocument, QueryFilter, QueryOptions, UpdateSpec } from './types.js';
import { SchemaValidator, type SchemaDefinition } from './core/schema-validator.js';

const log = makeLogger('collection-v2');

export interface CollectionV2Options {
  /** Transparent AES-256-GCM encryption for every record */
  crypto?:  CryptoLayer;
  /** Secondary field indexes */
  indexes?: IndexDefinition[];
  /**
   * Schema validation — validasi dokumen sebelum masuk ke storage.
   * Gunakan field.string(), field.number(), dll dari schema-validator.
   */
  schema?:  SchemaDefinition;
  /**
   * Mode validasi schema:
   *   'strict' (default) — throw ValidationError jika tidak valid
   *   'warn'             — log warning tapi tetap insert
   */
  schemaMode?: 'strict' | 'warn';
}

export class CollectionV2<T extends OvnDocument = OvnDocument> extends Collection<T> {
  private readonly secIdx:     SecondaryIndexManager;
  private readonly crypto?:    CryptoLayer;
  private readonly _dirPath:   string;
  private readonly _validator?: SchemaValidator;
  private readonly _schemaMode: 'strict' | 'warn';

  constructor(
    name:    string,
    engine:  StorageEngine,
    dirPath: string,
    opts:    CollectionV2Options = {},
  ) {
    super(name, engine);
    this._dirPath = dirPath;
    this.crypto   = opts.crypto;
    this.secIdx   = new SecondaryIndexManager(dirPath, name);
    for (const def of opts.indexes ?? []) this.secIdx.addIndex(def);

    // Inject decrypt hook into engine IMMEDIATELY so that when engine.open()
    // calls _loadIndex() / _rebuildIndex(), it can decrypt records to read _id.
    if (opts.crypto) {
      this._engine.decryptFn = (buf: Buffer) => opts.crypto!.decrypt(buf);
    }

    // Schema validator
    if (opts.schema) {
      this._validator  = new SchemaValidator(opts.schema);
      this._schemaMode = opts.schemaMode ?? 'strict';
    } else {
      this._schemaMode = 'strict';
    }
  }

  /** Must be called after construction to load persisted index state. */
  async init(): Promise<void> {
    await this.secIdx.open();
  }

  override async flush(): Promise<void> {
    await super.flush();
    await this.secIdx.save();
  }

  // ── Index management ──────────────────────────────────────

  async ensureIndex(def: IndexDefinition): Promise<void> {
    this.secIdx.addIndex(def);
  }

  /**
   * Rebuild all secondary indexes by scanning the full collection.
   * Call after adding indexes to an existing collection.
   */
  async rebuildIndexes(): Promise<void> {
    const docs: Record<string, unknown>[] = [];
    for await (const [, raw] of (this._engine as any).scan()) {
      try {
        const plain = this.crypto ? this.crypto.decrypt(raw) : raw;
        docs.push(JSON.parse(plain.toString('utf8')));
      } catch { /* skip */ }
    }
    this.secIdx.rebuildFromDocs(docs);
    await this.secIdx.save();
    log.info(`Rebuilt indexes for "${this._colName}"`, { docs: docs.length });
  }

  // ── Overrides ─────────────────────────────────────────────

  override async insertOne(doc: Omit<T, '_id'> & { _id?: string }): Promise<T> {
    const _id  = doc._id ?? generateId();
    const full = { ...doc, _id } as T;

    // Schema validation — dilakukan sebelum apapun (sebelum unique check)
    this._validateDoc(full as unknown as Record<string, unknown>, 'insertOne');

    this.secIdx.onInsert(full as unknown as Record<string, unknown>); // check unique FIRST
    let data = Buffer.from(JSON.stringify(full), "utf8") as Buffer;
    if (this.crypto) data = this.crypto.encrypt(data);
    await this._engine.insert(_id, data);
    return full;
  }

  override async insertMany(docs: Array<Omit<T, '_id'> & { _id?: string }>): Promise<T[]> {
    const results: T[] = [];
    for (const doc of docs) results.push(await this.insertOne(doc));
    return results;
  }

  override async findOne(filter: QueryFilter): Promise<T | null> {
    // ── Fast path 1: _id lookup — pakai B+Tree index utama langsung (O(log n)).
    // FIX v1.1: sebelumnya encrypted collection jatuh ke full scan untuk _id lookup.
    if (this._isIdOnlyFilter(filter)) {
      const id  = filter['_id'] as string;
      const raw = await this._engine.read(id);
      if (!raw) return null;
      try {
        const plain = this.crypto ? this.crypto.decrypt(raw) : raw;
        const doc   = JSON.parse(plain.toString('utf8')) as T;
        return this._matches(doc, filter) ? doc : null;
      } catch { return null; }
    }
    // ── Fast path 2: secondary index
    const idxHits = this._trySecondaryIndex(filter);
    if (idxHits !== null) {
      for (const id of idxHits) {
        const raw = await this._engine.read(id);
        if (!raw) continue;
        try {
          const plain = this.crypto ? this.crypto.decrypt(raw) : raw;
          const doc   = JSON.parse(plain.toString('utf8')) as T;
          if (this._matches(doc, filter)) return doc;
        } catch { continue; }
      }
      return null;
    }
    // ── Fallback: full scan
    if (!this.crypto) return super.findOne(filter);
    for await (const doc of this._scanDecrypted(filter)) return doc;
    return null;
  }

  override async find(filter: QueryFilter = {}, opts: QueryOptions = {}): Promise<T[]> {
    // ── Fast path: secondary index tersedia
    const idxHits = this._trySecondaryIndex(filter);
    if (idxHits !== null) {
      const { skip = 0, limit, sort, projection } = opts;
      const candidates: T[] = [];
      for (const id of idxHits) {
        const raw = await this._engine.read(id);
        if (!raw) continue;
        try {
          const plain = this.crypto ? this.crypto.decrypt(raw) : raw;
          const doc   = JSON.parse(plain.toString('utf8')) as T;
          if (this._matches(doc, filter)) candidates.push(doc);
        } catch { continue; }
      }
      let result = candidates;
      if (sort)       result = this._sortDocs(result, sort);
      if (skip > 0)   result = result.slice(skip);
      if (limit)      result = result.slice(0, limit);
      if (projection) result = result.map(d => this._project(d, projection) as T);
      return result;
    }
    if (!this.crypto) return super.find(filter, opts);
    // ── Full scan untuk encrypted collection tanpa index
    const { skip = 0, limit, sort, projection } = opts;
    const results: T[] = [];
    let skipped = 0;
    for await (const doc of this._scanDecrypted(filter)) {
      if (skipped < skip) { skipped++; continue; }
      results.push(doc);
      if (!sort && limit && results.length >= limit) break;
    }
    let final = results;
    if (sort)            final = this._sortDocs(final, sort);
    if (sort && limit)   final = final.slice(0, limit);
    if (projection)      final = final.map(d => this._project(d, projection) as T);
    return final;
  }

  override async updateOne(filter: QueryFilter, update: UpdateSpec): Promise<boolean> {
    const doc = await this.findOne(filter);
    if (!doc) return false;
    const oldDoc  = { ...doc } as Record<string, unknown>;
    const updated = this._applyUpdate(doc, update);
    this.secIdx.onUpdate(oldDoc, updated as unknown as Record<string, unknown>);
    const plain = Buffer.from(JSON.stringify(updated), 'utf8') as Buffer;
    const data  = this.crypto ? this.crypto.encrypt(plain) as Buffer : plain;
    await this._engine.update(doc._id, data);
    return true;
  }

  override async deleteOne(filter: QueryFilter): Promise<boolean> {
    const doc = await this.findOne(filter);
    if (!doc) return false;
    this.secIdx.onDelete(doc as unknown as Record<string, unknown>);
    return this._engine.delete(doc._id);
  }

  // ── Private ───────────────────────────────────────────────

  /** Jalankan schema validation. Throw atau warn sesuai schemaMode. */
  private _validateDoc(doc: Record<string, unknown>, context: string): void {
    if (!this._validator) return;
    const result = this._validator.validate(doc);
    if (result.ok) return;
    const msg = `[OvnDB:${this._colName}] Schema validation gagal (${context}):\n` +
      result.errors.map(e => `  • ${e}`).join('\n');
    if (this._schemaMode === 'warn') {
      log.warn(msg);
    } else {
      throw Object.assign(new Error(msg), { validationErrors: result.errors, name: 'ValidationError' });
    }
  }

  private _trySecondaryIndex(filter: QueryFilter): string[] | null {
    for (const [field, condition] of Object.entries(filter)) {
      if (field.startsWith('$') || !this.secIdx.hasIndex(field)) continue;
      if (condition !== null && typeof condition === 'object' && !Array.isArray(condition)) {
        const ops = condition as Record<string, unknown>;
        if (ops['$eq']  !== undefined) return this.secIdx.lookup(field, ops['$eq']);
        if (ops['$gte'] !== undefined || ops['$lte'] !== undefined)
          return this.secIdx.lookupRange(
            field,
            ops['$gte'] !== undefined ? String(ops['$gte']) : undefined,
            ops['$lte'] !== undefined ? String(ops['$lte']) : undefined,
          );
      } else {
        return this.secIdx.lookup(field, condition);
      }
    }
    return null;
  }

  private async *_scanDecrypted(filter: QueryFilter): AsyncIterableIterator<T> {
    for await (const [, raw] of this._engine.scan()) {
      try {
        const plain = this.crypto ? this.crypto.decrypt(raw) : raw;
        const doc   = JSON.parse(plain.toString('utf8')) as T;
        if (this._matches(doc, filter)) yield doc;
      } catch { /* wrong key or corrupt — skip */ }
    }
  }
}
