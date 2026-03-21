// ============================================================
//  OvnDB v1.1 — Public Entry Point
//
//  const db    = await OvnDB.open('./data');
//  const users = await db.collection<User>('users');
//  await users.insertOne({ name: 'Budi', phone: '628xxx', points: 0 });
//
//  v1.1 additions:
//   - FileLock: single-writer process guard (cegah corruption)
//   - Transaction: atomic multi-collection writes + rollback
//   - TTLIndex: auto-expiry untuk session, rate-limit, OTP
//   - Observability: slow query log, per-op metrics, P95/P99
//   - Schema re-exports: field, SchemaValidator, ValidationError
// ============================================================
import fsp  from 'fs/promises';
import path from 'path';
import { StorageEngine }  from './core/storage-engine.js';
import { Collection }     from './collection.js';
import { CollectionV2, type CollectionV2Options } from './collection-v2.js';
import { FileLock }       from './core/file-lock.js';
import { Transaction }    from './core/transaction.js';
import { makeLogger }     from './utils/logger.js';
import type { OvnDocument, DEFAULT_CACHE_SIZE } from './types.js';
import { DEFAULT_CACHE_SIZE as CACHE_DEFAULT }   from './types.js';

const log = makeLogger('db');

export interface OvnDBOptions {
  /** LRU cache entries per collection (default: 100_000) */
  cacheSize?: number;
  /** Create the data directory if missing (default: true) */
  mkdirp?:   boolean;
  /**
   * Aktifkan file lock untuk mencegah dua proses membuka DB yang sama.
   * Default: true
   */
  fileLock?: boolean;
}

export class OvnDB {
  private readonly dirPath:     string;
  private readonly cacheSize:   number;
  private readonly engines:     Map<string, StorageEngine>    = new Map();
  private readonly collections: Map<string, Collection<any>> = new Map();
  private readonly _lock:       FileLock | null;
  private _closed = false;

  private constructor(dirPath: string, cacheSize: number, lock: FileLock | null) {
    this.dirPath   = dirPath;
    this.cacheSize = cacheSize;
    this._lock     = lock;
  }

  /**
   * Open (or create) an OvnDB data directory.
   * Secara default mengaktifkan file lock untuk mencegah
   * dua proses membuka database yang sama secara bersamaan.
   */
  static async open(dirPath: string, opts: OvnDBOptions = {}): Promise<OvnDB> {
    const resolved  = path.resolve(dirPath);
    const cacheSize = opts.cacheSize ?? CACHE_DEFAULT;
    if (opts.mkdirp !== false) await fsp.mkdir(resolved, { recursive: true });

    // File lock — acquire sebelum membuka apapun
    let lock: FileLock | null = null;
    if (opts.fileLock !== false) {
      lock = new FileLock(resolved);
      await lock.acquire();
      log.debug('File lock acquired', { path: resolved });
    }

    log.info(`Opening database`, { path: resolved, cacheSize });
    return new OvnDB(resolved, cacheSize, lock);
  }

  /**
   * Buat sebuah Transaction baru untuk atomic multi-collection writes.
   *
   * @example
   *   const tx = db.beginTransaction();
   *   tx.insert(users, { name: 'Budi', phone: '628xxx' });
   *   tx.update(wallets, { _id: wId }, { $inc: { balance: -50_000 } });
   *   await tx.commit(); // atau tx.rollback()
   */
  beginTransaction(): Transaction {
    this._assertOpen();
    return new Transaction();
  }

  /**
   * Get (or lazily open) a standard collection.
   * Opens .ovn + .ovni + .wal files on first access.
   */
  async collection<T extends OvnDocument = OvnDocument>(name: string): Promise<Collection<T>> {
    this._assertOpen();
    if (this.collections.has(name)) return this.collections.get(name) as Collection<T>;
    const engine = new StorageEngine(this.dirPath, name, this.cacheSize);
    await engine.open();
    const col = new Collection<T>(name, engine);
    this.engines    .set(name, engine);
    this.collections.set(name, col);
    return col;
  }

  /**
   * Get (or lazily open) a CollectionV2 with optional crypto + secondary indexes.
   * CollectionV2 is a drop-in superset of Collection.
   */
  async collectionV2<T extends OvnDocument = OvnDocument>(
    name: string,
    opts: CollectionV2Options = {},
  ): Promise<CollectionV2<T>> {
    this._assertOpen();
    if (this.collections.has(name)) {
      const existing = this.collections.get(name);
      if (existing instanceof CollectionV2) return existing as CollectionV2<T>;
      throw new Error(`[OvnDB] Collection "${name}" already opened as basic Collection`);
    }
    const engine = new StorageEngine(this.dirPath, name, this.cacheSize);
    // IMPORTANT: inject decryptFn BEFORE engine.open() so that _loadIndex /
    // _rebuildIndex can decrypt payloads to recover _id from encrypted records.
    if (opts.crypto) {
      engine.decryptFn = (buf: Buffer) => opts.crypto!.decrypt(buf);
    }
    await engine.open();
    const col = new CollectionV2<T>(name, engine, this.dirPath, opts);
    await col.init();
    this.engines    .set(name, engine);
    this.collections.set(name, col);
    return col;
  }

  /** Drop a collection — deletes all associated files. */
  async dropCollection(name: string): Promise<void> {
    const engine = this.engines.get(name);
    if (engine) { await engine.close(); this.engines.delete(name); this.collections.delete(name); }
    const base = path.join(this.dirPath, name);
    await Promise.all([
      fsp.unlink(`${base}.ovn` ).catch(() => {}),
      fsp.unlink(`${base}.ovni`).catch(() => {}),
      fsp.unlink(`${base}.wal` ).catch(() => {}),
    ]);
  }

  /** List all collection names in this database. */
  async listCollections(): Promise<string[]> {
    const files = await fsp.readdir(this.dirPath);
    const names = new Set<string>();
    for (const f of files) if (f.endsWith('.ovn')) names.add(f.slice(0, -4));
    return [...names];
  }

  /** Flush all pending writes across all open collections. */
  async flushAll(): Promise<void> {
    await Promise.all([...this.collections.values()].map(c => c.flush()));
  }

  /** Gracefully close all collections and flush pending writes. */
  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    log.info('Closing database', { path: this.dirPath });
    await Promise.all([...this.engines.values()].map(e => e.close()));
    this.engines.clear();
    this.collections.clear();
    // Release file lock setelah semua engine ditutup
    if (this._lock) await this._lock.release();
  }

  get path():   string  { return this.dirPath; }
  get isOpen(): boolean { return !this._closed; }

  private _assertOpen(): void {
    if (this._closed) throw new Error('[OvnDB] Database is closed');
  }
}

// ── Re-exports ────────────────────────────────────────────────
export { Collection }                             from './collection.js';
export { CollectionV2, type CollectionV2Options } from './collection-v2.js';
export { CryptoLayer, cryptoFromPassphrase, CRYPTO_OVERHEAD } from './crypto/crypto-layer.js';
export { RelationManager, type RelationMap }      from './relations/relation-manager.js';
export { SecondaryIndexManager, type IndexDefinition } from './core/secondary-index.js';
export { generateId, idToTimestamp, isValidId }   from './utils/id-generator.js';
export { makeLogger }                             from './utils/logger.js';

// v1.1 new exports
export { FileLock }                               from './core/file-lock.js';
export { Transaction, RollbackFailedError, type TxStatus } from './core/transaction.js';
export { TTLIndex, type TTLIndexOptions }         from './core/ttl-index.js';
export {
  Observability, getObservability, resetObservability,
  type ObservabilityOptions, type ObservabilityReport, type QueryRecord,
} from './core/observability.js';
export {
  SchemaValidator, ValidationError, field,
  type SchemaDefinition, type FieldSchema, type FieldType, type ValidationResult,
} from './core/schema-validator.js';

export type {
  OvnDocument, QueryFilter, QueryOptions, UpdateSpec,
  OvnStats, FileFlags, ForeignKey, ForeignKeyArray,
} from './types.js';
