// ============================================================
//  OvnDB v2.1 — Public Entry Point
//
//  update: added backup(), status(), graceful shutdown,
//          listCollections() dengan metadata,
//          collectionExists(), renameCollection()
// ============================================================

import fsp  from 'fs/promises';
import path from 'path';
import { StorageEngine }  from './core/storage/storage-engine.js';
import { Collection }     from './collection/collection.js';
import { CollectionV2, type CollectionV2Options } from './collection/collection-v2.js';
import { FileLock }       from './utils/file-lock.js';
import { Transaction }    from './core/transaction/transaction.js';
import { makeLogger }     from './utils/logger.js';
import type { OvnDocument, DBStatus, CollectionStatus } from './types/index.js';

const log = makeLogger('db');

export interface OvnDBOptions {
  cacheSize?: number;
  mkdirp?:    boolean;
  fileLock?:  boolean;
  /**
   * feat: gracefulShutdown — auto-close DB saat process SIGTERM/SIGINT.
   * Mencegah data loss saat container/server di-kill.
   * @default false
   */
  gracefulShutdown?: boolean;
}

export class OvnDB {
  private readonly _dirPath:   string;
  private readonly _cacheSize: number;
  private readonly _engines:   Map<string, StorageEngine>    = new Map();
  private readonly _cols:      Map<string, Collection<any>>  = new Map();
  private readonly _lock:      FileLock | null;
  private readonly _openedAt:  number;
  private _closed = false;
  /** track encrypted collections for status() */
  private readonly _encrypted: Set<string> = new Set();

  private constructor(dirPath: string, cacheSize: number, lock: FileLock | null) {
    this._dirPath   = dirPath;
    this._cacheSize = cacheSize;
    this._lock      = lock;
    this._openedAt  = Date.now();
  }

  // ── Factory ───────────────────────────────────────────────

  static async open(dirPath: string, opts: OvnDBOptions = {}): Promise<OvnDB> {
    const resolved  = path.resolve(dirPath);
    const cacheSize = opts.cacheSize ?? 100_000;

    if (opts.mkdirp !== false) await fsp.mkdir(resolved, { recursive: true });

    let lock: FileLock | null = null;
    if (opts.fileLock !== false) {
      lock = new FileLock(resolved);
      await lock.acquire();
      log.debug('File lock acquired', { path: resolved });
    }

    const db = new OvnDB(resolved, cacheSize, lock);

    // feat: graceful shutdown — auto-close on process exit signals
    if (opts.gracefulShutdown) {
      const shutdown = async (signal: string) => {
        if (!db._closed) {
          log.info(`Graceful shutdown triggered by ${signal}`);
          await db.close().catch(err =>
            log.error('Graceful shutdown error', { err: String(err) }),
          );
        }
      };
      process.once('SIGTERM', () => shutdown('SIGTERM'));
      process.once('SIGINT',  () => shutdown('SIGINT'));
      process.once('beforeExit', () => shutdown('beforeExit'));
    }

    log.info('Opening database', { path: resolved, cacheSize });
    return db;
  }

  // ── Collection Access ─────────────────────────────────────

  async collection<T extends OvnDocument = OvnDocument>(name: string): Promise<Collection<T>> {
    this._assertOpen();
    const existing = this._cols.get(name);
    if (existing) return existing as Collection<T>;

    const colDir = path.join(this._dirPath, name);
    const engine = new StorageEngine(colDir, name, this._cacheSize);
    await engine.open();

    const col = new Collection<T>(name, engine);
    this._engines.set(name, engine);
    this._cols.set(name, col as Collection<any>);
    log.debug('Collection opened', { name });
    return col;
  }

  async collectionV2<T extends OvnDocument = OvnDocument>(
    name: string,
    opts: CollectionV2Options = {},
  ): Promise<CollectionV2<T>> {
    this._assertOpen();
    const existing = this._cols.get(name);
    if (existing) {
      if (existing instanceof CollectionV2) return existing as CollectionV2<T>;
      throw new Error(`[OvnDB] Collection "${name}" sudah dibuka sebagai Collection biasa`);
    }

    const colDir = path.join(this._dirPath, name);
    const engine = new StorageEngine(colDir, name, this._cacheSize);
    if (opts.crypto) {
      engine.decryptFn = (buf) => opts.crypto!.decrypt(buf);
      this._encrypted.add(name);
    }
    await engine.open();

    const col = new CollectionV2<T>(name, engine, opts);
    await col.init(opts);
    this._engines.set(name, engine);
    this._cols.set(name, col as unknown as Collection<any>);
    log.debug('CollectionV2 opened', { name, encrypted: !!opts.crypto });
    return col;
  }

  // ── Transaction ───────────────────────────────────────────

  beginTransaction(): Transaction {
    this._assertOpen();
    return new Transaction();
  }

  // ── Maintenance ───────────────────────────────────────────

  async dropCollection(name: string): Promise<void> {
    this._assertOpen();
    const engine = this._engines.get(name);
    if (engine) {
      await engine.close();
      this._engines.delete(name);
      this._cols.delete(name);
      this._encrypted.delete(name);
    }
    const colDir = path.join(this._dirPath, name);
    await fsp.rm(colDir, { recursive: true, force: true });
    log.info('Collection dropped', { name });
  }

  /** update: kembalikan nama collection dari direktori yang ada */
  async listCollections(): Promise<string[]> {
    this._assertOpen();
    try {
      const entries = await fsp.readdir(this._dirPath, { withFileTypes: true });
      return entries.filter(e => e.isDirectory()).map(e => e.name);
    } catch {
      return [];
    }
  }

  /**
   * feat: cek apakah collection dengan nama tertentu sudah ada di disk
   */
  async collectionExists(name: string): Promise<boolean> {
    this._assertOpen();
    try {
      const stat = await fsp.stat(path.join(this._dirPath, name));
      return stat.isDirectory();
    } catch {
      return false;
    }
  }

  async flushAll(): Promise<void> {
    this._assertOpen();
    await Promise.all([...this._cols.values()].map(c => c.flush()));
  }

  /**
   * feat: backup — buat salinan konsisten semua collection yang terbuka
   * ke direktori tujuan. Aman dilakukan saat DB aktif.
   *
   * @param destPath  Direktori tujuan backup (akan dibuat jika belum ada)
   *
   * @example
   *   await db.backup('./backup/2024-01-01');
   */
  async backup(destPath: string): Promise<void> {
    this._assertOpen();
    const resolved = path.resolve(destPath);
    await fsp.mkdir(resolved, { recursive: true });

    // Flush semua collection dulu
    await this.flushAll();

    // Backup setiap collection secara paralel
    await Promise.all(
      [...this._engines.entries()].map(([name, engine]) =>
        engine.backup(path.join(resolved, name)),
      ),
    );

    log.info('Full database backup complete', { dest: resolved, collections: this._engines.size });
  }

  /**
   * feat: status — kembalikan laporan kesehatan lengkap database.
   * Berguna untuk monitoring, alerting, dan debugging.
   *
   * @example
   *   const s = await db.status();
   *   console.log(`Healthy: ${s.isHealthy}, Total: ${s.totalSize} bytes`);
   */
  async status(): Promise<DBStatus> {
    this._assertOpen();

    const collections: CollectionStatus[] = [];
    let totalSize = 0;
    let isHealthy = true;

    for (const [name, engine] of this._engines.entries()) {
      try {
        const s = await engine.stats(name);
        const colStatus: CollectionStatus = {
          name,
          totalLive:     s.totalLive,
          totalDead:     s.totalDead,
          segmentCount:  s.segmentCount,
          totalFileSize: s.totalFileSize,
          fragmentRatio: s.fragmentRatio,
          encrypted:     this._encrypted.has(name),
          indexCount:    s.indexCount,
          cacheHitRate:  s.cacheHitRate,
        };
        collections.push(colStatus);
        totalSize += s.totalFileSize;
        // Tandai tidak sehat jika fragmentasi > 60%
        if (s.fragmentRatio > 0.6) isHealthy = false;
      } catch (err) {
        log.error('stats error', { name, err: String(err) });
        isHealthy = false;
      }
    }

    return {
      path:        this._dirPath,
      openedAt:    this._openedAt,
      collections,
      totalSize,
      isHealthy,
    };
  }

  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    log.info('Closing database', { path: this._dirPath });
    await Promise.all([...this._engines.values()].map(e => e.close()));
    this._engines.clear();
    this._cols.clear();
    this._encrypted.clear();
    if (this._lock) await this._lock.release();
    log.info('Database closed');
  }

  get path():   string  { return this._dirPath; }
  get isOpen(): boolean { return !this._closed; }

  private _assertOpen(): void {
    if (this._closed) throw new Error('[OvnDB] Database sudah ditutup');
  }
}

// ── Re-exports (public API) ───────────────────────────────────

export { Collection }                        from './collection/collection.js';
export { CollectionV2, type CollectionV2Options } from './collection/collection-v2.js';

export { CryptoLayer, cryptoFromPassphrase, CRYPTO_OVERHEAD } from './crypto/crypto-layer.js';

export {
  Transaction, RollbackFailedError, WriteConflictError,
} from './core/transaction/transaction.js';

export {
  SchemaValidator, ValidationError, field,
  type SchemaDefinition, type FieldSchema, type FieldType, type ValidationResult,
} from './schema/schema-validator.js';

export { TTLIndex, type TTLIndexOptions } from './ttl/ttl-index.js';

export {
  Observability, getObservability, resetObservability,
  type ObservabilityOptions, type ObservabilityReport, type OperationRecord,
} from './observability/observability.js';

export { RelationManager, type RelationMap } from './collection/relation-manager.js';

export {
  ChangeStream, ChangeStreamRegistry, type WatchOptions,
} from './collection/change-stream.js';

export { generateId, idToTimestamp, isValidId } from './utils/id-generator.js';
export { makeLogger } from './utils/logger.js';
export { FileLock }   from './utils/file-lock.js';

export type {
  OvnDocument, QueryFilter, QueryOptions, UpdateSpec,
  AggregationStage, OvnStats, IndexDefinition, QueryPlan,
  RecordPointer, SegmentMeta, CollectionManifest,
  ChangeEvent, ChangeOperationType,
  TxStatus, TxSnapshot,
  ForeignKey, ForeignKeyArray,
  // feat: new types
  BulkWriteOp, BulkWriteResult,
  DBStatus, CollectionStatus,
  CursorOptions,
} from './types/index.js';
