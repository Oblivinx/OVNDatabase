// ============================================================
//  OvnDB v4.0 — Public Entry Point
//  v4.0: FTS, BloomFilter, Compound Index, Savepoints, Import/Export
// ============================================================

import fsp  from 'fs/promises';
import path from 'path';
import { StorageEngine }  from './core/storage/storage-engine.js';
import { Collection }     from './collection/collection.js';
import { CollectionV2, type CollectionV2Options } from './collection/collection-v2.js';
import { FileLock }       from './utils/file-lock.js';
import { Transaction }    from './core/transaction/transaction.js';
import { makeLogger }     from './utils/logger.js';
import { validateCollectionName, assertPathInside } from './utils/security.js';
import type { OvnDocument, DBStatus, CollectionStatus } from './types/index.js';

const log = makeLogger('db');

export const VERSION = '4.0.0';

export interface OvnDBOptions {
  /** Byte limit untuk LRU cache per collection (default 256 MB). */
  cacheBytes?:       number;
  /** @deprecated Gunakan cacheBytes. cacheSize dianggap jumlah entry × 4096 bytes. */
  cacheSize?:        number;
  mkdirp?:           boolean;
  fileLock?:         boolean;
  /** Auto-close saat SIGTERM/SIGINT/beforeExit. */
  gracefulShutdown?: boolean;
  /** Inject compress function untuk G15. Contoh: zlib.gzipSync */
  compressFn?:       (buf: Buffer) => Buffer;
  /** Inject decompress function untuk G15. Contoh: zlib.gunzipSync */
  decompressFn?:     (buf: Buffer) => Buffer;
  /**
   * SECURITY: 32-byte HMAC key untuk manifest integrity.
   * Jika di-set, manifest checksum ditulis sebagai HMAC-SHA256 (bukan SHA-256 plain).
   * HMAC mendeteksi modifikasi disengaja — SHA-256 plain hanya mendeteksi korupsi acak.
   *
   * Untuk database terenkripsi, derive dari key yang sama:
   * @example
   *   import crypto from 'crypto';
   *   const masterKey = await cryptoFromPassphrase(pass, dir);
   *   // Derive integrity key dari master key (HKDF-like dengan hash)
   *   const integrityKey = crypto.createHash('sha256')
   *     .update(masterKey.key).update('ovndb-manifest-hmac').digest();
   *   const db = await OvnDB.open('./data', { integrityKey });
   */
  integrityKey?:     Buffer;
}

export class OvnDB {
  private readonly _dirPath:    string;
  private readonly _cacheBytes: number;
  private readonly _engines:    Map<string, StorageEngine>   = new Map();
  private readonly _cols:       Map<string, Collection<any>> = new Map();
  private readonly _lock:       FileLock | null;
  private readonly _openedAt:   number;
  private readonly _opts:       OvnDBOptions;
  private _closed = false;
  private readonly _encrypted: Set<string> = new Set();

  private constructor(dirPath: string, cacheBytes: number, lock: FileLock | null, opts: OvnDBOptions) {
    this._dirPath    = dirPath;
    this._cacheBytes = cacheBytes;
    this._lock       = lock;
    this._openedAt   = Date.now();
    this._opts       = opts;
  }

  static async open(dirPath: string, opts: OvnDBOptions = {}): Promise<OvnDB> {
    const resolved = path.resolve(dirPath);
    // G1: prefer cacheBytes, fallback ke cacheSize × 4096 untuk compat
    const cacheBytes = opts.cacheBytes
      ?? (opts.cacheSize ? opts.cacheSize * 4096 : 256 * 1024 * 1024);

    // SECURITY: validate integrityKey early so open() rejects immediately
    if (opts.integrityKey !== undefined && opts.integrityKey.length !== 32) {
      throw new Error('[OvnDB] integrityKey harus tepat 32 byte (256-bit untuk HMAC-SHA256)');
    }

    if (opts.mkdirp !== false) await fsp.mkdir(resolved, { recursive: true });

    let lock: FileLock | null = null;
    if (opts.fileLock !== false) {
      lock = new FileLock(resolved);
      await lock.acquire();
    }

    const db = new OvnDB(resolved, cacheBytes, lock, opts);

    if (opts.gracefulShutdown) {
      const shutdown = async (signal: string) => {
        if (!db._closed) {
          log.info(`Graceful shutdown (${signal})`);
          await db.close().catch(e => log.error('Shutdown error', { err: String(e) }));
        }
      };
      process.once('SIGTERM', () => shutdown('SIGTERM'));
      process.once('SIGINT',  () => shutdown('SIGINT'));
      process.once('beforeExit', () => shutdown('beforeExit'));
    }

    log.info('Database opened', { path: resolved, cacheBytes });
    return db;
  }

  // ── Collection Access ─────────────────────────────────────

  async collection<T extends OvnDocument = OvnDocument>(name: string): Promise<Collection<T>> {
    this._assertOpen();
    // SECURITY: cegah path traversal dan nama tidak valid
    validateCollectionName(name);
    const existing = this._cols.get(name);
    if (existing) return existing as Collection<T>;

    const colDir = path.join(this._dirPath, name);
    const engine = this._makeEngine(colDir, name);
    await engine.open();
    const col = new Collection<T>(name, engine);
    this._engines.set(name, engine);
    this._cols.set(name, col as Collection<any>);
    return col;
  }

  async collectionV2<T extends OvnDocument = OvnDocument>(
    name: string,
    opts: CollectionV2Options = {},
  ): Promise<CollectionV2<T>> {
    this._assertOpen();
    // SECURITY: cegah path traversal dan nama tidak valid
    validateCollectionName(name);
    const existing = this._cols.get(name);
    if (existing) {
      if (existing instanceof CollectionV2) return existing as CollectionV2<T>;
      throw new Error(`[OvnDB] Collection "${name}" sudah dibuka sebagai Collection biasa`);
    }

    const colDir = path.join(this._dirPath, name);
    const engine = this._makeEngine(colDir, name);
    if (opts.crypto) {
      engine.decryptFn = (buf) => opts.crypto!.decrypt(buf);
      this._encrypted.add(name);
    }
    await engine.open();
    const col = new CollectionV2<T>(name, engine, opts);
    await col.init(opts);
    this._engines.set(name, engine);
    this._cols.set(name, col as unknown as Collection<any>);
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
    // SECURITY: cegah path traversal
    validateCollectionName(name);
    const engine = this._engines.get(name);
    if (engine) {
      await engine.close();
      this._engines.delete(name);
      this._cols.delete(name);
      this._encrypted.delete(name);
    }
    await fsp.rm(path.join(this._dirPath, name), { recursive: true, force: true });
    log.info('Collection dropped', { name });
  }

  async listCollections(): Promise<string[]> {
    this._assertOpen();
    try {
      const entries = await fsp.readdir(this._dirPath, { withFileTypes: true });
      return entries.filter(e => e.isDirectory()).map(e => e.name);
    } catch { return []; }
  }

  async collectionExists(name: string): Promise<boolean> {
    this._assertOpen();
    try { return (await fsp.stat(path.join(this._dirPath, name))).isDirectory(); }
    catch { return false; }
  }

  async flushAll(): Promise<void> {
    this._assertOpen();
    await Promise.all([...this._cols.values()].map(c => c.flush()));
  }

  async backup(destPath: string): Promise<void> {
    this._assertOpen();
    const resolved = path.resolve(destPath);
    // SECURITY: pastikan backup destination tidak berada di dalam data dir
    // (mencegah overwrite data aktif) dan tidak escaping ke atas via ../
    // Kita hanya melarang dest yang SAMA dengan atau di dalam data dir sendiri.
    // Dest di luar data dir (misal /backups/...) diperbolehkan.
    if (resolved.startsWith(this._dirPath)) {
      throw new Error(
        '[OvnDB] Backup destination tidak boleh berada di dalam direktori data aktif'
      );
    }
    await fsp.mkdir(resolved, { recursive: true });
    await this.flushAll();
    await Promise.all(
      [...this._engines.entries()].map(([name, engine]) =>
        engine.backup(path.join(resolved, name)),
      ),
    );
    log.info('Full DB backup complete', { dest: resolved });
  }

  async status(): Promise<DBStatus> {
    this._assertOpen();
    const collections: CollectionStatus[] = [];
    let totalSize = 0, isHealthy = true;

    for (const [name, engine] of this._engines.entries()) {
      try {
        const s = await engine.stats(name);
        collections.push({
          name,
          totalLive:     s.totalLive,
          totalDead:     s.totalDead,
          segmentCount:  s.segmentCount,
          totalFileSize: s.totalFileSize,
          fragmentRatio: s.fragmentRatio,
          encrypted:     this._encrypted.has(name),
          indexCount:    s.indexCount,
          cacheHitRate:  s.cacheHitRate,
        });
        totalSize += s.totalFileSize;
        if (s.fragmentRatio > 0.6) isHealthy = false;
      } catch { isHealthy = false; }
    }

    return { path: this._dirPath, openedAt: this._openedAt, collections, totalSize, isHealthy };
  }

  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    await Promise.all([...this._engines.values()].map(e => e.close()));
    this._engines.clear();
    this._cols.clear();
    this._encrypted.clear();
    if (this._lock) await this._lock.release();
    log.info('Database closed');
  }

  get path():   string  { return this._dirPath; }
  get isOpen(): boolean { return !this._closed; }

  private _makeEngine(colDir: string, name: string): StorageEngine {
    const engine = new StorageEngine(colDir, name, this._cacheBytes);
    // G15: propagate compression hooks dari OvnDBOptions
    if (this._opts.compressFn)   engine.compressFn   = this._opts.compressFn;
    if (this._opts.decompressFn) engine.decompressFn = this._opts.decompressFn;
    // SECURITY: propagate HMAC integrity key untuk manifest protection
    if (this._opts.integrityKey) {
      if (this._opts.integrityKey.length !== 32)
        throw new Error('[OvnDB] integrityKey harus tepat 32 bytes');
      engine.integrityKey = this._opts.integrityKey;
    }
    return engine;
  }

  private _assertOpen(): void {
    if (this._closed) throw new Error('[OvnDB] Database sudah ditutup');
  }
}

// ── Re-exports ────────────────────────────────────────────────

export { Collection }                               from './collection/collection.js';
export { CollectionV2, type CollectionV2Options, type KeyRotationResult } from './collection/collection-v2.js';
export { CryptoLayer, FieldCrypto, cryptoFromPassphrase, CRYPTO_OVERHEAD } from './crypto/crypto-layer.js';
export { Transaction, RollbackFailedError, WriteConflictError } from './core/transaction/transaction.js';
export { SchemaValidator, ValidationError, field, type SchemaDefinition, type FieldSchema, type FieldType, type ValidationResult } from './schema/schema-validator.js';
export { TTLIndex, type TTLIndexOptions }            from './ttl/ttl-index.js';
export { Observability, getObservability, resetObservability, type ObservabilityOptions, type ObservabilityReport, type OperationRecord } from './observability/observability.js';
export { RelationManager, type RelationMap }         from './collection/relation-manager.js';
export { ChangeStream, ChangeStreamRegistry, type WatchOptions } from './collection/change-stream.js';
export { MigrationRunner, type MigrationOptions, type MigrationResult, type MigrationProgress } from './migration/migration-runner.js';
export { generateId, idToTimestamp, isValidId }      from './utils/id-generator.js';
export { makeLogger }                                from './utils/logger.js';
export { FileLock }                                  from './utils/file-lock.js';
export type { ExecutionStats }                        from './core/query/planner.js';

// v4.0 new exports
export { FTSIndex }                                  from './core/index/fts-index.js';
export { BloomFilter }                               from './core/storage/bloom-filter.js';
export { exportTo, importFrom }                      from './collection/import-export.js';

export type {
  OvnDocument, QueryFilter, QueryOptions, UpdateSpec,
  AggregationStage, OvnStats, IndexDefinition, QueryPlan,
  RecordPointer, SegmentMeta, CollectionManifest,
  ChangeEvent, ChangeOperationType,
  TxStatus, TxSnapshot,
  ForeignKey, ForeignKeyArray,
  BulkWriteOp, BulkWriteResult,
  DBStatus, CollectionStatus,
  CursorOptions,
  // v4.0 new types
  TextIndexDefinition,
  ExportOptions, ImportOptions, ImportResult,
} from './types/index.js';
