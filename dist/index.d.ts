import { Collection } from './collection/collection.js';
import { CollectionV2, type CollectionV2Options } from './collection/collection-v2.js';
import { Transaction } from './core/transaction/transaction.js';
import type { OvnDocument, DBStatus } from './types/index.js';
export declare const VERSION = "4.0.0";
export interface OvnDBOptions {
    /** Byte limit untuk LRU cache per collection (default 256 MB). */
    cacheBytes?: number;
    /** @deprecated Gunakan cacheBytes. cacheSize dianggap jumlah entry × 4096 bytes. */
    cacheSize?: number;
    mkdirp?: boolean;
    fileLock?: boolean;
    /** Auto-close saat SIGTERM/SIGINT/beforeExit. */
    gracefulShutdown?: boolean;
    /** Inject compress function untuk G15. Contoh: zlib.gzipSync */
    compressFn?: (buf: Buffer) => Buffer;
    /** Inject decompress function untuk G15. Contoh: zlib.gunzipSync */
    decompressFn?: (buf: Buffer) => Buffer;
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
    integrityKey?: Buffer;
}
export declare class OvnDB {
    private readonly _dirPath;
    private readonly _cacheBytes;
    private readonly _engines;
    private readonly _cols;
    private readonly _lock;
    private readonly _openedAt;
    private readonly _opts;
    private _closed;
    private readonly _encrypted;
    private constructor();
    static open(dirPath: string, opts?: OvnDBOptions): Promise<OvnDB>;
    collection<T extends OvnDocument = OvnDocument>(name: string): Promise<Collection<T>>;
    collectionV2<T extends OvnDocument = OvnDocument>(name: string, opts?: CollectionV2Options): Promise<CollectionV2<T>>;
    beginTransaction(): Transaction;
    dropCollection(name: string): Promise<void>;
    listCollections(): Promise<string[]>;
    collectionExists(name: string): Promise<boolean>;
    flushAll(): Promise<void>;
    backup(destPath: string): Promise<void>;
    status(): Promise<DBStatus>;
    close(): Promise<void>;
    get path(): string;
    get isOpen(): boolean;
    private _makeEngine;
    private _assertOpen;
}
export { Collection } from './collection/collection.js';
export { CollectionV2, type CollectionV2Options, type KeyRotationResult } from './collection/collection-v2.js';
export { CryptoLayer, FieldCrypto, cryptoFromPassphrase, CRYPTO_OVERHEAD } from './crypto/crypto-layer.js';
export { Transaction, RollbackFailedError, WriteConflictError } from './core/transaction/transaction.js';
export { SchemaValidator, ValidationError, field, type SchemaDefinition, type FieldSchema, type FieldType, type ValidationResult } from './schema/schema-validator.js';
export { TTLIndex, type TTLIndexOptions } from './ttl/ttl-index.js';
export { Observability, getObservability, resetObservability, type ObservabilityOptions, type ObservabilityReport, type OperationRecord } from './observability/observability.js';
export { RelationManager, type RelationMap } from './collection/relation-manager.js';
export { ChangeStream, ChangeStreamRegistry, type WatchOptions } from './collection/change-stream.js';
export { MigrationRunner, type MigrationOptions, type MigrationResult, type MigrationProgress } from './migration/migration-runner.js';
export { generateId, idToTimestamp, isValidId } from './utils/id-generator.js';
export { makeLogger } from './utils/logger.js';
export { FileLock } from './utils/file-lock.js';
export type { ExecutionStats } from './core/query/planner.js';
export { FTSIndex } from './core/index/fts-index.js';
export { BloomFilter } from './core/storage/bloom-filter.js';
export { exportTo, importFrom } from './collection/import-export.js';
export type { OvnDocument, QueryFilter, QueryOptions, UpdateSpec, AggregationStage, OvnStats, IndexDefinition, QueryPlan, RecordPointer, SegmentMeta, CollectionManifest, ChangeEvent, ChangeOperationType, TxStatus, TxSnapshot, ForeignKey, ForeignKeyArray, BulkWriteOp, BulkWriteResult, DBStatus, CollectionStatus, CursorOptions, TextIndexDefinition, ExportOptions, ImportOptions, ImportResult, } from './types/index.js';
//# sourceMappingURL=index.d.ts.map