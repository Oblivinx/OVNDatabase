// ============================================================
//  OvnDB v4.0 — Public Entry Point
//  v4.0: FTS, BloomFilter, Compound Index, Savepoints, Import/Export
// ============================================================
import fsp from 'fs/promises';
import path from 'path';
import { StorageEngine } from './core/storage/storage-engine.js';
import { Collection } from './collection/collection.js';
import { CollectionV2 } from './collection/collection-v2.js';
import { FileLock } from './utils/file-lock.js';
import { Transaction } from './core/transaction/transaction.js';
import { makeLogger } from './utils/logger.js';
import { validateCollectionName } from './utils/security.js';
const log = makeLogger('db');
export const VERSION = '4.0.0';
export class OvnDB {
    _dirPath;
    _cacheBytes;
    _engines = new Map();
    _cols = new Map();
    _lock;
    _openedAt;
    _opts;
    _closed = false;
    _encrypted = new Set();
    constructor(dirPath, cacheBytes, lock, opts) {
        this._dirPath = dirPath;
        this._cacheBytes = cacheBytes;
        this._lock = lock;
        this._openedAt = Date.now();
        this._opts = opts;
    }
    static async open(dirPath, opts = {}) {
        const resolved = path.resolve(dirPath);
        // G1: prefer cacheBytes, fallback ke cacheSize × 4096 untuk compat
        const cacheBytes = opts.cacheBytes
            ?? (opts.cacheSize ? opts.cacheSize * 4096 : 256 * 1024 * 1024);
        // SECURITY: validate integrityKey early so open() rejects immediately
        if (opts.integrityKey !== undefined && opts.integrityKey.length !== 32) {
            throw new Error('[OvnDB] integrityKey harus tepat 32 byte (256-bit untuk HMAC-SHA256)');
        }
        if (opts.mkdirp !== false)
            await fsp.mkdir(resolved, { recursive: true });
        let lock = null;
        if (opts.fileLock !== false) {
            lock = new FileLock(resolved);
            await lock.acquire();
        }
        const db = new OvnDB(resolved, cacheBytes, lock, opts);
        if (opts.gracefulShutdown) {
            const shutdown = async (signal) => {
                if (!db._closed) {
                    log.info(`Graceful shutdown (${signal})`);
                    await db.close().catch(e => log.error('Shutdown error', { err: String(e) }));
                }
            };
            process.once('SIGTERM', () => shutdown('SIGTERM'));
            process.once('SIGINT', () => shutdown('SIGINT'));
            process.once('beforeExit', () => shutdown('beforeExit'));
        }
        log.info('Database opened', { path: resolved, cacheBytes });
        return db;
    }
    // ── Collection Access ─────────────────────────────────────
    async collection(name) {
        this._assertOpen();
        // SECURITY: cegah path traversal dan nama tidak valid
        validateCollectionName(name);
        const existing = this._cols.get(name);
        if (existing)
            return existing;
        const colDir = path.join(this._dirPath, name);
        const engine = this._makeEngine(colDir, name);
        await engine.open();
        const col = new Collection(name, engine);
        this._engines.set(name, engine);
        this._cols.set(name, col);
        return col;
    }
    async collectionV2(name, opts = {}) {
        this._assertOpen();
        // SECURITY: cegah path traversal dan nama tidak valid
        validateCollectionName(name);
        const existing = this._cols.get(name);
        if (existing) {
            if (existing instanceof CollectionV2)
                return existing;
            throw new Error(`[OvnDB] Collection "${name}" sudah dibuka sebagai Collection biasa`);
        }
        const colDir = path.join(this._dirPath, name);
        const engine = this._makeEngine(colDir, name);
        if (opts.crypto) {
            engine.decryptFn = (buf) => opts.crypto.decrypt(buf);
            this._encrypted.add(name);
        }
        await engine.open();
        const col = new CollectionV2(name, engine, opts);
        await col.init(opts);
        this._engines.set(name, engine);
        this._cols.set(name, col);
        return col;
    }
    // ── Transaction ───────────────────────────────────────────
    beginTransaction() {
        this._assertOpen();
        return new Transaction();
    }
    // ── Maintenance ───────────────────────────────────────────
    async dropCollection(name) {
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
    async listCollections() {
        this._assertOpen();
        try {
            const entries = await fsp.readdir(this._dirPath, { withFileTypes: true });
            return entries.filter(e => e.isDirectory()).map(e => e.name);
        }
        catch {
            return [];
        }
    }
    async collectionExists(name) {
        this._assertOpen();
        try {
            return (await fsp.stat(path.join(this._dirPath, name))).isDirectory();
        }
        catch {
            return false;
        }
    }
    async flushAll() {
        this._assertOpen();
        await Promise.all([...this._cols.values()].map(c => c.flush()));
    }
    async backup(destPath) {
        this._assertOpen();
        const resolved = path.resolve(destPath);
        // SECURITY: pastikan backup destination tidak berada di dalam data dir
        // (mencegah overwrite data aktif) dan tidak escaping ke atas via ../
        // Kita hanya melarang dest yang SAMA dengan atau di dalam data dir sendiri.
        // Dest di luar data dir (misal /backups/...) diperbolehkan.
        if (resolved.startsWith(this._dirPath)) {
            throw new Error('[OvnDB] Backup destination tidak boleh berada di dalam direktori data aktif');
        }
        await fsp.mkdir(resolved, { recursive: true });
        await this.flushAll();
        await Promise.all([...this._engines.entries()].map(([name, engine]) => engine.backup(path.join(resolved, name))));
        log.info('Full DB backup complete', { dest: resolved });
    }
    async status() {
        this._assertOpen();
        const collections = [];
        let totalSize = 0, isHealthy = true;
        for (const [name, engine] of this._engines.entries()) {
            try {
                const s = await engine.stats(name);
                collections.push({
                    name,
                    totalLive: s.totalLive,
                    totalDead: s.totalDead,
                    segmentCount: s.segmentCount,
                    totalFileSize: s.totalFileSize,
                    fragmentRatio: s.fragmentRatio,
                    encrypted: this._encrypted.has(name),
                    indexCount: s.indexCount,
                    cacheHitRate: s.cacheHitRate,
                });
                totalSize += s.totalFileSize;
                if (s.fragmentRatio > 0.6)
                    isHealthy = false;
            }
            catch {
                isHealthy = false;
            }
        }
        return { path: this._dirPath, openedAt: this._openedAt, collections, totalSize, isHealthy };
    }
    async close() {
        if (this._closed)
            return;
        this._closed = true;
        await Promise.all([...this._engines.values()].map(e => e.close()));
        this._engines.clear();
        this._cols.clear();
        this._encrypted.clear();
        if (this._lock)
            await this._lock.release();
        log.info('Database closed');
    }
    get path() { return this._dirPath; }
    get isOpen() { return !this._closed; }
    _makeEngine(colDir, name) {
        const engine = new StorageEngine(colDir, name, this._cacheBytes);
        // G15: propagate compression hooks dari OvnDBOptions
        if (this._opts.compressFn)
            engine.compressFn = this._opts.compressFn;
        if (this._opts.decompressFn)
            engine.decompressFn = this._opts.decompressFn;
        // SECURITY: propagate HMAC integrity key untuk manifest protection
        if (this._opts.integrityKey) {
            if (this._opts.integrityKey.length !== 32)
                throw new Error('[OvnDB] integrityKey harus tepat 32 bytes');
            engine.integrityKey = this._opts.integrityKey;
        }
        return engine;
    }
    _assertOpen() {
        if (this._closed)
            throw new Error('[OvnDB] Database sudah ditutup');
    }
}
// ── Re-exports ────────────────────────────────────────────────
export { Collection } from './collection/collection.js';
export { CollectionV2 } from './collection/collection-v2.js';
export { CryptoLayer, FieldCrypto, cryptoFromPassphrase, CRYPTO_OVERHEAD } from './crypto/crypto-layer.js';
export { Transaction, RollbackFailedError, WriteConflictError } from './core/transaction/transaction.js';
export { SchemaValidator, ValidationError, field } from './schema/schema-validator.js';
export { TTLIndex } from './ttl/ttl-index.js';
export { Observability, getObservability, resetObservability } from './observability/observability.js';
export { RelationManager } from './collection/relation-manager.js';
export { ChangeStream, ChangeStreamRegistry } from './collection/change-stream.js';
export { MigrationRunner } from './migration/migration-runner.js';
export { generateId, idToTimestamp, isValidId } from './utils/id-generator.js';
export { makeLogger } from './utils/logger.js';
export { FileLock } from './utils/file-lock.js';
// v4.0 new exports
export { FTSIndex } from './core/index/fts-index.js';
export { BloomFilter } from './core/storage/bloom-filter.js';
export { exportTo, importFrom } from './collection/import-export.js';
//# sourceMappingURL=index.js.map