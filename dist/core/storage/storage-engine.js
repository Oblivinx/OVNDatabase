// ============================================================
//  OvnDB v3.0 — Storage Engine
//
//  G2 FIX: deleteAll() sekarang O(1) — tree.clear() + markAllDeleted()
//  G3 FIX: autoCompact pass _id via callback bukan full tree scan O(n²)
//  G4 FIX: writeBuffer byte limit (MAX_BUFFER_BYTES = 64MB)
//  G15 integrate: pass compressFn/decompressFn ke SegmentManager
// ============================================================
import fsp from 'fs/promises';
import path from 'path';
import { FLUSH_INTERVAL_MS, FLUSH_THRESHOLD, BULK_FLUSH_THRESHOLD, MAX_CACHE_BYTES, MAX_BUFFER_BYTES, COMPACTION_CHECK_MS, } from '../../types/constants.js';
import { LRUCache } from '../cache/lru-cache.js';
import { SegmentManager } from './segment-manager.js';
import { PageManager } from './page-manager.js';
import { PagedBPlusTree } from '../index/btree-paged.js';
import { WAL } from '../wal/wal.js';
import { MVCCManager } from '../transaction/mvcc.js';
import { makeLogger } from '../../utils/logger.js';
const log = makeLogger('storage-engine');
export class StorageEngine {
    dirPath;
    colName;
    segments;
    pageManager;
    tree;
    wal;
    mvcc;
    // G1: LRU cache dengan byte limit
    cache;
    writeBuffer = new Map();
    // G4: track byte usage di writeBuffer
    _bufferBytes = 0;
    _secondaryIdx;
    flushTimer = null;
    compactTimer = null;
    _flushPromise = null;
    _closed = false;
    _bulkMode = false;
    /** G15: inject compress/decompress functions */
    compressFn;
    decompressFn;
    /** Inject decrypt function untuk CollectionV2 */
    decryptFn;
    /**
     * SECURITY: inject 32-byte HMAC key untuk manifest integrity verification.
     * Jika di-set, manifest ditulis/dibaca dengan HMAC-SHA256 bukan SHA-256 plain.
     * Derive dari passphrase yang sama dengan encryption key menggunakan HKDF:
     *   engine.integrityKey = crypto.hkdfSync('sha256', masterKey, salt,
     *     Buffer.from('ovndb-manifest-hmac'), 32);
     */
    integrityKey;
    constructor(dirPath, colName, cacheBytes = MAX_CACHE_BYTES) {
        this.dirPath = dirPath;
        this.colName = colName;
        this.segments = new SegmentManager(dirPath, colName);
        this.pageManager = new PageManager(path.join(dirPath, `${colName}.ovni`));
        this.tree = new PagedBPlusTree(this.pageManager);
        this.wal = new WAL(dirPath, colName);
        this.mvcc = new MVCCManager();
        // G1: byte-based LRU
        this.cache = new LRUCache(cacheBytes);
    }
    // ── Lifecycle ─────────────────────────────────────────────
    async open() {
        await fsp.mkdir(this.dirPath, { recursive: true });
        // G15: propagate compression hooks ke SegmentManager sebelum open
        if (this.compressFn)
            this.segments.compressFn = this.compressFn;
        if (this.decompressFn)
            this.segments.decompressFn = this.decompressFn;
        // SECURITY: propagate integrityKey untuk HMAC manifest verification
        if (this.integrityKey)
            this.segments.integrityKey = this.integrityKey;
        await this.segments.open();
        await this.pageManager.open();
        await this.tree.init();
        const walEntries = await this.wal.open();
        if (walEntries.length > 0) {
            log.info(`Replaying ${walEntries.length} WAL entries`, { col: this.colName });
            for (const e of walEntries) {
                this.writeBuffer.set(e.key, { id: e.key, data: e.data, op: e.op, txId: e.txId });
                this._bufferBytes += e.data.length;
            }
            await this._doFlush();
        }
        this.flushTimer = setInterval(() => {
            if (this.writeBuffer.size > 0 && !this._closed)
                this._flush().catch(err => log.error('periodic flush error', { err: String(err) }));
        }, FLUSH_INTERVAL_MS);
        this.flushTimer.unref?.();
        this.compactTimer = setInterval(() => {
            if (!this._closed)
                this._autoCompact();
        }, COMPACTION_CHECK_MS);
        this.compactTimer.unref?.();
        log.info(`StorageEngine opened`, {
            col: this.colName, segments: this.segments.segmentCount,
            liveCount: String(this.segments.totalLive),
        });
    }
    async close() {
        if (this._closed)
            return;
        this._closed = true;
        if (this.flushTimer) {
            clearInterval(this.flushTimer);
            this.flushTimer = null;
        }
        if (this.compactTimer) {
            clearInterval(this.compactTimer);
            this.compactTimer = null;
        }
        if (this.writeBuffer.size > 0)
            await this._doFlush();
        await this.pageManager.flushDirty();
        await this.pageManager.close();
        if (this._secondaryIdx)
            await this._secondaryIdx.save();
        await this.wal.checkpoint();
        await this.segments.close();
        log.info(`StorageEngine closed`, { col: this.colName });
    }
    // ── Secondary Index ───────────────────────────────────────
    setSecondaryIndex(mgr) { this._secondaryIdx = mgr; }
    // ── Bulk Load API ────────────────────────────────────────
    beginBulkLoad() {
        this._bulkMode = true;
        log.info('Bulk-load mode ON', { col: this.colName });
    }
    async endBulkLoad() {
        this._bulkMode = false;
        if (this.writeBuffer.size > 0)
            await this._doFlush(true);
        await this.pageManager.flushDirty();
        await this.wal.checkpoint();
        if (this._secondaryIdx)
            await this._secondaryIdx.save();
        log.info('Bulk-load mode OFF — data flushed to disk', { col: this.colName });
    }
    // ── Write API ─────────────────────────────────────────────
    async insertBulk(id, data, txId) {
        const tid = txId ?? this.mvcc.autoCommitTxId();
        this._bufferWrite(id, data, 1 /* WalOp.INSERT */, tid);
        const threshold = this._bulkMode ? BULK_FLUSH_THRESHOLD : FLUSH_THRESHOLD;
        if (this.writeBuffer.size >= threshold || this._bufferBytes >= MAX_BUFFER_BYTES) {
            await this._flush();
        }
    }
    async insert(id, data, txId) {
        if (await this._liveExists(id))
            throw new Error(`[OvnDB] Duplicate _id`);
        const tid = txId ?? this.mvcc.autoCommitTxId();
        this.mvcc.recordWrite(tid, id);
        await this.wal.append(1 /* WalOp.INSERT */, id, data, tid);
        this._bufferWrite(id, data, 1 /* WalOp.INSERT */, tid);
        // G4: flush jika melebihi count ATAU bytes threshold
        if (this.writeBuffer.size >= FLUSH_THRESHOLD || this._bufferBytes >= MAX_BUFFER_BYTES) {
            await this._flush();
        }
    }
    async update(id, data, txId) {
        if (!await this._liveExists(id))
            throw new Error(`[OvnDB] Record not found`);
        const tid = txId ?? this.mvcc.autoCommitTxId();
        this.mvcc.recordWrite(tid, id);
        await this.wal.append(2 /* WalOp.UPDATE */, id, data, tid);
        this._bufferWrite(id, data, 2 /* WalOp.UPDATE */, tid);
        if (this.writeBuffer.size >= FLUSH_THRESHOLD || this._bufferBytes >= MAX_BUFFER_BYTES) {
            await this._flush();
        }
    }
    async upsert(id, data, txId) {
        const exists = await this._liveExists(id);
        const op = exists ? 2 /* WalOp.UPDATE */ : 1 /* WalOp.INSERT */;
        const tid = txId ?? this.mvcc.autoCommitTxId();
        this.mvcc.recordWrite(tid, id);
        await this.wal.append(op, id, data, tid);
        this._bufferWrite(id, data, op, tid);
        if (this.writeBuffer.size >= FLUSH_THRESHOLD || this._bufferBytes >= MAX_BUFFER_BYTES) {
            await this._flush();
        }
    }
    async delete(id, txId) {
        if (!await this._liveExists(id))
            return false;
        const tid = txId ?? this.mvcc.autoCommitTxId();
        this.mvcc.recordWrite(tid, id);
        await this.wal.append(3 /* WalOp.DELETE */, id, Buffer.alloc(0), tid);
        this._bufferWrite(id, Buffer.alloc(0), 3 /* WalOp.DELETE */, tid);
        this.cache.delete(id);
        if (this.writeBuffer.size >= FLUSH_THRESHOLD || this._bufferBytes >= MAX_BUFFER_BYTES) {
            await this._flush();
        }
        return true;
    }
    /**
     * G2: deleteAll() — O(1) path.
     * Alih-alih loop per record, kita:
     * 1. Flush pending buffer
     * 2. B+ Tree.clear() — reset ke root leaf kosong (O(1))
     * 3. SegmentManager.markAllDeleted() — set status DELETED per segment (1 pass per segment)
     * 4. Clear LRU cache dan writeBuffer
     */
    async deleteAll() {
        if (this.writeBuffer.size > 0)
            await this._doFlush();
        // G7: tree.clear() O(1) via PageManager.reset()
        await this.tree.clear();
        // FIX: pakai markAllDeletedAsync() agar event loop tidak ter-freeze
        // untuk collection besar. Yield setiap 1000 records + antar segment.
        await this.segments.markAllDeletedAsync();
        this.writeBuffer.clear();
        this._bufferBytes = 0;
        this.cache.clear();
        this.segments.fdatasyncActive();
        await this.wal.checkpoint();
        await this.pageManager.flushDirty();
        log.info(`deleteAll complete`, { col: this.colName });
    }
    // ── Read API ──────────────────────────────────────────────
    async read(id) {
        const pending = this.writeBuffer.get(id);
        if (pending) {
            if (pending.op === 3 /* WalOp.DELETE */)
                return null;
            // writeBuffer menyimpan ciphertext/compressed — perlu decrypt
            return this.decryptFn ? this.decryptFn(pending.data) : pending.data;
        }
        const cached = this.cache.get(id);
        if (cached)
            return cached;
        const ptr = await this.tree.get(id);
        if (!ptr)
            return null;
        return this._readFromSegment(id, ptr);
    }
    async *scan(decryptFn) {
        const seen = new Set();
        const decrypt = decryptFn ?? this.decryptFn;
        // 1. writeBuffer — paling fresh, decrypt agar konsisten
        for (const [id, pending] of this.writeBuffer) {
            seen.add(id);
            if (pending.op !== 3 /* WalOp.DELETE */) {
                const buf = decrypt ? decrypt(pending.data) : pending.data;
                yield [id, buf];
            }
        }
        // 2. Segment scan
        for await (const { data } of this.segments.scanAll(decrypt)) {
            try {
                const doc = JSON.parse(data.toString('utf8'));
                const id = doc._id;
                if (!id || seen.has(id))
                    continue;
                seen.add(id);
                this.cache.set(id, data);
                yield [id, data];
            }
            catch {
                continue;
            }
        }
    }
    async *scanRange(gte, lte) {
        const seen = new Set();
        const decrypt = this.decryptFn;
        for (const [id, pending] of this.writeBuffer) {
            if (gte && id < gte)
                continue;
            if (lte && id > lte)
                continue;
            seen.add(id);
            if (pending.op !== 3 /* WalOp.DELETE */) {
                const buf = decrypt ? decrypt(pending.data) : pending.data;
                yield [id, buf];
            }
        }
        for await (const [id, ptr] of this.tree.range(gte, lte)) {
            if (seen.has(id))
                continue;
            seen.add(id);
            const data = await this._readFromSegment(id, ptr);
            if (data)
                yield [id, data];
        }
    }
    async flush() { return this._doFlush(true); }
    async forceCompact() {
        if (this.writeBuffer.size > 0)
            await this._doFlush();
        await new Promise(resolve => {
            this._autoCompact();
            setTimeout(async () => { await this.pageManager.flushDirty(); resolve(); }, 100);
        });
        log.info('Force compact triggered', { col: this.colName });
    }
    async backup(destPath) {
        if (this.writeBuffer.size > 0)
            await this._doFlush(true);
        await this.pageManager.flushDirty();
        await this.wal.checkpoint();
        // SECURITY: pastikan destPath tidak berada di dalam dirPath sendiri
        const resolvedDest = path.resolve(destPath);
        const resolvedSrc = path.resolve(this.dirPath);
        if (resolvedDest.startsWith(resolvedSrc)) {
            throw new Error('[OvnDB] Backup destination tidak boleh berada di dalam direktori sumber');
        }
        await fsp.mkdir(destPath, { recursive: true });
        const entries = await fsp.readdir(this.dirPath, { withFileTypes: true });
        await Promise.all(entries.filter(e => e.isFile()).map(e => fsp.copyFile(path.join(this.dirPath, e.name), path.join(destPath, e.name))));
        // SECURITY: tidak log path user ke output untuk menghindari log injection
        log.info(`Backup complete`, { col: this.colName });
    }
    async stats(collection) {
        return {
            collection,
            totalLive: this.segments.totalLive + BigInt(this.writeBuffer.size),
            totalDead: this.segments.totalDead,
            segmentCount: this.segments.segmentCount,
            totalFileSize: this.segments.totalFileSize,
            fragmentRatio: this.segments.fragmentRatio,
            cacheSize: this.cache.size,
            cacheHitRate: this.cache.hitRate,
            indexCount: Number(this.tree.size),
            walPending: this.wal.pending,
            bufferPoolUsed: this.pageManager.totalPages,
        };
    }
    // ── Privates ──────────────────────────────────────────────
    async _liveExists(id) {
        const pending = this.writeBuffer.get(id);
        if (pending)
            return pending.op !== 3 /* WalOp.DELETE */;
        return this.tree.has(id);
    }
    _bufferWrite(id, data, op, txId) {
        const existing = this.writeBuffer.get(id);
        if (existing)
            this._bufferBytes -= existing.data.length;
        this.writeBuffer.set(id, { id, data, op, txId });
        this._bufferBytes += data.length;
        if (op !== 3 /* WalOp.DELETE */) {
            // Cache plaintext untuk read performance
            try {
                const plain = this.decryptFn ? this.decryptFn(data) : data;
                this.cache.set(id, plain);
            }
            catch { /* decrypt bisa gagal di edge case */ }
        }
        else {
            this.cache.delete(id);
        }
    }
    _flush() {
        if (this._flushPromise)
            return this._flushPromise;
        const fullSync = !this._bulkMode;
        this._flushPromise = this._doFlush(fullSync).finally(() => { this._flushPromise = null; });
        return this._flushPromise;
    }
    async _doFlush(fullSync = true) {
        if (this.writeBuffer.size === 0)
            return;
        const entries = [...this.writeBuffer.entries()];
        this.writeBuffer.clear();
        this._bufferBytes = 0;
        for (const [id, { data, op, txId }] of entries) {
            if (op === 3 /* WalOp.DELETE */) {
                const ptr = await this.tree.get(id);
                if (ptr) {
                    this.segments.deleteRecord(ptr);
                    await this.tree.delete(id);
                }
            }
            else {
                if (op === 2 /* WalOp.UPDATE */) {
                    const oldPtr = await this.tree.get(id);
                    if (oldPtr)
                        this.segments.deleteRecord(oldPtr);
                }
                const ptr = this.segments.writeRecord(data, txId);
                await this.tree.set(id, ptr);
            }
        }
        if (fullSync) {
            this.segments.fdatasyncActive();
            await this.wal.checkpoint();
            await this.pageManager.flushDirty();
            if (this._secondaryIdx)
                await this._secondaryIdx.save();
        }
    }
    async _readFromSegment(id, ptr) {
        const raw = this.segments.readRecord(ptr);
        if (!raw)
            return null;
        // segments.readRecord sudah decompressed, tinggal decrypt
        const payload = this.decryptFn ? this.decryptFn(raw) : raw;
        this.cache.set(id, payload);
        return payload;
    }
    /**
     * G3: autoCompact callback menerima _id sekarang.
     * Constraint baru: SegmentManager.autoCompact menerima callback dengan signature baru
     * yang juga memberikan _id sehingga B+ Tree update jadi O(1) per record.
     * Karena SegmentManager lama tidak pass _id, kita scan data untuk extract _id.
     */
    _autoCompact() {
        this.segments.autoCompact(async (oldPtr, newPtr, data) => {
            // G3: data sudah tersedia dari _compactSegment — langsung extract _id
            // Tidak perlu readRecord(newPtr) karena temp file belum di-rename
            try {
                const plain = this.decryptFn ? this.decryptFn(data) : data;
                const doc = JSON.parse(plain.toString('utf8'));
                if (doc._id)
                    await this.tree.set(doc._id, newPtr);
            }
            catch {
                // Fallback: scan tree untuk temukan key yang cocok (rare case)
                for await (const [id, ptr] of this.tree.entries()) {
                    if (ptr.segmentId === oldPtr.segmentId && ptr.offset === oldPtr.offset) {
                        await this.tree.set(id, newPtr);
                        break;
                    }
                }
            }
        }).catch(err => log.error('auto-compact error', { err: String(err) }));
    }
}
//# sourceMappingURL=storage-engine.js.map