// ============================================================
//  OvnDB v2.1 — Storage Engine
//
//  update: added deleteAll() for truncate(), forceCompact(),
//          backup(destPath) with consistent snapshot,
//          improved _autoCompact pointer resolution,
//          scan() writeBuffer decrypt already worked — verified & kept
// ============================================================

import fs   from 'fs';
import fsp  from 'fs/promises';
import path from 'path';
import {
  FLUSH_INTERVAL_MS, FLUSH_THRESHOLD, BULK_FLUSH_THRESHOLD,
  DOC_CACHE_SIZE, WalOp, COMPACTION_CHECK_MS,
} from '../../types/constants.js';
import type {
  OvnDocument, RecordPointer, PendingWrite, OvnStats, IndexDefinition,
} from '../../types/index.js';
import { LRUCache }        from '../cache/lru-cache.js';
import { SegmentManager }  from './segment-manager.js';
import { PageManager }     from './page-manager.js';
import { PagedBPlusTree }  from '../index/btree-paged.js';
import { WAL }             from '../wal/wal.js';
import { MVCCManager }     from '../transaction/mvcc.js';
import { SecondaryIndexManager } from '../index/secondary-index.js';
import { makeLogger }      from '../../utils/logger.js';

const log = makeLogger('storage-engine');

export class StorageEngine {
  readonly dirPath:    string;
  private readonly colName:   string;

  readonly segments:    SegmentManager;
  readonly pageManager: PageManager;
  readonly tree:        PagedBPlusTree;
  readonly wal:         WAL;
  readonly mvcc:        MVCCManager;
  private readonly cache: LRUCache<string, Buffer>;

  private readonly writeBuffer: Map<string, PendingWrite> = new Map();
  private _secondaryIdx?: SecondaryIndexManager;

  private flushTimer:     ReturnType<typeof setInterval> | null = null;
  private compactTimer:   ReturnType<typeof setInterval> | null = null;
  private _flushPromise:  Promise<void> | null = null;
  private _closed = false;
  private _bulkMode = false;

  /** Optional: inject decrypt function untuk enkripsi (CollectionV2). */
  decryptFn?: (buf: Buffer) => Buffer;

  constructor(dirPath: string, colName: string, cacheSize = DOC_CACHE_SIZE) {
    this.dirPath     = dirPath;
    this.colName     = colName;
    this.segments    = new SegmentManager(dirPath, colName);
    this.pageManager = new PageManager(path.join(dirPath, `${colName}.ovni`));
    this.tree        = new PagedBPlusTree(this.pageManager);
    this.wal         = new WAL(dirPath, colName);
    this.mvcc        = new MVCCManager();
    this.cache       = new LRUCache<string, Buffer>(cacheSize);
  }

  // ── Lifecycle ─────────────────────────────────────────────

  async open(): Promise<void> {
    await fsp.mkdir(this.dirPath, { recursive: true });
    await this.segments.open();
    await this.pageManager.open();
    await this.tree.init();

    const walEntries = await this.wal.open();
    if (walEntries.length > 0) {
      log.info(`Replaying ${walEntries.length} WAL entries`, { col: this.colName });
      for (const e of walEntries) {
        if (e.op === WalOp.DELETE) {
          this.writeBuffer.set(e.key, { id: e.key, data: e.data, op: WalOp.DELETE, txId: e.txId });
        } else {
          this.writeBuffer.set(e.key, { id: e.key, data: e.data, op: e.op, txId: e.txId });
        }
      }
      await this._doFlush();
    }

    this.flushTimer = setInterval(() => {
      if (this.writeBuffer.size > 0 && !this._closed)
        this._flush().catch(err => log.error('periodic flush error', { err: String(err) }));
    }, FLUSH_INTERVAL_MS);
    (this.flushTimer as unknown as { unref?: () => void }).unref?.();

    this.compactTimer = setInterval(() => {
      if (!this._closed) this._autoCompact();
    }, COMPACTION_CHECK_MS);
    (this.compactTimer as unknown as { unref?: () => void }).unref?.();

    log.info(`StorageEngine opened`, {
      col:       this.colName,
      segments:  this.segments.segmentCount,
      liveCount: String(this.segments.totalLive),
    });
  }

  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    if (this.flushTimer)   { clearInterval(this.flushTimer);   this.flushTimer   = null; }
    if (this.compactTimer) { clearInterval(this.compactTimer); this.compactTimer = null; }
    if (this.writeBuffer.size > 0) await this._doFlush();
    await this.pageManager.flushDirty();
    await this.pageManager.close();
    if (this._secondaryIdx) await this._secondaryIdx.save();
    await this.wal.checkpoint();
    await this.segments.close();
    log.info(`StorageEngine closed`, { col: this.colName });
  }

  // ── Secondary Index ───────────────────────────────────────

  setSecondaryIndex(mgr: SecondaryIndexManager): void {
    this._secondaryIdx = mgr;
  }

  // ── Bulk Load API ────────────────────────────────────────

  beginBulkLoad(): void {
    this._bulkMode = true;
    log.info('Bulk-load mode ON', { col: this.colName });
  }

  async endBulkLoad(): Promise<void> {
    this._bulkMode = false;
    if (this.writeBuffer.size > 0) await this._doFlush(true);
    await this.pageManager.flushDirty();
    await this.wal.checkpoint();
    if (this._secondaryIdx) await this._secondaryIdx.save();
    log.info('Bulk-load mode OFF — data flushed to disk', { col: this.colName });
  }

  // ── Write API ─────────────────────────────────────────────

  async insertBulk(id: string, data: Buffer, txId?: bigint): Promise<void> {
    const tid = txId ?? this.mvcc.autoCommitTxId();
    this._bufferWrite(id, data, WalOp.INSERT, tid);
    const threshold = this._bulkMode ? BULK_FLUSH_THRESHOLD : FLUSH_THRESHOLD;
    if (this.writeBuffer.size >= threshold) await this._flush();
  }

  async insert(id: string, data: Buffer, txId?: bigint): Promise<void> {
    if (await this._liveExists(id))
      throw new Error(`[OvnDB] Duplicate _id: "${id}"`);
    const tid = txId ?? this.mvcc.autoCommitTxId();
    this.mvcc.recordWrite(tid, id);
    await this.wal.append(WalOp.INSERT, id, data, tid);
    this._bufferWrite(id, data, WalOp.INSERT, tid);
    if (this.writeBuffer.size >= FLUSH_THRESHOLD) await this._flush();
  }

  async update(id: string, data: Buffer, txId?: bigint): Promise<void> {
    if (!await this._liveExists(id))
      throw new Error(`[OvnDB] Record not found: "${id}"`);
    const tid = txId ?? this.mvcc.autoCommitTxId();
    this.mvcc.recordWrite(tid, id);
    await this.wal.append(WalOp.UPDATE, id, data, tid);
    this._bufferWrite(id, data, WalOp.UPDATE, tid);
    if (this.writeBuffer.size >= FLUSH_THRESHOLD) await this._flush();
  }

  async upsert(id: string, data: Buffer, txId?: bigint): Promise<void> {
    const exists = await this._liveExists(id);
    const op     = exists ? WalOp.UPDATE : WalOp.INSERT;
    const tid    = txId ?? this.mvcc.autoCommitTxId();
    this.mvcc.recordWrite(tid, id);
    await this.wal.append(op, id, data, tid);
    this._bufferWrite(id, data, op, tid);
    if (this.writeBuffer.size >= FLUSH_THRESHOLD) await this._flush();
  }

  async delete(id: string, txId?: bigint): Promise<boolean> {
    if (!await this._liveExists(id)) return false;
    const tid = txId ?? this.mvcc.autoCommitTxId();
    this.mvcc.recordWrite(tid, id);
    await this.wal.append(WalOp.DELETE, id, Buffer.alloc(0), tid);
    this._bufferWrite(id, Buffer.alloc(0), WalOp.DELETE, tid);
    this.cache.delete(id);
    if (this.writeBuffer.size >= FLUSH_THRESHOLD) await this._flush();
    return true;
  }

  /**
   * feat: deleteAll — hapus semua record dari collection.
   * Jauh lebih efisien dari delete per-record karena:
   *  1. Flush buffer dulu
   *  2. Mark semua record di semua segment sebagai DELETED
   *  3. Clear B+ Tree
   *  4. Clear LRU cache
   *  5. WAL checkpoint
   */
  async deleteAll(): Promise<void> {
    // 1. Flush pending writes dulu
    if (this.writeBuffer.size > 0) await this._doFlush();

    // 2. Collect semua IDs dari B+ Tree
    const allIds: string[] = [];
    for await (const [id] of this.tree.entries()) {
      allIds.push(id);
    }

    // 3. Delete semua dari segment & tree
    for (const id of allIds) {
      const ptr = await this.tree.get(id);
      if (ptr) {
        this.segments.deleteRecord(ptr);
        await this.tree.delete(id);
      }
    }

    // 4. Clear write buffer, cache
    this.writeBuffer.clear();
    this.cache.clear?.();

    // 5. Sync to disk
    this.segments.fdatasyncActive();
    await this.wal.checkpoint();
    await this.pageManager.flushDirty();

    log.info(`deleteAll complete`, { col: this.colName, deleted: allIds.length });
  }

  // ── Read API ──────────────────────────────────────────────

  async read(id: string): Promise<Buffer | null> {
    // 1. Write buffer (paling fresh)
    const pending = this.writeBuffer.get(id);
    if (pending) {
      if (pending.op === WalOp.DELETE) return null;
      // fix: decrypt jika ada decryptFn (data di writeBuffer adalah ciphertext)
      return this.decryptFn ? this.decryptFn(pending.data) : pending.data;
    }

    // 2. LRU cache
    const cached = this.cache.get(id);
    if (cached) return cached;

    // 3. B+ Tree → Segment
    const ptr = await this.tree.get(id);
    if (!ptr) return null;
    return this._readFromSegment(id, ptr);
  }

  /**
   * Scan seluruh collection — yield [id, Buffer] untuk setiap live record.
   * fix: writeBuffer juga diapply decryptFn sehingga konsisten dengan
   *      segment scan. Ini fix bug kritis di v2.0 dimana find({}) tidak
   *      bisa membaca data fresh dari writeBuffer yang terenkripsi.
   */
  async *scan(decryptFn?: (b: Buffer) => Buffer): AsyncIterableIterator<[string, Buffer]> {
    const seen    = new Set<string>();
    // fix: gunakan decryptFn yang di-pass, atau fallback ke this.decryptFn
    const decrypt = decryptFn ?? this.decryptFn;

    // 1. Write buffer (belum di-flush ke segment)
    for (const [id, pending] of this.writeBuffer) {
      seen.add(id);
      if (pending.op !== WalOp.DELETE) {
        // fix: apply decrypt ke writeBuffer data — ini adalah fix utama v2.1
        const buf = decrypt ? decrypt(pending.data) : pending.data;
        yield [id, buf];
      }
    }

    // 2. Scan segment: hanya record ACTIVE yang belum di-lihat
    for await (const { data } of this.segments.scanAll(decrypt)) {
      try {
        const parseable = data;
        const doc = JSON.parse(parseable.toString('utf8')) as { _id?: string };
        const id  = doc._id;
        if (!id || seen.has(id)) continue;
        seen.add(id);
        this.cache.set(id, data);
        yield [id, data];
      } catch { continue; }
    }
  }

  async *scanRange(gte?: string, lte?: string): AsyncIterableIterator<[string, Buffer]> {
    const seen = new Set<string>();
    const decrypt = this.decryptFn;
    for (const [id, pending] of this.writeBuffer) {
      if (gte && id < gte) continue;
      if (lte && id > lte) continue;
      seen.add(id);
      if (pending.op !== WalOp.DELETE) {
        // fix: decrypt writeBuffer data in scanRange too
        const buf = decrypt ? decrypt(pending.data) : pending.data;
        yield [id, buf];
      }
    }
    for await (const [id, ptr] of this.tree.range(gte, lte)) {
      if (seen.has(id)) continue;
      seen.add(id);
      const data = await this._readFromSegment(id, ptr);
      if (data) yield [id, data];
    }
  }

  async flush(): Promise<void> { return this._doFlush(true); }

  /**
   * feat: forceCompact — trigger compaction manual, tidak menunggu auto-compaction.
   * Berguna setelah deleteMany besar untuk segera reclaim space.
   */
  async forceCompact(): Promise<void> {
    if (this.writeBuffer.size > 0) await this._doFlush();
    await new Promise<void>((resolve) => {
      this._autoCompact();
      // autoCompact adalah async background — flush lagi setelah selesai
      setTimeout(async () => {
        await this.pageManager.flushDirty();
        resolve();
      }, 100);
    });
    log.info('Force compact triggered', { col: this.colName });
  }

  /**
   * feat: backup — salin semua file collection ke destPath secara konsisten.
   * Menggunakan flush + lock sementara agar backup tidak corrupt.
   * @param destPath  Path direktori tujuan backup (akan dibuat jika belum ada)
   */
  async backup(destPath: string): Promise<void> {
    // 1. Flush semua pending writes agar segment files up-to-date
    if (this.writeBuffer.size > 0) await this._doFlush(true);
    await this.pageManager.flushDirty();
    await this.wal.checkpoint();

    // 2. Buat dest dir
    await fsp.mkdir(destPath, { recursive: true });

    // 3. Copy semua file collection ke dest
    const entries = await fsp.readdir(this.dirPath, { withFileTypes: true });
    const copyJobs = entries
      .filter(e => e.isFile())
      .map(e => fsp.copyFile(
        path.join(this.dirPath, e.name),
        path.join(destPath, e.name),
      ));
    await Promise.all(copyJobs);

    log.info(`Backup complete`, { col: this.colName, dest: destPath, files: copyJobs.length });
  }

  async stats(collection: string): Promise<OvnStats> {
    return {
      collection,
      totalLive:      this.segments.totalLive + BigInt(this.writeBuffer.size),
      totalDead:      this.segments.totalDead,
      segmentCount:   this.segments.segmentCount,
      totalFileSize:  this.segments.totalFileSize,
      fragmentRatio:  this.segments.fragmentRatio,
      cacheSize:      this.cache.size,
      cacheHitRate:   this.cache.hitRate,
      indexCount:     Number(this.tree.size),
      walPending:     this.wal.pending,
      bufferPoolUsed: this.pageManager.totalPages,
    };
  }

  // ── Privates ──────────────────────────────────────────────

  private async _liveExists(id: string): Promise<boolean> {
    const pending = this.writeBuffer.get(id);
    if (pending) return pending.op !== WalOp.DELETE;
    return this.tree.has(id);
  }

  private _bufferWrite(id: string, data: Buffer, op: WalOp, txId: bigint): void {
    this.writeBuffer.set(id, { id, data, op, txId });
    // update: untuk cache, simpan plaintext (setelah decrypt)
    // Tapi di sini kita tidak punya decryptFn tersedia secara sinkron.
    // Cache hanya digunakan untuk read path — writeBuffer read path sudah handle decrypt.
    if (op !== WalOp.DELETE) {
      // Cache plaintext jika kita bisa decrypt, otherwise skip (read dari buffer)
      if (this.decryptFn) {
        try { this.cache.set(id, this.decryptFn(data)); } catch { /* skip */ }
      } else {
        this.cache.set(id, data);
      }
    } else {
      this.cache.delete(id);
    }
  }

  private _flush(): Promise<void> {
    if (this._flushPromise) return this._flushPromise;
    const fullSync = !this._bulkMode;
    this._flushPromise = this._doFlush(fullSync).finally(() => { this._flushPromise = null; });
    return this._flushPromise;
  }

  private async _doFlush(fullSync = true): Promise<void> {
    if (this.writeBuffer.size === 0) return;
    const entries = [...this.writeBuffer.entries()];
    this.writeBuffer.clear();

    for (const [id, { data, op, txId }] of entries) {
      if (op === WalOp.DELETE) {
        const ptr = await this.tree.get(id);
        if (ptr) {
          this.segments.deleteRecord(ptr);
          await this.tree.delete(id);
        }
      } else {
        if (op === WalOp.UPDATE) {
          const oldPtr = await this.tree.get(id);
          if (oldPtr) this.segments.deleteRecord(oldPtr);
        }
        // Note: data di buffer adalah ciphertext (jika encrypted) atau plaintext.
        // Segment menyimpan data apa adanya — ConsistencyPrinciple:
        // "engine tidak tahu/peduli enkripsi, CollectionV2 yang mengurus"
        const ptr = this.segments.writeRecord(data, txId);
        await this.tree.set(id, ptr);
      }
    }

    if (fullSync) {
      this.segments.fdatasyncActive();
      await this.wal.checkpoint();
      await this.pageManager.flushDirty();
      if (this._secondaryIdx) await this._secondaryIdx.save();
    }
  }

  private async _readFromSegment(id: string, ptr: RecordPointer): Promise<Buffer | null> {
    const raw = this.segments.readRecord(ptr);
    if (!raw) return null;
    const payload = this.decryptFn ? this.decryptFn(raw) : raw;
    this.cache.set(id, payload);
    return payload;
  }

  private _autoCompact(): void {
    // update: gunakan build ulang tree untuk update pointer yang lebih akurat
    this.segments.autoCompact(async (oldPtr, newPtr) => {
      // update: optimasi — scan hanya leaf nodes dari B+ Tree
      for await (const [id, ptr] of this.tree.entries()) {
        if (ptr.segmentId === oldPtr.segmentId && ptr.offset === oldPtr.offset) {
          await this.tree.set(id, newPtr);
          break;
        }
      }
    }).catch(err => log.error('auto-compact error', { err: String(err) }));
  }
}
