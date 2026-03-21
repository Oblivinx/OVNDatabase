// ============================================================
//  StorageEngine v2 — binary file I/O, WAL, B+ Tree, LRU Cache
// ============================================================
import fs   from 'fs';
import fsp  from 'fs/promises';
import path from 'path';

import {
  OVN_MAGIC, HEADER_SIZE, FLUSH_INTERVAL_MS, FLUSH_THRESHOLD,
  COMPACTION_RATIO, DEFAULT_CACHE_SIZE,
  RecordStatus, WalOp, FileFlags,
  RECORD_PREFIX_SIZE, RECORD_OVERHEAD,
  type FileHeader, type RecordPointer, type PendingWrite, type OvnStats,
} from '../types.js';
import { crc32, writeCrc, readCrc } from '../utils/crc32.js';
import { BPlusTree }                from './btree.js';
import { LRUCache }                 from './lru-cache.js';
import { WAL }                      from './wal.js';
import { makeLogger }               from '../utils/logger.js';

const VERSION = 1;
const log     = makeLogger('storage');

export class StorageEngine {
  private readonly filePath:   string;
  private readonly indexPath:  string;
  private readonly wal:        WAL;
  readonly tree:               BPlusTree;           // exposed for index rebuild
  private readonly cache:      LRUCache<string, Buffer>;
  private fd:      number | null = null;
  private header:  FileHeader = {
    version: VERSION, flags: FileFlags.NONE,
    recordCount: 0, liveCount: 0,
    dataEnd: HEADER_SIZE,
    createdAt: Date.now(), updatedAt: Date.now(),
  };
  private readonly writeBuffer: Map<string, PendingWrite> = new Map();
  private flushTimer:    ReturnType<typeof setInterval> | null = null;
  private _flushPromise: Promise<void> | null = null;
  private _closed = false;

  /**
   * Optional decrypt hook — injected by CollectionV2 when encryption is enabled.
   * Used by scan() and _rebuildIndex() to extract _id from encrypted payloads.
   */
  decryptFn?: (buf: Buffer) => Buffer;

  constructor(dirPath: string, collection: string, cacheSize = DEFAULT_CACHE_SIZE) {
    this.filePath  = path.join(dirPath, `${collection}.ovn`);
    this.indexPath = path.join(dirPath, `${collection}.ovni`);
    this.wal       = new WAL(dirPath, collection);
    this.tree      = new BPlusTree();
    this.cache     = new LRUCache<string, Buffer>(cacheSize);
  }

  // ── Lifecycle ─────────────────────────────────────────────

  async open(): Promise<void> {
    // Touch file, then open r+ (avoid O_APPEND which breaks pwrite on Linux)
    if (!fs.existsSync(this.filePath)) fs.writeFileSync(this.filePath, Buffer.alloc(0));
    this.fd = fs.openSync(this.filePath, 'r+');
    const stat = fs.fstatSync(this.fd);
    if (stat.size < HEADER_SIZE) this._writeHeader();
    else this._readHeader();

    await this._loadIndex();

    const walEntries = await this.wal.open();
    if (walEntries.length > 0) {
      log.info(`Replaying ${walEntries.length} WAL entries`, { file: this.filePath });
      for (const e of walEntries) {
        if (e.op === WalOp.DELETE) this._applyDeleteToBuffer(e.key);
        else this._bufferWrite(e.key, e.data, e.op);
      }
      await this._flush();
    }

    this.flushTimer = setInterval(() => {
      if (this.writeBuffer.size > 0 && !this._closed)
        this._flush().catch(err => log.error('flush error', { err: String(err) }));
    }, FLUSH_INTERVAL_MS);
    (this.flushTimer as unknown as { unref?: () => void }).unref?.();
  }

  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    if (this.flushTimer) { clearInterval(this.flushTimer); this.flushTimer = null; }
    if (this.writeBuffer.size > 0) await this._flush();
    await this._saveIndex();
    await this.wal.close();
    if (this.fd !== null) { fs.closeSync(this.fd); this.fd = null; }
  }

  // ── Writes ────────────────────────────────────────────────

  async insert(id: string, data: Buffer): Promise<void> {
    if (this._liveExists(id)) throw new Error(`[OvnDB] Duplicate _id: ${id}`);
    await this.wal.append(WalOp.INSERT, id, data);
    this._bufferWrite(id, data, WalOp.INSERT);
    if (this.writeBuffer.size >= FLUSH_THRESHOLD) await this._flush();
  }

  async update(id: string, data: Buffer): Promise<void> {
    if (!this._liveExists(id)) throw new Error(`[OvnDB] Record not found: ${id}`);
    await this.wal.append(WalOp.UPDATE, id, data);
    this._bufferWrite(id, data, WalOp.UPDATE);
    if (this.writeBuffer.size >= FLUSH_THRESHOLD) await this._flush();
  }

  async upsert(id: string, data: Buffer): Promise<void> {
    const op = this._liveExists(id) ? WalOp.UPDATE : WalOp.INSERT;
    await this.wal.append(op, id, data);
    this._bufferWrite(id, data, op);
    if (this.writeBuffer.size >= FLUSH_THRESHOLD) await this._flush();
  }

  async delete(id: string): Promise<boolean> {
    if (!this._liveExists(id)) return false;
    await this.wal.append(WalOp.DELETE, id);
    this._applyDeleteToBuffer(id);
    if (this.writeBuffer.size >= FLUSH_THRESHOLD) await this._flush();
    return true;
  }

  // ── Reads ─────────────────────────────────────────────────

  async read(id: string): Promise<Buffer | null> {
    const pending = this.writeBuffer.get(id);
    if (pending) return pending.op === WalOp.DELETE ? null : pending.data;
    const cached = this.cache.get(id);
    if (cached) return cached;
    const ptr = this.tree.get(id);
    if (!ptr) return null;
    return this._readFromDisk(id, ptr);
  }

  private readonly SCAN_CHUNK = 2 * 1024 * 1024; // 2MB read buffer

  async *scan(): AsyncIterableIterator<[string, Buffer]> {
    const seen = new Set<string>();

    // Write buffer first (pre-flush records not yet on disk)
    for (const [id, pending] of this.writeBuffer) {
      seen.add(id);
      if (pending.op !== WalOp.DELETE) yield [id, pending.data];
    }

    if (this.fd === null) return;
    const fileEnd    = this.header.dataEnd;
    let   filePos    = HEADER_SIZE;
    const chunkBuf   = Buffer.allocUnsafe(this.SCAN_CHUNK);
    let   chunkStart = -1;
    let   chunkEnd   = -1;

    const ensureLoaded = (offset: number, needed: number): boolean => {
      if (offset >= chunkStart && offset + needed <= chunkEnd) return true;
      chunkStart = offset;
      const toRead = Math.min(this.SCAN_CHUNK, fileEnd - offset);
      if (toRead <= 0) return false;
      const n = fs.readSync(this.fd!, chunkBuf, 0, toRead, offset);
      chunkEnd = chunkStart + n;
      return n >= needed;
    };

    while (filePos < fileEnd) {
      if (!ensureLoaded(filePos, RECORD_PREFIX_SIZE)) break;
      const lo      = filePos - chunkStart;
      const status  = chunkBuf.readUInt8(lo);
      const dataLen = chunkBuf.readUInt32LE(lo + 1);
      const total   = RECORD_OVERHEAD + dataLen;

      if (status !== RecordStatus.ACTIVE) { filePos += total; continue; }
      if (!ensureLoaded(filePos, total)) break;

      const loff    = filePos - chunkStart;
      const payload = chunkBuf.subarray(loff + RECORD_PREFIX_SIZE, loff + RECORD_PREFIX_SIZE + dataLen);
      const stored  = chunkBuf.readUInt32LE(loff + RECORD_PREFIX_SIZE + dataLen);
      if (stored !== crc32(chunkBuf.subarray(loff, loff + RECORD_PREFIX_SIZE + dataLen))) {
        filePos += total; continue;
      }

      let id: string | undefined;
      try {
        // If encryption is enabled, decrypt to extract _id for index rebuild.
        // The yielded raw bytes remain encrypted — CollectionV2.scan decrypts on read.
        const parseable = this.decryptFn
          ? this.decryptFn(Buffer.from(payload))
          : payload;
        const doc = JSON.parse(parseable.toString('utf8')) as { _id?: string };
        id = doc._id;
      } catch { filePos += total; continue; }

      if (!id || seen.has(id)) { filePos += total; continue; }
      seen.add(id);
      const copy = Buffer.from(payload);
      this.cache.set(id, copy);
      yield [id, copy];
      filePos += total;
    }
  }

  async *scanRange(gte?: string, lte?: string): AsyncIterableIterator<[string, Buffer]> {
    const seen = new Set<string>();
    for (const [id, pending] of this.writeBuffer) {
      if (gte && id < gte) continue;
      if (lte && id > lte) continue;
      seen.add(id);
      if (pending.op !== WalOp.DELETE) yield [id, pending.data];
    }
    for (const [id, ptr] of this.tree.range(gte, lte)) {
      if (seen.has(id)) continue;
      seen.add(id);
      const cached = this.cache.get(id);
      if (cached) { yield [id, cached]; continue; }
      const raw = this._readFromDisk(id, ptr);
      if (raw) yield [id, raw];
    }
  }

  // ── Maintenance ───────────────────────────────────────────

  async compact(): Promise<void> {
    if (this.fd === null) return;
    if (this.writeBuffer.size > 0) await this._flush();
    const stat    = fs.fstatSync(this.fd);
    const liveEst = this.tree.size * (RECORD_OVERHEAD + 80);
    const frag    = Math.max(0, stat.size - HEADER_SIZE - liveEst) / Math.max(stat.size, 1);
    if (frag < COMPACTION_RATIO) return;

    log.info(`Compacting (frag ${(frag * 100).toFixed(1)}%)`, { file: this.filePath });
    const tmpPath = this.filePath + '.compact';
    const tmpFd   = fs.openSync(tmpPath, 'w');
    let newEnd    = HEADER_SIZE;
    fs.writeSync(tmpFd, Buffer.alloc(HEADER_SIZE), 0, HEADER_SIZE, 0);

    for (const [id, ptr] of this.tree.entries()) {
      const raw = this._readFromDisk(id, ptr);
      if (!raw) continue;
      const rec = this._buildRecord(raw);
      fs.writeSync(tmpFd, rec, 0, rec.length, newEnd);
      this.tree.set(id, { offset: newEnd, totalSize: rec.length, dataSize: raw.length });
      newEnd += rec.length;
    }
    fs.closeSync(tmpFd);
    fs.closeSync(this.fd);
    fs.renameSync(tmpPath, this.filePath);
    this.fd = fs.openSync(this.filePath, 'r+');
    this.header.dataEnd     = newEnd;
    this.header.recordCount = this.tree.size;
    this.header.liveCount   = this.tree.size;
    this.header.updatedAt   = Date.now();
    this._writeHeader();
    await this.wal.checkpoint();
    await this._saveIndex();
    this.cache.clear();
    log.info(`Compact done — ${(newEnd / 1024 / 1024).toFixed(2)} MB`);
  }

  async flush(): Promise<void> { return this._flush(); }

  async stats(collection: string): Promise<OvnStats> {
    const stat = this.fd ? fs.fstatSync(this.fd) : { size: 0 };
    const liveEst = this.tree.size * (RECORD_OVERHEAD + 80);
    return {
      collection,
      recordCount:   this.header.recordCount,
      liveCount:     this.header.liveCount + this.writeBuffer.size,
      fileSize:      stat.size,
      fragmentRatio: Math.max(0, stat.size - HEADER_SIZE - liveEst) / Math.max(stat.size, 1),
      cacheSize:     this.cache.size,
      cacheHitRate:  this.cache.hitRate,
      indexEntries:  this.tree.size + [...this.writeBuffer.values()].filter(p => p.op !== WalOp.DELETE).length,
      walPending:    this.wal.pending,
    };
  }

  // ── Privates ──────────────────────────────────────────────

  private _liveExists(id: string): boolean {
    const pending = this.writeBuffer.get(id);
    if (pending) return pending.op !== WalOp.DELETE;
    return this.tree.has(id);
  }

  private _bufferWrite(id: string, data: Buffer, op: WalOp): void {
    this.writeBuffer.set(id, { id, data, op });
    this.cache.set(id, data);
  }

  private _applyDeleteToBuffer(id: string): void {
    // Simpan disk offset ke dalam data buffer SEBELUM hapus dari tree.
    // _doFlush membutuhkan offset ini untuk menulis byte DELETED ke disk.
    const ptr = this.tree.get(id);
    const offsetBuf = Buffer.allocUnsafe(8);
    if (ptr) {
      offsetBuf.writeDoubleBE(ptr.offset, 0);
    } else {
      offsetBuf.writeDoubleBE(-1, 0); // tidak ada di disk (hanya di buffer)
    }
    this.writeBuffer.set(id, { id, data: offsetBuf, op: WalOp.DELETE });
    this.cache.delete(id);
    this.tree.delete(id);
    this.header.liveCount = Math.max(0, this.header.liveCount - 1);
  }

  private _flush(): Promise<void> {
    if (this._flushPromise) return this._flushPromise;
    this._flushPromise = this._doFlush().finally(() => { this._flushPromise = null; });
    return this._flushPromise;
  }

  private async _doFlush(): Promise<void> {
    if (this.writeBuffer.size === 0 || this.fd === null) return;
    const entries = [...this.writeBuffer.entries()];
    this.writeBuffer.clear();

    for (const [id, { data, op }] of entries) {
      if (op === WalOp.DELETE) {
        // Offset disimpan di data buffer (8 bytes BigEndian double).
        // tree.delete(id) sudah dipanggil di _applyDeleteToBuffer — jangan panggil lagi.
        const diskOffset = data.length === 8 ? data.readDoubleBE(0) : -1;
        if (diskOffset >= HEADER_SIZE) {
          fs.writeSync(this.fd!, Buffer.from([RecordStatus.DELETED]), 0, 1, diskOffset);
        }
        // Jangan panggil tree.delete(id) lagi — sudah di-delete di _applyDeleteToBuffer
      } else {
        const rec    = this._buildRecord(data);
        const offset = this.header.dataEnd;
        fs.writeSync(this.fd!, rec, 0, rec.length, offset);
        this.tree.set(id, { offset, totalSize: rec.length, dataSize: data.length });
        this.header.dataEnd     += rec.length;
        this.header.recordCount += 1;
        if (op === WalOp.INSERT) this.header.liveCount += 1;
      }
    }
    this.header.updatedAt = Date.now();
    this._writeHeader();
    fs.fdatasyncSync(this.fd!);
    await this.wal.checkpoint();
  }

  private _buildRecord(data: Buffer): Buffer {
    const buf = Buffer.allocUnsafe(RECORD_OVERHEAD + data.length);
    buf.writeUInt8(RecordStatus.ACTIVE, 0);
    buf.writeUInt32LE(data.length, 1);
    data.copy(buf, RECORD_PREFIX_SIZE);
    writeCrc(buf, RECORD_PREFIX_SIZE + data.length,
      crc32(buf.subarray(0, RECORD_PREFIX_SIZE + data.length)));
    return buf;
  }

  private _readFromDisk(id: string, ptr: RecordPointer): Buffer | null {
    if (this.fd === null) return null;
    const raw = Buffer.allocUnsafe(ptr.totalSize);
    const n   = fs.readSync(this.fd, raw, 0, ptr.totalSize, ptr.offset);
    if (n < ptr.totalSize) return null;
    if (raw.readUInt8(0) === RecordStatus.DELETED) return null;
    const dataLen = raw.readUInt32LE(1);
    const end     = RECORD_PREFIX_SIZE + dataLen;
    if (readCrc(raw, end) !== crc32(raw.subarray(0, end)))
      throw new Error(`[OvnDB] CRC mismatch record "${id}" @ offset ${ptr.offset}`);
    const payload = Buffer.from(raw.subarray(RECORD_PREFIX_SIZE, end));
    this.cache.set(id, payload);
    return payload;
  }

  private _writeHeader(): void {
    if (this.fd === null) return;
    const h = Buffer.alloc(HEADER_SIZE);
    let pos = 0;
    OVN_MAGIC.copy(h, pos);                       pos += 4;
    h.writeUInt16LE(this.header.version, pos);     pos += 2;
    h.writeUInt16LE(this.header.flags,   pos);     pos += 2;
    h.writeUInt32LE(this.header.recordCount, pos); pos += 4;
    h.writeUInt32LE(this.header.liveCount,   pos); pos += 4;
    h.writeDoubleBE(this.header.dataEnd,     pos); pos += 8;
    h.writeDoubleBE(this.header.createdAt,   pos); pos += 8;
    h.writeDoubleBE(this.header.updatedAt,   pos); pos += 8;
    writeCrc(h, 60, crc32(h.subarray(0, 60)));
    fs.writeSync(this.fd, h, 0, HEADER_SIZE, 0);
  }

  private _readHeader(): void {
    if (this.fd === null) return;
    const h = Buffer.allocUnsafe(HEADER_SIZE);
    fs.readSync(this.fd, h, 0, HEADER_SIZE, 0);
    if (!h.subarray(0, 4).equals(OVN_MAGIC))
      throw new Error(`[OvnDB] Invalid magic bytes: ${this.filePath}`);
    if (readCrc(h, 60) !== crc32(h.subarray(0, 60)))
      throw new Error('[OvnDB] Header CRC mismatch — file may be corrupted');
    let pos = 4;
    this.header.version     = h.readUInt16LE(pos); pos += 2;
    this.header.flags       = h.readUInt16LE(pos); pos += 2;
    this.header.recordCount = h.readUInt32LE(pos); pos += 4;
    this.header.liveCount   = h.readUInt32LE(pos); pos += 4;
    this.header.dataEnd     = h.readDoubleBE(pos); pos += 8;
    this.header.createdAt   = h.readDoubleBE(pos); pos += 8;
    this.header.updatedAt   = h.readDoubleBE(pos);
  }

  private async _saveIndex(): Promise<void> {
    await fsp.writeFile(this.indexPath, this.tree.toBuffer());
  }

  private async _loadIndex(): Promise<void> {
    try {
      const buf = await fsp.readFile(this.indexPath);
      if (buf.length >= 4) this.tree.fromBuffer(buf);
    } catch {
      if (this.fd !== null && this.header.dataEnd > HEADER_SIZE)
        await this._rebuildIndex();
    }
  }

  private async _rebuildIndex(): Promise<void> {
    if (this.fd === null) return;
    log.info('Rebuilding index from data file…', { file: this.filePath });
    let pos = HEADER_SIZE;
    while (pos < this.header.dataEnd) {
      const prefix = Buffer.allocUnsafe(RECORD_PREFIX_SIZE);
      if (fs.readSync(this.fd, prefix, 0, RECORD_PREFIX_SIZE, pos) < RECORD_PREFIX_SIZE) break;
      const status  = prefix.readUInt8(0);
      const dataLen = prefix.readUInt32LE(1);
      const total   = RECORD_OVERHEAD + dataLen;
      if (status === RecordStatus.ACTIVE) {
        const dataBuf = Buffer.allocUnsafe(dataLen);
        fs.readSync(this.fd, dataBuf, 0, dataLen, pos + RECORD_PREFIX_SIZE);
        try {
          // Decrypt if needed to extract _id
          const parseable = this.decryptFn ? this.decryptFn(dataBuf) : dataBuf;
          const doc = JSON.parse(parseable.toString('utf8')) as { _id?: string };
          if (doc._id) this.tree.set(doc._id, { offset: pos, totalSize: total, dataSize: dataLen });
        } catch { /* skip corrupt or wrong-key records */ }
      }
      pos += total;
    }
    log.info(`Index rebuilt — ${this.tree.size} records`);
    await this._saveIndex();
  }
}
