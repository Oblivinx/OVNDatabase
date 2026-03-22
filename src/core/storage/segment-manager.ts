// ============================================================
//  OvnDB v3.0 — Segment Manager
//
//  G15 FIX: Kompresi per-record via compressFn/decompressFn hooks.
//           FileFlags.COMPRESSED sekarang diimplementasi penuh.
//  G16 FIX: scanAll() support fromSegment/fromOffset untuk partial scan.
//  G17 FIX: Manifest checksum SHA-256 — detect corruption saat open().
//           markAllDeleted() untuk deleteAll() O(1) path.
//
//  v3.1 FIXES:
//  - Tambah markAllDeletedAsync(): yield event loop setiap 1000 records (fix event loop freeze)
// ============================================================

import fs   from 'fs';
import fsp  from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import {
  SEGMENT_SIZE, MANIFEST_FILE, COMPACTION_RATIO,
  OVN_MAGIC, HEADER_SIZE, REC_OVERHEAD, REC_PREFIX_SIZE, REC_STATUS_SIZE,
  REC_TXID_SIZE, REC_CRC_SIZE, RecordStatus, FileFlags,
  MANIFEST_CHECKSUM_ALGO,
} from '../../types/constants.js';
import type { SegmentMeta, CollectionManifest, RecordPointer } from '../../types/index.js';
import { crc32, writeCrc, readCrc } from '../../utils/crc32.js';
import { makeLogger } from '../../utils/logger.js';

const log = makeLogger('segment');

export class SegmentManager {
  private readonly dirPath:    string;
  private readonly collection: string;
  private manifest!: CollectionManifest;
  private fds: Map<number, number> = new Map();
  private _closed = false;

  // G15: optional compression hooks
  compressFn?:   (buf: Buffer) => Buffer;
  decompressFn?: (buf: Buffer) => Buffer;

  constructor(dirPath: string, collection: string) {
    this.dirPath    = dirPath;
    this.collection = collection;
  }

  // ── Lifecycle ─────────────────────────────────────────────

  async open(): Promise<void> {
    await fsp.mkdir(this.dirPath, { recursive: true });
    const manifestPath = this._manifestPath();

    if (fs.existsSync(manifestPath)) {
      const raw    = await fsp.readFile(manifestPath, 'utf8');
      const parsed = JSON.parse(raw) as Record<string, unknown>;

      // G17: verifikasi checksum manifest
      if (parsed['checksum']) {
        const storedChecksum = parsed['checksum'] as string;
        const { checksum: _cs, ...rest } = parsed;
        const content = JSON.stringify(rest, null, 2);
        const computed = crypto.createHash(MANIFEST_CHECKSUM_ALGO).update(content).digest('hex');
        if (computed !== storedChecksum) {
          throw new Error(
            `[SegmentManager] Manifest checksum mismatch for "${this.collection}" — ` +
            `possible corruption. Restore from backup or delete manifest to rebuild.`,
          );
        }
      }

      this.manifest = {
        ...parsed,
        totalLive: BigInt((parsed['totalLive'] as string) ?? '0'),
        totalDead: BigInt((parsed['totalDead'] as string) ?? '0'),
      } as unknown as CollectionManifest;
    } else {
      this.manifest = {
        version:    3,
        collection: this.collection,
        flags:      FileFlags.NONE,
        segments:   [],
        createdAt:  Date.now(),
        updatedAt:  Date.now(),
        totalLive:  0n,
        totalDead:  0n,
      };
    }

    for (const seg of this.manifest.segments) this._openSegment(seg.id);
    if (this.manifest.segments.length === 0) await this._createNewSegment();

    log.info(`Opened ${this.manifest.segments.length} segment(s)`, {
      collection: this.collection,
      totalLive:  String(this.manifest.totalLive),
    });
  }

  async close(): Promise<void> {
    if (this._closed) return;
    this._closed = true;
    await this._saveManifest();
    for (const [, fd] of this.fds) {
      try { fs.fdatasyncSync(fd); fs.closeSync(fd); } catch { /* ignore */ }
    }
    this.fds.clear();
  }

  // ── Write ─────────────────────────────────────────────────

  writeRecord(data: Buffer, txId: bigint): RecordPointer {
    // G15: kompres data sebelum tulis jika compressFn tersedia
    const payload = this.compressFn ? this.compressFn(data) : data;

    let active = this._activeSegment();
    if (active.size + payload.length + REC_OVERHEAD > SEGMENT_SIZE) {
      this._createNewSegmentSync();
      active = this._activeSegment();
    }

    const fd     = this._fd(active.id);
    const offset = active.size;
    const rec    = this._buildRecord(payload, txId);
    fs.writeSync(fd, rec, 0, rec.length, HEADER_SIZE + offset);

    const ptr: RecordPointer = {
      segmentId: active.id,
      offset,
      totalSize: rec.length,
      dataSize:  payload.length,
      txId,
    };
    active.size += rec.length;
    active.live++;
    this.manifest.totalLive++;
    this.manifest.updatedAt = Date.now();
    return ptr;
  }

  deleteRecord(ptr: RecordPointer): void {
    const fd = this._fd(ptr.segmentId);
    fs.writeSync(fd, Buffer.from([RecordStatus.DELETED]), 0, 1, HEADER_SIZE + ptr.offset);
    const seg = this._seg(ptr.segmentId);
    if (seg) { seg.dead++; seg.live = Math.max(0, seg.live - 1); }
    this.manifest.totalDead++;
    this.manifest.totalLive = this.manifest.totalLive > 0n
      ? this.manifest.totalLive - 1n : 0n;
    this.manifest.updatedAt = Date.now();
  }

  readRecord(ptr: RecordPointer): Buffer | null {
    const fd  = this._fd(ptr.segmentId);
    const buf = Buffer.allocUnsafe(ptr.totalSize);
    const n   = fs.readSync(fd, buf, 0, ptr.totalSize, HEADER_SIZE + ptr.offset);
    if (n < ptr.totalSize) return null;

    const status = buf.readUInt8(0);
    if (status === RecordStatus.DELETED) return null;

    const dataLen = buf.readUInt32LE(REC_STATUS_SIZE + REC_TXID_SIZE);
    const dataEnd = REC_PREFIX_SIZE + dataLen;
    const stored  = readCrc(buf, dataEnd);
    if (stored !== crc32(buf.subarray(0, dataEnd))) {
      log.warn(`CRC mismatch @ seg ${ptr.segmentId} offset ${ptr.offset}`);
      return null;
    }

    const raw = Buffer.from(buf.subarray(REC_PREFIX_SIZE, dataEnd));
    // G15: decompress jika perlu
    return this.decompressFn ? this.decompressFn(raw) : raw;
  }

  /**
   * G16: scanAll dengan support partial scan (cursor pagination).
   * @param opts.fromSegment  Mulai dari segment ID ini (inklusif)
   * @param opts.fromOffset   Mulai dari offset ini (hanya berlaku pada fromSegment)
   */
  async *scanAll(
    decryptFn?: (b: Buffer) => Buffer,
    opts?: { fromSegment?: number; fromOffset?: number },
  ): AsyncIterableIterator<{ ptr: RecordPointer; data: Buffer }> {
    for (const seg of this.manifest.segments) {
      if (opts?.fromSegment !== undefined && seg.id < opts.fromSegment) continue;
      const startOffset = (opts?.fromSegment === seg.id) ? (opts?.fromOffset ?? 0) : 0;
      yield* this._scanSegment(seg.id, decryptFn, startOffset);
    }
  }

  async *_scanSegment(
    segId: number,
    decryptFn?: (b: Buffer) => Buffer,
    startOffset = 0,
  ): AsyncIterableIterator<{ ptr: RecordPointer; data: Buffer }> {
    const seg = this._seg(segId);
    if (!seg || seg.size === 0) return;
    const fd = this._fd(segId);

    let filePos = startOffset;
    const prefixBuf = Buffer.allocUnsafe(REC_PREFIX_SIZE);

    while (filePos < seg.size) {
      const nr = fs.readSync(fd, prefixBuf, 0, REC_PREFIX_SIZE, HEADER_SIZE + filePos);
      if (nr < REC_PREFIX_SIZE) break;

      const status  = prefixBuf.readUInt8(0);
      const txId    = prefixBuf.readBigUInt64LE(REC_STATUS_SIZE);
      const dataLen = prefixBuf.readUInt32LE(REC_STATUS_SIZE + REC_TXID_SIZE);
      const total   = REC_OVERHEAD + dataLen;

      if (filePos + total > seg.size) break;

      if (status === RecordStatus.ACTIVE) {
        const payloadBuf = Buffer.allocUnsafe(dataLen + REC_CRC_SIZE);
        const np = fs.readSync(fd, payloadBuf, 0, payloadBuf.length, HEADER_SIZE + filePos + REC_PREFIX_SIZE);
        if (np < payloadBuf.length) { filePos += total; continue; }

        const crcBuf = Buffer.allocUnsafe(REC_PREFIX_SIZE + dataLen);
        prefixBuf.copy(crcBuf, 0);
        payloadBuf.copy(crcBuf, REC_PREFIX_SIZE, 0, dataLen);
        const storedCrc = payloadBuf.readUInt32LE(dataLen);
        if (storedCrc === crc32(crcBuf)) {
          let raw: Buffer = Buffer.from(payloadBuf.subarray(0, dataLen));
          // G15: decompress sebelum decrypt
          if (this.decompressFn) raw = this.decompressFn(raw);
          const data = decryptFn ? decryptFn(raw) : raw;
          yield {
            ptr: { segmentId: segId, offset: filePos, totalSize: total, dataSize: dataLen, txId },
            data,
          };
        }
      }
      filePos += total;
    }
  }

  // ── G17 helper: markAllDeleted() untuk deleteAll() ───

  /**
   * Tandai semua record sebagai DELETED — versi sync (untuk collection kecil).
   * Dipakai oleh StorageEngine.deleteAll().
   *
   * PERINGATAN: Untuk collection besar (>50K records), gunakan markAllDeletedAsync()
   * agar event loop tidak ter-freeze selama proses berlangsung.
   */
  markAllDeleted(): void {
    const deleted = Buffer.from([RecordStatus.DELETED]);
    for (const seg of this.manifest.segments) {
      if (seg.size === 0) continue;
      const fd        = this._fd(seg.id);
      const prefixBuf = Buffer.allocUnsafe(REC_PREFIX_SIZE);
      let pos = 0;

      while (pos < seg.size) {
        const nr = fs.readSync(fd, prefixBuf, 0, REC_PREFIX_SIZE, HEADER_SIZE + pos);
        if (nr < REC_PREFIX_SIZE) break;
        const status  = prefixBuf.readUInt8(0);
        const dataLen = prefixBuf.readUInt32LE(REC_STATUS_SIZE + REC_TXID_SIZE);
        const total   = REC_OVERHEAD + dataLen;
        if (pos + total > seg.size) break;
        if (status === RecordStatus.ACTIVE) {
          fs.writeSync(fd, deleted, 0, 1, HEADER_SIZE + pos);
        }
        pos += total;
      }

      this.manifest.totalDead += BigInt(seg.live);
      this.manifest.totalLive  = 0n;
      seg.dead += seg.live;
      seg.live  = 0;
    }
    this.manifest.updatedAt = Date.now();
  }

  /**
   * Versi async dari markAllDeleted() — tidak memblokir event loop.
   * Yield ke event loop setiap 1000 records dan antar segment.
   * Gunakan ini untuk collection besar (>50K dokumen) via deleteAll().
   */
  async markAllDeletedAsync(): Promise<void> {
    const deleted = Buffer.from([RecordStatus.DELETED]);

    for (const seg of this.manifest.segments) {
      if (seg.size === 0) continue;
      const fd        = this._fd(seg.id);
      const prefixBuf = Buffer.allocUnsafe(REC_PREFIX_SIZE);
      let pos       = 0;
      let processed = 0;

      while (pos < seg.size) {
        const nr = fs.readSync(fd, prefixBuf, 0, REC_PREFIX_SIZE, HEADER_SIZE + pos);
        if (nr < REC_PREFIX_SIZE) break;
        const status  = prefixBuf.readUInt8(0);
        const dataLen = prefixBuf.readUInt32LE(REC_STATUS_SIZE + REC_TXID_SIZE);
        const total   = REC_OVERHEAD + dataLen;
        if (pos + total > seg.size) break;

        if (status === RecordStatus.ACTIVE) {
          fs.writeSync(fd, deleted, 0, 1, HEADER_SIZE + pos);
          processed++;
          // Yield ke event loop setiap 1000 active records agar tidak freeze
          if (processed % 1000 === 0) {
            await new Promise<void>(r => setImmediate(r));
          }
        }
        pos += total;
      }

      this.manifest.totalDead += BigInt(seg.live);
      this.manifest.totalLive  = 0n;
      seg.dead += seg.live;
      seg.live  = 0;

      // Yield antar segment
      await new Promise<void>(r => setImmediate(r));
    }
    this.manifest.updatedAt = Date.now();
  }

  // ── Compaction ────────────────────────────────────────────

  async autoCompact(
    onPointerMoved: (oldPtr: RecordPointer, newPtr: RecordPointer) => void,
  ): Promise<number[]> {
    const compacted: number[] = [];
    for (const seg of [...this.manifest.segments]) {
      const frag = seg.live > 0 ? seg.dead / (seg.live + seg.dead) : 0;
      if (frag < COMPACTION_RATIO) continue;
      log.info(`Compacting segment ${seg.id} (frag ${(frag * 100).toFixed(1)}%)`, {
        collection: this.collection,
      });
      await this._compactSegment(seg.id, onPointerMoved);
      compacted.push(seg.id);
    }
    if (compacted.length > 0) await this._saveManifest();
    return compacted;
  }

  private async _compactSegment(
    segId: number,
    onPointerMoved: (old: RecordPointer, newPtr: RecordPointer) => void,
  ): Promise<void> {
    const tmpPath = this._segPath(segId) + '.compact';
    const tmpFd   = fs.openSync(tmpPath, 'w');
    const header  = Buffer.alloc(HEADER_SIZE);
    OVN_MAGIC.copy(header, 0);
    fs.writeSync(tmpFd, header, 0, HEADER_SIZE, 0);

    let newOffset = 0;
    // G15: saat compact, data sudah plaintext (dari _scanSegment yang decompresses)
    // Kita perlu re-compress saat menulis ulang
    for await (const { ptr, data } of this._scanSegment(segId)) {
      const payload = this.compressFn ? this.compressFn(data) : data;
      const rec     = this._buildRecord(payload, ptr.txId);
      fs.writeSync(tmpFd, rec, 0, rec.length, HEADER_SIZE + newOffset);
      const newPtr: RecordPointer = {
        segmentId: segId, offset: newOffset,
        totalSize: rec.length, dataSize: payload.length, txId: ptr.txId,
      };
      onPointerMoved(ptr, newPtr);
      newOffset += rec.length;
    }
    fs.fdatasyncSync(tmpFd);
    fs.closeSync(tmpFd);

    const oldFd = this._fd(segId);
    fs.closeSync(oldFd);
    fs.renameSync(tmpPath, this._segPath(segId));
    const newFd = fs.openSync(this._segPath(segId), 'r+');
    this.fds.set(segId, newFd);

    const seg  = this._seg(segId)!;
    const deadBefore = BigInt(seg.dead);
    seg.size   = newOffset;
    seg.dead   = 0;
    this.manifest.totalDead = this.manifest.totalDead >= deadBefore
      ? this.manifest.totalDead - deadBefore : 0n;
    this.manifest.updatedAt = Date.now();
  }

  // ── Stats ─────────────────────────────────────────────────

  get totalLive(): bigint      { return this.manifest.totalLive; }
  get totalDead(): bigint      { return this.manifest.totalDead; }
  get segmentCount(): number   { return this.manifest.segments.length; }
  get totalFileSize(): number  {
    return this.manifest.segments.reduce((s, seg) => s + seg.size + HEADER_SIZE, 0);
  }
  get fragmentRatio(): number {
    const live  = Number(this.manifest.totalLive);
    const dead  = Number(this.manifest.totalDead);
    const total = live + dead;
    return total > 0 ? dead / total : 0;
  }

  async saveManifest(): Promise<void> { await this._saveManifest(); }

  fdatasyncActive(): void {
    const active = this._activeSegment();
    fs.fdatasyncSync(this._fd(active.id));
  }

  // ── Privates ──────────────────────────────────────────────

  private _buildRecord(data: Buffer, txId: bigint): Buffer {
    const buf = Buffer.allocUnsafe(REC_OVERHEAD + data.length);
    buf.writeUInt8(RecordStatus.ACTIVE, 0);
    buf.writeBigUInt64LE(txId,     REC_STATUS_SIZE);
    buf.writeUInt32LE(data.length, REC_STATUS_SIZE + REC_TXID_SIZE);
    data.copy(buf, REC_PREFIX_SIZE);
    writeCrc(buf, REC_PREFIX_SIZE + data.length,
      crc32(buf.subarray(0, REC_PREFIX_SIZE + data.length)));
    return buf;
  }

  private _activeSegment(): SegmentMeta {
    return this.manifest.segments[this.manifest.segments.length - 1]!;
  }

  private _seg(id: number): SegmentMeta | undefined {
    return this.manifest.segments.find(s => s.id === id);
  }

  private _fd(segId: number): number {
    const fd = this.fds.get(segId);
    if (fd === undefined) throw new Error(`[SegmentManager] Segment ${segId} not open`);
    return fd;
  }

  private _segPath(id: number): string {
    return path.join(this.dirPath, `${this.collection}.seg-${String(id).padStart(4, '0')}.ovn`);
  }

  private _manifestPath(): string {
    return path.join(this.dirPath, `${this.collection}.${MANIFEST_FILE}`);
  }

  private _openSegment(id: number): void {
    this.fds.set(id, fs.openSync(this._segPath(id), 'r+'));
  }

  private async _createNewSegment(): Promise<void> {
    const id  = this.manifest.segments.length;
    const p   = this._segPath(id);
    const fd  = fs.openSync(p, 'w+');
    const hdr = Buffer.alloc(HEADER_SIZE);
    OVN_MAGIC.copy(hdr, 0);
    fs.writeSync(fd, hdr, 0, HEADER_SIZE, 0);
    fs.fdatasyncSync(fd);
    this.fds.set(id, fd);
    this.manifest.segments.push({ id, path: p, size: 0, live: 0, dead: 0, fragmentation: 0 });
    await this._saveManifest();
    log.debug(`Created segment ${id}`, { collection: this.collection });
  }

  private _createNewSegmentSync(): void {
    const id  = this.manifest.segments.length;
    const p   = this._segPath(id);
    const fd  = fs.openSync(p, 'w+');
    const hdr = Buffer.alloc(HEADER_SIZE);
    OVN_MAGIC.copy(hdr, 0);
    fs.writeSync(fd, hdr, 0, HEADER_SIZE, 0);
    fs.fdatasyncSync(fd);
    this.fds.set(id, fd);
    this.manifest.segments.push({ id, path: p, size: 0, live: 0, dead: 0, fragmentation: 0 });
    const tmp = this._buildManifestContent();
    fs.writeFileSync(this._manifestPath(), tmp.content, 'utf8');
  }

  /**
   * G17: Build manifest JSON dengan SHA-256 checksum.
   */
  private _buildManifestContent(): { content: string } {
    const base = {
      ...this.manifest,
      totalLive: String(this.manifest.totalLive),
      totalDead: String(this.manifest.totalDead),
    };
    // Hitung checksum dari content tanpa field checksum itu sendiri
    const withoutChecksum = JSON.stringify(base, null, 2);
    const checksum = crypto.createHash(MANIFEST_CHECKSUM_ALGO).update(withoutChecksum).digest('hex');
    const withChecksum = JSON.stringify({ ...base, checksum }, null, 2);
    return { content: withChecksum };
  }

  private async _saveManifest(): Promise<void> {
    const { content } = this._buildManifestContent();
    const tmp = this._manifestPath() + '.tmp';
    await fsp.writeFile(tmp, content, 'utf8');
    await fsp.rename(tmp, this._manifestPath());
  }
}
