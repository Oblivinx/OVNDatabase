// ============================================================
//  OvnDB v2.0 — Segment Manager
//
//  MENGAPA DIBUTUHKAN:
//   v1 menyimpan semua data dalam SATU file .ovn.
//   Masalah utama: compact() harus rewrite seluruh file.
//   Untuk file 100GB, ini blok seluruh collection selama menit.
//
//  v2 memecah data menjadi SEGMENT files (max 512MB per segment):
//   - collection.seg-0000.ovn, .seg-0001.ovn, dst.
//   - collection.manifest.json mencatat metadata semua segment
//   - Compaction bekerja PER SEGMENT secara background
//   - Segment yang tidak di-compact tetap bisa dibaca/ditulis
//   - Mendukung petabyte data (unlimited segment count)
//
//  RecordPointer sekarang menyertakan segmentId sehingga engine
//  tahu di file mana suatu record berada.
// ============================================================

import fs   from 'fs';
import fsp  from 'fs/promises';
import path from 'path';
import {
  SEGMENT_SIZE, MANIFEST_FILE, COMPACTION_RATIO,
  OVN_MAGIC, HEADER_SIZE, REC_OVERHEAD, REC_PREFIX_SIZE, REC_STATUS_SIZE,
  REC_TXID_SIZE, REC_CRC_SIZE, RecordStatus, FileFlags,
} from '../../types/constants.js';
import type { SegmentMeta, CollectionManifest, RecordPointer } from '../../types/index.js';
import { crc32, writeCrc, readCrc } from '../../utils/crc32.js';
import { makeLogger } from '../../utils/logger.js';

const log = makeLogger('segment');

export class SegmentManager {
  private readonly dirPath:    string;
  private readonly collection: string;
  private manifest!: CollectionManifest;
  private fds: Map<number, number> = new Map(); // segId → file descriptor
  private _closed = false;

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
      const parsed = JSON.parse(raw);
      // BigInt tidak bisa JSON.parse langsung
      this.manifest = {
        ...parsed,
        totalLive: BigInt(parsed.totalLive ?? 0),
        totalDead: BigInt(parsed.totalDead ?? 0),
      };
    } else {
      this.manifest = {
        version:    2,
        collection: this.collection,
        flags:      FileFlags.NONE,
        segments:   [],
        createdAt:  Date.now(),
        updatedAt:  Date.now(),
        totalLive:  0n,
        totalDead:  0n,
      };
    }

    // Buka semua segment yang ada
    for (const seg of this.manifest.segments) {
      this._openSegment(seg.id);
    }

    // Pastikan ada active segment
    if (this.manifest.segments.length === 0) {
      await this._createNewSegment();
    }

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

  /**
   * Tulis satu record ke active segment.
   * @returns RecordPointer — posisi fisik record yang baru ditulis
   */
  writeRecord(data: Buffer, txId: bigint): RecordPointer {
    let active = this._activeSegment();
    // Buka segment baru jika active sudah penuh
    if (active.size + data.length + REC_OVERHEAD > SEGMENT_SIZE) {
      // Flush sync karena kita langsung open yang baru
      this._createNewSegmentSync();
      active = this._activeSegment();
    }

    const fd     = this._fd(active.id);
    const offset = active.size;
    const rec    = this._buildRecord(data, txId);
    fs.writeSync(fd, rec, 0, rec.length, HEADER_SIZE + offset);

    const ptr: RecordPointer = {
      segmentId: active.id,
      offset,
      totalSize: rec.length,
      dataSize:  data.length,
      txId,
    };

    active.size += rec.length;
    active.live++;
    this.manifest.totalLive++;
    this.manifest.updatedAt = Date.now();
    return ptr;
  }

  /**
   * Tandai record sebagai DELETED (soft delete — tulis byte 0x00 di status field).
   */
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

  /**
   * Baca payload record dari disk.
   * @returns Buffer payload, atau null jika record deleted/corrupt
   */
  readRecord(ptr: RecordPointer): Buffer | null {
    const fd = this._fd(ptr.segmentId);
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
    return Buffer.from(buf.subarray(REC_PREFIX_SIZE, dataEnd));
  }

  /**
   * Scan seluruh segment dari awal — yield setiap [id, ptr, data] yang aktif.
   * Digunakan untuk full-scan query & index rebuild.
   */
  async *scanAll(decryptFn?: (b: Buffer) => Buffer): AsyncIterableIterator<{ ptr: RecordPointer; data: Buffer }> {
    for (const seg of this.manifest.segments) {
      yield* this._scanSegment(seg.id, decryptFn);
    }
  }

  /** Scan satu segment saja. */
  async *_scanSegment(segId: number, decryptFn?: (b: Buffer) => Buffer): AsyncIterableIterator<{ ptr: RecordPointer; data: Buffer }> {
    const seg = this._seg(segId);
    if (!seg || seg.size === 0) return;
    const fd = this._fd(segId);

    // Baca record satu per satu dari awal segment.
    // filePos = offset dari akhir header (0 = byte pertama setelah HEADER_SIZE).
    let filePos = 0;
    const prefixBuf = Buffer.allocUnsafe(REC_PREFIX_SIZE);

    while (filePos < seg.size) {
      // Baca prefix (status + txId + dataLen)
      const nr = fs.readSync(fd, prefixBuf, 0, REC_PREFIX_SIZE, HEADER_SIZE + filePos);
      if (nr < REC_PREFIX_SIZE) break;

      const status  = prefixBuf.readUInt8(0);
      const txId    = prefixBuf.readBigUInt64LE(REC_STATUS_SIZE);
      const dataLen = prefixBuf.readUInt32LE(REC_STATUS_SIZE + REC_TXID_SIZE);
      const total   = REC_OVERHEAD + dataLen;

      // Pastikan record masih dalam batas segment
      if (filePos + total > seg.size) break;

      if (status === RecordStatus.ACTIVE) {
        // Baca payload + CRC
        const payloadBuf = Buffer.allocUnsafe(dataLen + REC_CRC_SIZE);
        const np = fs.readSync(fd, payloadBuf, 0, payloadBuf.length, HEADER_SIZE + filePos + REC_PREFIX_SIZE);
        if (np < payloadBuf.length) { filePos += total; continue; }

        // Verifikasi CRC: covers prefix + payload (tanpa CRC itu sendiri)
        const crcBuf = Buffer.allocUnsafe(REC_PREFIX_SIZE + dataLen);
        prefixBuf.copy(crcBuf, 0);
        payloadBuf.copy(crcBuf, REC_PREFIX_SIZE, 0, dataLen);
        const storedCrc = payloadBuf.readUInt32LE(dataLen);
        if (storedCrc === crc32(crcBuf)) {
          const rawPayload = payloadBuf.subarray(0, dataLen);
          const payload    = decryptFn ? decryptFn(Buffer.from(rawPayload)) : Buffer.from(rawPayload);
          yield {
            ptr: {
              segmentId: segId,
              offset:    filePos,   // offset relatif ke HEADER_SIZE (0-based)
              totalSize: total,
              dataSize:  dataLen,
              txId,
            },
            data: payload,
          };
        }
      }

      filePos += total;
    }
  }

  // ── Compaction ────────────────────────────────────────────

  /**
   * Periksa semua segment dan compact yang sudah melebihi threshold fragmentasi.
   * Compaction berjalan per-segment — segment lain tetap bisa diakses.
   * @returns daftar segmentId yang di-compact
   */
  async autoCompact(onPointerMoved: (oldPtr: RecordPointer, newPtr: RecordPointer) => void): Promise<number[]> {
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
    for await (const { ptr, data } of this._scanSegment(segId)) {
      const rec    = this._buildRecord(data, ptr.txId);
      fs.writeSync(tmpFd, rec, 0, rec.length, HEADER_SIZE + newOffset);
      const newPtr: RecordPointer = {
        segmentId: segId,
        offset:    newOffset,
        totalSize: rec.length,
        dataSize:  data.length,
        txId:      ptr.txId,
      };
      onPointerMoved(ptr, newPtr);
      newOffset += rec.length;
    }
    fs.fdatasyncSync(tmpFd);
    fs.closeSync(tmpFd);

    // Atomic rename
    const oldFd = this._fd(segId);
    fs.closeSync(oldFd);
    fs.renameSync(tmpPath, this._segPath(segId));
    const newFd = fs.openSync(this._segPath(segId), 'r+');
    this.fds.set(segId, newFd);

    const seg  = this._seg(segId)!;
    seg.size   = newOffset;
    seg.dead   = 0;
    // live count tidak berubah (record yang sama, hanya dipadatkan)
    this.manifest.totalDead -= BigInt(seg.dead);
    this.manifest.updatedAt = Date.now();
  }

  // ── Stats ─────────────────────────────────────────────────

  get totalLive(): bigint      { return this.manifest.totalLive; }
  get totalDead(): bigint      { return this.manifest.totalDead; }
  get segmentCount(): number   { return this.manifest.segments.length; }
  get totalFileSize(): number  { return this.manifest.segments.reduce((s, seg) => s + seg.size + HEADER_SIZE, 0); }

  get fragmentRatio(): number {
    const live = Number(this.manifest.totalLive);
    const dead = Number(this.manifest.totalDead);
    const total = live + dead;
    return total > 0 ? dead / total : 0;
  }

  async saveManifest(): Promise<void> { await this._saveManifest(); }

  fdatasyncActive(): void {
    const active = this._activeSegment();
    const fd = this._fd(active.id);
    fs.fdatasyncSync(fd);
  }

  // ── Privates ──────────────────────────────────────────────

  private _buildRecord(data: Buffer, txId: bigint): Buffer {
    const buf = Buffer.allocUnsafe(REC_OVERHEAD + data.length);
    buf.writeUInt8(RecordStatus.ACTIVE, 0);
    buf.writeBigUInt64LE(txId,    REC_STATUS_SIZE);
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
    const p  = this._segPath(id);
    const fd = fs.openSync(p, 'r+');
    this.fds.set(id, fd);
  }

  private async _createNewSegment(): Promise<void> {
    const id   = this.manifest.segments.length;
    const p    = this._segPath(id);
    const fd   = fs.openSync(p, 'w+');
    const hdr  = Buffer.alloc(HEADER_SIZE);
    OVN_MAGIC.copy(hdr, 0);
    fs.writeSync(fd, hdr, 0, HEADER_SIZE, 0);
    fs.fdatasyncSync(fd);
    this.fds.set(id, fd);
    const meta: SegmentMeta = { id, path: p, size: 0, live: 0, dead: 0, fragmentation: 0 };
    this.manifest.segments.push(meta);
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
    // Simpan manifest secara sync
    const data = JSON.stringify({ ...this.manifest, totalLive: String(this.manifest.totalLive), totalDead: String(this.manifest.totalDead) }, null, 2);
    fs.writeFileSync(this._manifestPath(), data, 'utf8');
  }

  private async _saveManifest(): Promise<void> {
    // BigInt tidak bisa diserialisasi JSON langsung
    const data = JSON.stringify({
      ...this.manifest,
      totalLive: String(this.manifest.totalLive),
      totalDead: String(this.manifest.totalDead),
    }, null, 2);
    // Atomic write lewat temp file
    const tmp = this._manifestPath() + '.tmp';
    await fsp.writeFile(tmp, data, 'utf8');
    await fsp.rename(tmp, this._manifestPath());
  }
}
