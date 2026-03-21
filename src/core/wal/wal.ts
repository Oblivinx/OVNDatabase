// ============================================================
//  OvnDB v2.0 — WAL v2 (Write-Ahead Log) dengan Group Commit
//
//  IMPROVEMENT DARI v1:
//   v1: satu fdatasync() per operasi — throughput ~5K ops/s
//   v2: group commit — kumpulkan N operasi atau tunggu T ms,
//       lalu satu fdatasync() untuk semua → throughput ~50K+ ops/s
//
//  Cara kerja group commit:
//   1. append() masukkan operasi ke pending queue & kembalikan Promise
//   2. Timer (WAL_GROUP_WAIT_MS) atau threshold (WAL_GROUP_SIZE)
//      memicu _doGroupCommit()
//   3. _doGroupCommit() tulis semua pending dalam satu Buffer.concat()
//      lalu satu fdatasync()
//   4. Semua Promise dari step 1 di-resolve sekaligus
//
//  Keuntungan: satu disk sync melayani ratusan concurrent writers.
//  Tradeoff: latensi +5ms per operasi di worst case (WAL_GROUP_WAIT_MS).
// ============================================================

import fs   from 'fs';
import fsp  from 'fs/promises';
import path from 'path';
import {
  WAL_MAGIC, WAL_GROUP_SIZE, WAL_GROUP_WAIT_MS, WalOp,
} from '../../types/constants.js';
import { crc32, writeCrc, readCrc } from '../../utils/crc32.js';
import { makeLogger } from '../../utils/logger.js';

const log = makeLogger('wal');

export interface WalEntry {
  seqno:  bigint;
  op:     WalOp;
  key:    string;
  data:   Buffer;
  txId:   bigint;
}

interface PendingGroup {
  entries: Buffer[];
  resolvers: Array<() => void>;
  rejecters: Array<(e: Error) => void>;
}

export class WAL {
  private readonly filePath: string;
  private fd:            number | null = null;
  private seqno:         bigint = 0n;
  private writePos:      number = 0;
  private _pendingCount: number = 0;

  // Group commit state
  private group: PendingGroup = { entries: [], resolvers: [], rejecters: [] };
  private groupTimer: ReturnType<typeof setTimeout> | null = null;
  private _committing = false;

  constructor(dirPath: string, collectionName: string) {
    this.filePath = path.join(dirPath, `${collectionName}.wal`);
  }

  // ── Lifecycle ─────────────────────────────────────────────

  async open(): Promise<WalEntry[]> {
    if (!fs.existsSync(this.filePath)) fs.writeFileSync(this.filePath, Buffer.alloc(0));
    this.fd       = fs.openSync(this.filePath, 'r+');
    const entries = this._replay();
    this.writePos = fs.fstatSync(this.fd).size;
    log.debug(`WAL opened, ${entries.length} entries to replay`, { file: this.filePath });
    return entries;
  }

  async close(): Promise<void> {
    if (this.groupTimer) { clearTimeout(this.groupTimer); this.groupTimer = null; }
    await this._doGroupCommit(); // flush sisa pending
    if (this.fd !== null) { fs.closeSync(this.fd); this.fd = null; }
  }

  // ── Append ────────────────────────────────────────────────

  /**
   * Tambahkan entry ke WAL. Kembalikan Promise yang resolve setelah
   * entry ini di-flush ke disk (lewat group commit).
   *
   * Caller bisa await ini untuk durabilitas penuh, atau fire-and-forget
   * untuk throughput maksimum (dengan risiko kehilangan data saat crash).
   */
  append(op: WalOp, key: string, data: Buffer = Buffer.alloc(0), txId: bigint = 0n): Promise<void> {
    if (this.fd === null) return Promise.reject(new Error('WAL not open'));

    this.seqno++;
    const keyBuf = Buffer.from(key, 'utf8');
    const size   = 4 + 8 + 8 + 1 + 2 + keyBuf.length + 4 + data.length + 4;
    //             magic seqno txId op keyLen key dataLen data crc
    const buf    = Buffer.allocUnsafe(size);
    let p = 0;
    WAL_MAGIC.copy(buf, p);               p += 4;
    buf.writeBigUInt64LE(this.seqno, p);  p += 8;
    buf.writeBigUInt64LE(txId, p);        p += 8;
    buf.writeUInt8(op, p);                p += 1;
    buf.writeUInt16LE(keyBuf.length, p);  p += 2;
    keyBuf.copy(buf, p);                  p += keyBuf.length;
    buf.writeUInt32LE(data.length, p);    p += 4;
    data.copy(buf, p);                    p += data.length;
    writeCrc(buf, p, crc32(buf.subarray(0, p)));
    this._pendingCount++;

    return new Promise<void>((resolve, reject) => {
      this.group.entries.push(buf);
      this.group.resolvers.push(resolve);
      this.group.rejecters.push(reject);

      // Trigger commit jika threshold terpenuhi
      if (this.group.entries.length >= WAL_GROUP_SIZE) {
        if (this.groupTimer) { clearTimeout(this.groupTimer); this.groupTimer = null; }
        setImmediate(() => this._doGroupCommit());
      } else if (!this.groupTimer) {
        // Set timer untuk commit setelah WAL_GROUP_WAIT_MS jika belum ada
        this.groupTimer = setTimeout(() => {
          this.groupTimer = null;
          this._doGroupCommit();
        }, WAL_GROUP_WAIT_MS);
      }
    });
  }

  /**
   * Checkpoint: hapus isi WAL setelah semua data sudah di-flush ke segment.
   * Dipanggil setelah setiap flush cycle.
   * Skip fdatasync jika WAL sudah kosong (tidak ada yang perlu di-sync).
   */
  async checkpoint(): Promise<void> {
    if (this.fd === null) return;
    // Optimasi: jika WAL sudah kosong, skip I/O sama sekali
    if (this.writePos === 0 && this._pendingCount === 0) return;
    fs.ftruncateSync(this.fd, 0);
    fs.fdatasyncSync(this.fd);
    this.writePos     = 0;
    this._pendingCount = 0;
    log.debug('WAL checkpointed');
  }

  get pending(): number { return this._pendingCount; }

  // ── Privates ──────────────────────────────────────────────

  private async _doGroupCommit(): Promise<void> {
    if (this._committing || this.group.entries.length === 0) return;
    this._committing = true;

    // Snapshot grup yang akan di-commit
    const { entries, resolvers, rejecters } = this.group;
    this.group = { entries: [], resolvers: [], rejecters: [] };

    try {
      const combined = Buffer.concat(entries);
      fs.writeSync(this.fd!, combined, 0, combined.length, this.writePos);
      this.writePos += combined.length;
      fs.fdatasyncSync(this.fd!);
      // Resolve semua promise dalam grup ini
      for (const r of resolvers) r();
    } catch (err) {
      for (const r of rejecters) r(err as Error);
    } finally {
      this._committing = false;
      // Ada pending baru yang masuk saat kita commit? Commit lagi.
      if (this.group.entries.length > 0) {
        setImmediate(() => this._doGroupCommit());
      }
    }
  }

  private _replay(): WalEntry[] {
    if (!this.fd) return [];
    const stat = fs.fstatSync(this.fd);
    if (stat.size === 0) return [];
    const raw  = Buffer.allocUnsafe(stat.size);
    fs.readSync(this.fd, raw, 0, stat.size, 0);

    const all: WalEntry[] = [];
    let pos = 0, maxSeq = 0n, lastCkpt = -1;

    while (pos < raw.length) {
      const start = pos;
      if (raw.length - pos < 24) break;
      if (!raw.subarray(pos, pos + 4).equals(WAL_MAGIC)) break;
      pos += 4;
      const seqno  = raw.readBigUInt64LE(pos); pos += 8;
      const txId   = raw.readBigUInt64LE(pos); pos += 8;
      const op     = raw.readUInt8(pos) as WalOp; pos += 1;
      const keyLen = raw.readUInt16LE(pos); pos += 2;
      if (pos + keyLen > raw.length) break;
      const key    = raw.toString('utf8', pos, pos + keyLen); pos += keyLen;
      if (pos + 4 > raw.length) break;
      const dataLen = raw.readUInt32LE(pos); pos += 4;
      if (pos + dataLen + 4 > raw.length) break;
      const data   = raw.subarray(pos, pos + dataLen); pos += dataLen;
      const stored = readCrc(raw, pos); pos += 4;
      if (stored !== crc32(raw.subarray(start, pos - 4))) {
        log.warn('WAL CRC mismatch, stopping replay at pos ' + start);
        break;
      }
      if (seqno > maxSeq) maxSeq = seqno;
      if (op === WalOp.CHECKPOINT) lastCkpt = all.length;
      all.push({ seqno, op, key, data: Buffer.from(data), txId });
    }

    this.seqno = maxSeq;
    const toReplay = lastCkpt >= 0 ? all.slice(lastCkpt + 1) : all;
    this._pendingCount = toReplay.length;
    return toReplay.filter(e => e.op !== WalOp.CHECKPOINT);
  }
}
