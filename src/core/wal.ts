// WAL v4 — In-memory buffer, single writeSync per flush batch
import fs   from 'fs';
import fsp  from 'fs/promises';
import path from 'path';
import { WalOp, WAL_MAGIC } from '../types.js';
import { crc32, writeCrc, readCrc } from '../utils/crc32.js';
import { makeLogger } from '../utils/logger.js';

const log = makeLogger('wal');

export interface WalEntry {
  seqno: bigint;
  op:    WalOp;
  key:   string;
  data:  Buffer;
}

export class WAL {
  private readonly filePath: string;
  private fd:           number | null = null;
  private seqno:        bigint = 0n;
  private walWritePos:  number = 0;
  private pendingCount: number = 0;
  private readonly memBuf: Buffer[] = [];
  private memBufBytes = 0;

  constructor(dirPath: string, collectionName: string) {
    this.filePath = path.join(dirPath, `${collectionName}.wal`);
  }

  async open(): Promise<WalEntry[]> {
    if (!fs.existsSync(this.filePath)) fs.writeFileSync(this.filePath, Buffer.alloc(0));
    this.fd = fs.openSync(this.filePath, 'r+');
    const entries    = this._replay();
    this.walWritePos = fs.fstatSync(this.fd).size;
    return entries;
  }

  async close(): Promise<void> {
    if (this.memBuf.length > 0) await this._flushMemBuf();
    if (this.fd !== null) { fs.closeSync(this.fd); this.fd = null; }
  }

  async append(op: WalOp, key: string, data: Buffer = Buffer.alloc(0)): Promise<void> {
    if (this.fd === null) throw new Error('WAL not open');
    this.seqno++;
    const keyBuf = Buffer.from(key, 'utf8');
    const size   = 4 + 8 + 1 + 2 + keyBuf.length + 4 + data.length + 4;
    const buf    = Buffer.allocUnsafe(size);
    let p = 0;
    WAL_MAGIC.copy(buf, p);               p += 4;
    buf.writeBigUInt64LE(this.seqno, p);  p += 8;
    buf.writeUInt8(op, p);                p += 1;
    buf.writeUInt16LE(keyBuf.length, p);  p += 2;
    keyBuf.copy(buf, p);                  p += keyBuf.length;
    buf.writeUInt32LE(data.length, p);    p += 4;
    data.copy(buf, p);                    p += data.length;
    writeCrc(buf, p, crc32(buf.subarray(0, p)));
    this.memBuf.push(buf);
    this.memBufBytes += buf.length;
    this.pendingCount++;
  }

  async checkpoint(): Promise<void> {
    if (this.fd === null) return;
    await this._flushMemBuf();
    fs.fdatasyncSync(this.fd);
    fs.ftruncateSync(this.fd, 0);
    fs.fdatasyncSync(this.fd);
    this.walWritePos  = 0;
    this.pendingCount = 0;
  }

  get pending(): number { return this.pendingCount; }

  async destroy(): Promise<void> {
    await this.close();
    await fsp.unlink(this.filePath).catch(() => {});
  }

  private async _flushMemBuf(): Promise<void> {
    if (this.memBuf.length === 0 || this.fd === null) return;
    const combined = Buffer.concat(this.memBuf, this.memBufBytes);
    fs.writeSync(this.fd, combined, 0, combined.length, this.walWritePos);
    this.walWritePos   += combined.length;
    this.memBuf.length  = 0;
    this.memBufBytes    = 0;
  }

  private _replay(): WalEntry[] {
    const stat = fs.fstatSync(this.fd!);
    if (stat.size === 0) return [];
    const raw  = Buffer.allocUnsafe(stat.size);
    const read = fs.readSync(this.fd!, raw, 0, stat.size, 0);
    const buf  = raw.subarray(0, read);
    const all: WalEntry[] = [];
    let pos = 0, lastCkpt = -1, maxSeq = 0n;

    while (pos < buf.length) {
      const start = pos;
      if (buf.length - pos < 19) break;
      if (!buf.subarray(pos, pos + 4).equals(WAL_MAGIC)) break;
      pos += 4;
      const seqno   = buf.readBigUInt64LE(pos); pos += 8;
      const op      = buf.readUInt8(pos) as WalOp; pos += 1;
      const keyLen  = buf.readUInt16LE(pos); pos += 2;
      if (pos + keyLen > buf.length) break;
      const key     = buf.toString('utf8', pos, pos + keyLen); pos += keyLen;
      if (pos + 4 > buf.length) break;
      const dataLen = buf.readUInt32LE(pos); pos += 4;
      if (pos + dataLen + 4 > buf.length) break;
      const data    = buf.subarray(pos, pos + dataLen); pos += dataLen;
      const stored  = readCrc(buf, pos); pos += 4;
      if (stored !== crc32(buf.subarray(start, pos - 4))) {
        log.warn('CRC mismatch — stopping replay at pos ' + start);
        break;
      }
      if (seqno > maxSeq) maxSeq = seqno;
      if (op === WalOp.CHECKPOINT) lastCkpt = all.length;
      all.push({ seqno, op, key, data: Buffer.from(data) });
    }

    this.seqno = maxSeq;
    const toReplay = lastCkpt >= 0 ? all.slice(lastCkpt + 1) : all;
    this.pendingCount = toReplay.length;
    return toReplay.filter(e => e.op !== WalOp.CHECKPOINT);
  }
}
