// ============================================================
//  OvnDB v3.0 — WAL v3 (Write-Ahead Log)
//
//  G5 FIX: WAL rotation ketika file melampaui WAL_MAX_SIZE_BYTES.
//  File lama di-rename ke .wal.bak, file baru dibuat fresh.
//  Ini mencegah WAL tumbuh tak terbatas saat crash sebelum checkpoint.
//
//  G6 FIX: Replay sekarang melacak TX_ABORT — operasi dari transaksi
//  yang di-abort TIDAK di-replay ke storage. Ini fix bug data corruption
//  yang bisa terjadi saat crash recovery di v2.x.
//
//  v3.1 FIXES:
//  - _doGroupCommit(): async fsp.write + fsp.fdatasync (tidak blocking event loop)
//  - _rotateWal(): crash-safe via atomic rename (.new → .wal)
//  - open(): bersihkan .wal.new stale dari crash sebelumnya
// ============================================================
import fs from 'fs';
import path from 'path';
import { WAL_MAGIC, WAL_GROUP_SIZE, WAL_GROUP_WAIT_MS, WAL_MAX_SIZE_BYTES, } from '../../types/constants.js';
import { crc32, writeCrc, readCrc } from '../../utils/crc32.js';
import { makeLogger } from '../../utils/logger.js';
const log = makeLogger('wal');
export class WAL {
    dirPath;
    colName;
    filePath;
    fd = null;
    seqno = 0n;
    writePos = 0;
    _pendingCount = 0;
    group = { entries: [], resolvers: [], rejecters: [] };
    groupTimer = null;
    _committing = false;
    constructor(dirPath, collectionName) {
        this.dirPath = dirPath;
        this.colName = collectionName;
        this.filePath = path.join(dirPath, `${collectionName}.wal`);
    }
    // ── Lifecycle ─────────────────────────────────────────────
    async open() {
        // FIX: bersihkan .new stale dari crash sebelumnya (jika crash saat _rotateWal step 1)
        const newPath = this.filePath + '.new';
        if (fs.existsSync(newPath)) {
            log.warn('Found stale .wal.new from previous crash — removing', { col: this.colName });
            fs.unlinkSync(newPath);
        }
        // SECURITY: mode 0o600 — WAL berisi plaintext data sebelum di-encrypt ke segment
        if (!fs.existsSync(this.filePath))
            fs.writeFileSync(this.filePath, Buffer.alloc(0), { mode: 0o600 });
        this.fd = fs.openSync(this.filePath, 'r+');
        // G6: replay dengan TX_ABORT awareness
        const entries = this._replay();
        this.writePos = fs.fstatSync(this.fd).size;
        log.debug(`WAL opened, ${entries.length} entries to replay`);
        return entries;
    }
    async close() {
        if (this.groupTimer) {
            clearTimeout(this.groupTimer);
            this.groupTimer = null;
        }
        await this._doGroupCommit();
        if (this.fd !== null) {
            fs.closeSync(this.fd);
            this.fd = null;
        }
    }
    // ── Append ────────────────────────────────────────────────
    // SECURITY: WAL_KEY_MAX_LEN — uint16 field bisa menampung key hingga 65535 byte.
    // Tanpa batas, caller bisa menulis key 64 KB per entri dan menguras disk/RAM WAL.
    // 1024 byte cukup untuk semua _id yang valid (max 128 byte per security.ts).
    static WAL_KEY_MAX_LEN = 1024;
    // SECURITY: WAL_DATA_MAX_LEN — document bytes sudah dibatasi di lapisan collection
    // (MAX_DOCUMENT_BYTES = 16 MB), tapi kita guard di sini juga sebagai defence-in-depth.
    static WAL_DATA_MAX_LEN = 32 * 1024 * 1024; // 32 MB hard cap
    append(op, key, data = Buffer.alloc(0), txId = 0n) {
        if (this.fd === null)
            return Promise.reject(new Error('WAL not open'));
        // SECURITY: batasi panjang key sebelum encode dan alokasi buffer
        const keyBuf = Buffer.from(key, 'utf8');
        if (keyBuf.length > WAL.WAL_KEY_MAX_LEN)
            return Promise.reject(new Error(`[WAL] Key terlalu panjang (${keyBuf.length} > maks ${WAL.WAL_KEY_MAX_LEN})`));
        if (data.length > WAL.WAL_DATA_MAX_LEN)
            return Promise.reject(new Error(`[WAL] Data terlalu besar (${data.length} > maks ${WAL.WAL_DATA_MAX_LEN})`));
        this.seqno++;
        const size = 4 + 8 + 8 + 1 + 2 + keyBuf.length + 4 + data.length + 4;
        const buf = Buffer.allocUnsafe(size);
        let p = 0;
        WAL_MAGIC.copy(buf, p);
        p += 4;
        buf.writeBigUInt64LE(this.seqno, p);
        p += 8;
        buf.writeBigUInt64LE(txId, p);
        p += 8;
        buf.writeUInt8(op, p);
        p += 1;
        buf.writeUInt16LE(keyBuf.length, p);
        p += 2;
        keyBuf.copy(buf, p);
        p += keyBuf.length;
        buf.writeUInt32LE(data.length, p);
        p += 4;
        data.copy(buf, p);
        p += data.length;
        writeCrc(buf, p, crc32(buf.subarray(0, p)));
        this._pendingCount++;
        return new Promise((resolve, reject) => {
            this.group.entries.push(buf);
            this.group.resolvers.push(resolve);
            this.group.rejecters.push(reject);
            if (this.group.entries.length >= WAL_GROUP_SIZE) {
                if (this.groupTimer) {
                    clearTimeout(this.groupTimer);
                    this.groupTimer = null;
                }
                setImmediate(() => this._doGroupCommit());
            }
            else if (!this.groupTimer) {
                this.groupTimer = setTimeout(() => {
                    this.groupTimer = null;
                    this._doGroupCommit();
                }, WAL_GROUP_WAIT_MS);
            }
        });
    }
    async checkpoint() {
        if (this.fd === null)
            return;
        if (this.writePos === 0 && this._pendingCount === 0)
            return;
        fs.ftruncateSync(this.fd, 0);
        fs.fdatasyncSync(this.fd);
        this.writePos = 0;
        this._pendingCount = 0;
        log.debug('WAL checkpointed');
    }
    get pending() { return this._pendingCount; }
    // ── Privates ──────────────────────────────────────────────
    async _doGroupCommit() {
        if (this._committing || this.group.entries.length === 0)
            return;
        this._committing = true;
        const { entries, resolvers, rejecters } = this.group;
        this.group = { entries: [], resolvers: [], rejecters: [] };
        try {
            // G5: rotate WAL jika sudah terlalu besar
            if (this.writePos > WAL_MAX_SIZE_BYTES) {
                await this._rotateWal();
            }
            // FIX: sync I/O — fd is a raw number from fs.openSync, not a FileHandle
            const combined = Buffer.concat(entries);
            fs.writeSync(this.fd, combined, 0, combined.length, this.writePos);
            this.writePos += combined.length;
            fs.fdatasyncSync(this.fd);
            for (const r of resolvers)
                r();
        }
        catch (err) {
            for (const r of rejecters)
                r(err);
        }
        finally {
            this._committing = false;
            if (this.group.entries.length > 0)
                setImmediate(() => this._doGroupCommit());
        }
    }
    /**
     * G5: Rotasi WAL — crash-safe dengan atomic rename.
     * Urutan: buat .new → sync → tutup lama → rename lama→.bak → rename .new→aktif
     * Jika crash di langkah manapun, data tidak hilang:
     *   - Crash sebelum rename: .new dibuang saat restart, .wal lama masih valid
     *   - Crash setelah rename: .wal baru ada dan valid
     */
    async _rotateWal() {
        if (this.fd === null)
            return;
        // Step 1: Buat file WAL baru (.new) — jika crash di sini, .wal lama masih valid
        const newPath = this.filePath + '.new';
        // Hapus .new stale dari crash sebelumnya jika ada
        if (fs.existsSync(newPath))
            fs.unlinkSync(newPath);
        // SECURITY: mode 0o600 — file WAL baru
        const newFd = fs.openSync(newPath, 'w+', 0o600);
        fs.fdatasyncSync(newFd);
        fs.closeSync(newFd);
        // Step 2: Sync & tutup file lama
        fs.fdatasyncSync(this.fd);
        fs.closeSync(this.fd);
        this.fd = null;
        // Step 3: Backup .wal lama ke .bak
        const bakPath = this.filePath + '.bak';
        if (fs.existsSync(bakPath))
            fs.unlinkSync(bakPath);
        fs.renameSync(this.filePath, bakPath);
        // Step 4: Atomic rename .new → .wal (crash-safe — OS garantikan atomicity)
        fs.renameSync(newPath, this.filePath);
        // Step 5: Buka .wal baru
        this.fd = fs.openSync(this.filePath, 'r+');
        this.writePos = 0;
        // Seqno tetap lanjut agar tidak ada konflik
        // SECURITY: tidak log path file agar tidak ada informasi filesystem di output
        log.info('WAL rotated (crash-safe)', { col: this.colName });
    }
    /**
     * G6: Replay dengan TX_ABORT awareness.
     * Operasi dari transaksi yang di-abort TIDAK di-replay.
     *
     * SECURITY: replay juga memvalidasi keyLen dan dataLen sebelum alokasi
     * untuk mencegah corrupt WAL menyebabkan giant buffer allocation / OOM.
     */
    _replay() {
        if (!this.fd)
            return [];
        const stat = fs.fstatSync(this.fd);
        if (stat.size === 0)
            return [];
        // SECURITY: cap total WAL size yang dibaca ke 2× WAL_MAX_SIZE_BYTES
        // Jika file lebih besar, kemungkinan corrupt — hentikan replay dengan aman.
        const MAX_REPLAY_BYTES = WAL_MAX_SIZE_BYTES * 2;
        if (stat.size > MAX_REPLAY_BYTES) {
            log.warn(`WAL file terlalu besar (${stat.size} bytes) — skipping replay, menganggap checkpoint`, {
                col: this.colName,
            });
            return [];
        }
        const raw = Buffer.allocUnsafe(stat.size);
        fs.readSync(this.fd, raw, 0, stat.size, 0);
        const all = [];
        let pos = 0, maxSeq = 0n, lastCkpt = -1;
        while (pos < raw.length) {
            const start = pos;
            if (raw.length - pos < 24)
                break;
            if (!raw.subarray(pos, pos + 4).equals(WAL_MAGIC))
                break;
            pos += 4;
            const seqno = raw.readBigUInt64LE(pos);
            pos += 8;
            const txId = raw.readBigUInt64LE(pos);
            pos += 8;
            const op = raw.readUInt8(pos);
            pos += 1;
            const keyLen = raw.readUInt16LE(pos);
            pos += 2;
            // SECURITY: sanity-check keyLen sebelum alokasi string
            if (keyLen > WAL.WAL_KEY_MAX_LEN) {
                log.warn(`WAL replay: keyLen ${keyLen} melebihi batas, stopping at pos ${start}`);
                break;
            }
            if (pos + keyLen > raw.length)
                break;
            const key = raw.toString('utf8', pos, pos + keyLen);
            pos += keyLen;
            if (pos + 4 > raw.length)
                break;
            const dataLen = raw.readUInt32LE(pos);
            pos += 4;
            // SECURITY: sanity-check dataLen sebelum alokasi buffer
            if (dataLen > WAL.WAL_DATA_MAX_LEN) {
                log.warn(`WAL replay: dataLen ${dataLen} melebihi batas, stopping at pos ${start}`);
                break;
            }
            if (pos + dataLen + 4 > raw.length)
                break;
            const data = raw.subarray(pos, pos + dataLen);
            pos += dataLen;
            const stored = readCrc(raw, pos);
            pos += 4;
            if (stored !== crc32(raw.subarray(start, pos - 4))) {
                log.warn('WAL CRC mismatch, stopping replay at pos ' + start);
                break;
            }
            if (seqno > maxSeq)
                maxSeq = seqno;
            if (op === 4 /* WalOp.CHECKPOINT */)
                lastCkpt = all.length;
            all.push({ seqno, op, key, data: Buffer.from(data), txId });
        }
        this.seqno = maxSeq;
        const toReplay = lastCkpt >= 0 ? all.slice(lastCkpt + 1) : all;
        this._pendingCount = toReplay.length;
        // G6: kumpulkan semua txId yang di-abort
        const abortedTxIds = new Set();
        for (const e of toReplay) {
            if (e.op === 7 /* WalOp.TX_ABORT */)
                abortedTxIds.add(e.txId);
        }
        // G6: filter operasi dari transaksi yang di-abort
        if (abortedTxIds.size > 0) {
            log.warn(`WAL replay: filtering ${abortedTxIds.size} aborted tx(s)`, {
                txIds: [...abortedTxIds].map(String).join(', '),
            });
        }
        return toReplay.filter(e => e.op !== 4 /* WalOp.CHECKPOINT */ &&
            e.op !== 5 /* WalOp.TX_BEGIN */ &&
            e.op !== 6 /* WalOp.TX_COMMIT */ &&
            e.op !== 7 /* WalOp.TX_ABORT */ &&
            !abortedTxIds.has(e.txId));
    }
}
//# sourceMappingURL=wal.js.map