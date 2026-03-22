// ============================================================
//  OvnDB v4.0 — Segment Manager
//
//  v4.0: Bloom Filter per-segment — probabilistic fast-miss
//        check sebelum B+ Tree lookup. ~80%+ disk read savings
//        untuk findOne miss pada dataset besar.
//
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import crypto from 'crypto';
import { SEGMENT_SIZE, MANIFEST_FILE, COMPACTION_RATIO, OVN_MAGIC, HEADER_SIZE, REC_OVERHEAD, REC_PREFIX_SIZE, REC_STATUS_SIZE, REC_TXID_SIZE, REC_CRC_SIZE, MANIFEST_CHECKSUM_ALGO, } from '../../types/constants.js';
import { crc32, writeCrc, readCrc } from '../../utils/crc32.js';
import { makeLogger } from '../../utils/logger.js';
import { BloomFilter } from './bloom-filter.js';
const log = makeLogger('segment');
export class SegmentManager {
    dirPath;
    collection;
    manifest;
    fds = new Map();
    _closed = false;
    // v4.0: BloomFilter per-segment — key = segmentId
    bloomFilters = new Map();
    // G15: optional compression hooks
    compressFn;
    decompressFn;
    /**
     * SECURITY: integrityKey untuk HMAC-SHA256 manifest verification.
     * Jika di-set, manifest checksum dihitung sebagai HMAC bukan plain SHA-256.
     * HMAC mendeteksi modifikasi disengaja oleh pihak yang bisa menulis ke
     * data dir tapi tidak mengetahui key. Plain SHA-256 hanya mendeteksi
     * korupsi acak (flipbit, partial write, dll).
     *
     * Untuk database terenkripsi: set ini ke Buffer 32-byte yang di-derive
     * dari passphrase yang sama (gunakan HKDF atau sub-key dari CryptoLayer).
     * Contoh di CollectionV2 / OvnDB.open():
     *   engine.segments.integrityKey = crypto.hkdfSync(
     *     'sha256', masterKey, salt, Buffer.from('ovndb-manifest-hmac'), 32
     *   );
     */
    integrityKey;
    constructor(dirPath, collection) {
        this.dirPath = dirPath;
        this.collection = collection;
    }
    // ── Integrity Helpers ─────────────────────────────────────
    /**
     * Hitung checksum manifest: HMAC-SHA256 jika integrityKey tersedia,
     * SHA-256 plain jika tidak. Kedua mode di-tag dengan prefix agar
     * open() bisa membedakan format lama (sha256:) dari baru (hmac:).
     */
    _computeManifestChecksum(content) {
        if (this.integrityKey) {
            if (this.integrityKey.length !== 32)
                throw new Error('[SegmentManager] integrityKey harus 32 bytes');
            const mac = crypto.createHmac('sha256', this.integrityKey).update(content).digest('hex');
            return `hmac:${mac}`;
        }
        const hash = crypto.createHash(MANIFEST_CHECKSUM_ALGO).update(content).digest('hex');
        return `sha256:${hash}`;
    }
    /**
     * Verifikasi checksum dari manifest yang sudah dimuat.
     * Menangani: format lama (hex string saja), sha256: prefix, hmac: prefix.
     * Gagal-tertutup: jika format tidak dikenal → throw.
     */
    _verifyManifestChecksum(content, stored) {
        if (stored.startsWith('hmac:')) {
            // Format baru dengan HMAC
            if (!this.integrityKey) {
                // Manifest ditulis dengan HMAC tapi kita tidak punya key — tidak bisa verifikasi
                throw new Error(`[SegmentManager] Manifest "${this.collection}" menggunakan HMAC tapi ` +
                    `integrityKey tidak di-set. Sediakan key yang sama untuk membuka database ini.`);
            }
            const mac = crypto.createHmac('sha256', this.integrityKey).update(content).digest('hex');
            const expected = `hmac:${mac}`;
            // Gunakan timingSafeEqual agar tidak rentan timing attack
            if (!crypto.timingSafeEqual(Buffer.from(stored), Buffer.from(expected))) {
                throw new Error(`[SegmentManager] Manifest HMAC mismatch untuk "${this.collection}" — ` +
                    `data mungkin dimodifikasi. Restore dari backup.`);
            }
        }
        else if (stored.startsWith('sha256:')) {
            // Format baru dengan SHA-256 plain
            const hash = crypto.createHash(MANIFEST_CHECKSUM_ALGO).update(content).digest('hex');
            const expected = `sha256:${hash}`;
            if (stored !== expected) {
                throw new Error(`[SegmentManager] Manifest SHA-256 mismatch untuk "${this.collection}" — ` +
                    `possible corruption. Restore dari backup.`);
            }
        }
        else {
            // Format lama: hex string tanpa prefix (ditulis sebelum patch ini)
            // Backward-compat: verifikasi sebagai SHA-256 plain tanpa prefix
            const computed = crypto.createHash(MANIFEST_CHECKSUM_ALGO).update(content).digest('hex');
            if (computed !== stored) {
                throw new Error(`[SegmentManager] Manifest checksum mismatch for "${this.collection}" — ` +
                    `possible corruption. Restore from backup or delete manifest to rebuild.`);
            }
        }
    }
    // ── Lifecycle ─────────────────────────────────────────────
    async open() {
        // SECURITY: mode 0o700 — hanya owner yang bisa baca/tulis/list direktori data
        // Tanpa ini, user lain di sistem yang sama bisa membaca file database.
        await fsp.mkdir(this.dirPath, { recursive: true, mode: 0o700 });
        const manifestPath = this._manifestPath();
        if (fs.existsSync(manifestPath)) {
            const raw = await fsp.readFile(manifestPath, 'utf8');
            const parsed = JSON.parse(raw);
            // G17 + SECURITY HARDENING: verifikasi integritas manifest
            // Gunakan HMAC jika integrityKey di-set, SHA-256 plain jika tidak.
            if (parsed['checksum']) {
                const storedChecksum = parsed['checksum'];
                const { checksum: _cs, ...rest } = parsed;
                const content = JSON.stringify(rest);
                // _verifyManifestChecksum throws jika tidak cocok
                this._verifyManifestChecksum(content, storedChecksum);
            }
            this.manifest = {
                ...parsed,
                totalLive: BigInt(parsed['totalLive'] ?? '0'),
                totalDead: BigInt(parsed['totalDead'] ?? '0'),
            };
        }
        else {
            this.manifest = {
                version: 3,
                collection: this.collection,
                flags: 0 /* FileFlags.NONE */,
                segments: [],
                createdAt: Date.now(),
                updatedAt: Date.now(),
                totalLive: 0n,
                totalDead: 0n,
            };
        }
        for (const seg of this.manifest.segments)
            this._openSegment(seg.id);
        if (this.manifest.segments.length === 0)
            await this._createNewSegment();
        // v4.0: Build bloom filters from existing segments
        await this._buildBloomFilters();
        log.info(`Opened ${this.manifest.segments.length} segment(s)`, {
            collection: this.collection,
            totalLive: String(this.manifest.totalLive),
        });
    }
    async close() {
        if (this._closed)
            return;
        this._closed = true;
        await this._saveManifest();
        for (const [, fd] of this.fds) {
            try {
                fs.fdatasyncSync(fd);
                fs.closeSync(fd);
            }
            catch { /* ignore */ }
        }
        this.fds.clear();
    }
    // ── Write ─────────────────────────────────────────────────
    writeRecord(data, txId) {
        // G15: kompres data sebelum tulis jika compressFn tersedia
        const payload = this.compressFn ? this.compressFn(data) : data;
        let active = this._activeSegment();
        if (active.size + payload.length + REC_OVERHEAD > SEGMENT_SIZE) {
            this._createNewSegmentSync();
            active = this._activeSegment();
        }
        const fd = this._fd(active.id);
        const offset = active.size;
        const rec = this._buildRecord(payload, txId);
        fs.writeSync(fd, rec, 0, rec.length, HEADER_SIZE + offset);
        const ptr = {
            segmentId: active.id,
            offset,
            totalSize: rec.length,
            dataSize: payload.length,
            txId,
        };
        active.size += rec.length;
        active.live++;
        this.manifest.totalLive++;
        this.manifest.updatedAt = Date.now();
        return ptr;
    }
    deleteRecord(ptr) {
        const fd = this._fd(ptr.segmentId);
        fs.writeSync(fd, Buffer.from([0 /* RecordStatus.DELETED */]), 0, 1, HEADER_SIZE + ptr.offset);
        const seg = this._seg(ptr.segmentId);
        if (seg) {
            seg.dead++;
            seg.live = Math.max(0, seg.live - 1);
        }
        this.manifest.totalDead++;
        this.manifest.totalLive = this.manifest.totalLive > 0n
            ? this.manifest.totalLive - 1n : 0n;
        this.manifest.updatedAt = Date.now();
    }
    readRecord(ptr) {
        const fd = this._fd(ptr.segmentId);
        const buf = Buffer.allocUnsafe(ptr.totalSize);
        const n = fs.readSync(fd, buf, 0, ptr.totalSize, HEADER_SIZE + ptr.offset);
        if (n < ptr.totalSize)
            return null;
        const status = buf.readUInt8(0);
        if (status === 0 /* RecordStatus.DELETED */)
            return null;
        const dataLen = buf.readUInt32LE(REC_STATUS_SIZE + REC_TXID_SIZE);
        const dataEnd = REC_PREFIX_SIZE + dataLen;
        const stored = readCrc(buf, dataEnd);
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
    async *scanAll(decryptFn, opts) {
        for (const seg of this.manifest.segments) {
            if (opts?.fromSegment !== undefined && seg.id < opts.fromSegment)
                continue;
            const startOffset = (opts?.fromSegment === seg.id) ? (opts?.fromOffset ?? 0) : 0;
            yield* this._scanSegment(seg.id, decryptFn, startOffset);
        }
    }
    async *_scanSegment(segId, decryptFn, startOffset = 0) {
        const seg = this._seg(segId);
        if (!seg || seg.size === 0)
            return;
        const fd = this._fd(segId);
        let filePos = startOffset;
        const prefixBuf = Buffer.allocUnsafe(REC_PREFIX_SIZE);
        while (filePos < seg.size) {
            const nr = fs.readSync(fd, prefixBuf, 0, REC_PREFIX_SIZE, HEADER_SIZE + filePos);
            if (nr < REC_PREFIX_SIZE)
                break;
            const status = prefixBuf.readUInt8(0);
            const txId = prefixBuf.readBigUInt64LE(REC_STATUS_SIZE);
            const dataLen = prefixBuf.readUInt32LE(REC_STATUS_SIZE + REC_TXID_SIZE);
            const total = REC_OVERHEAD + dataLen;
            if (filePos + total > seg.size)
                break;
            if (status === 1 /* RecordStatus.ACTIVE */) {
                const payloadBuf = Buffer.allocUnsafe(dataLen + REC_CRC_SIZE);
                const np = fs.readSync(fd, payloadBuf, 0, payloadBuf.length, HEADER_SIZE + filePos + REC_PREFIX_SIZE);
                if (np < payloadBuf.length) {
                    filePos += total;
                    continue;
                }
                const crcBuf = Buffer.allocUnsafe(REC_PREFIX_SIZE + dataLen);
                prefixBuf.copy(crcBuf, 0);
                payloadBuf.copy(crcBuf, REC_PREFIX_SIZE, 0, dataLen);
                const storedCrc = payloadBuf.readUInt32LE(dataLen);
                if (storedCrc === crc32(crcBuf)) {
                    let raw = Buffer.from(payloadBuf.subarray(0, dataLen));
                    // G15: decompress sebelum decrypt
                    if (this.decompressFn)
                        raw = this.decompressFn(raw);
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
    markAllDeleted() {
        const deleted = Buffer.from([0 /* RecordStatus.DELETED */]);
        for (const seg of this.manifest.segments) {
            if (seg.size === 0)
                continue;
            const fd = this._fd(seg.id);
            const prefixBuf = Buffer.allocUnsafe(REC_PREFIX_SIZE);
            let pos = 0;
            while (pos < seg.size) {
                const nr = fs.readSync(fd, prefixBuf, 0, REC_PREFIX_SIZE, HEADER_SIZE + pos);
                if (nr < REC_PREFIX_SIZE)
                    break;
                const status = prefixBuf.readUInt8(0);
                const dataLen = prefixBuf.readUInt32LE(REC_STATUS_SIZE + REC_TXID_SIZE);
                const total = REC_OVERHEAD + dataLen;
                if (pos + total > seg.size)
                    break;
                if (status === 1 /* RecordStatus.ACTIVE */) {
                    fs.writeSync(fd, deleted, 0, 1, HEADER_SIZE + pos);
                }
                pos += total;
            }
            this.manifest.totalDead += BigInt(seg.live);
            this.manifest.totalLive = 0n;
            seg.dead += seg.live;
            seg.live = 0;
        }
        this.manifest.updatedAt = Date.now();
    }
    /**
     * Versi async dari markAllDeleted() — tidak memblokir event loop.
     * Yield ke event loop setiap 1000 records dan antar segment.
     * Gunakan ini untuk collection besar (>50K dokumen) via deleteAll().
     */
    async markAllDeletedAsync() {
        const deleted = Buffer.from([0 /* RecordStatus.DELETED */]);
        for (const seg of this.manifest.segments) {
            if (seg.size === 0)
                continue;
            const fd = this._fd(seg.id);
            const prefixBuf = Buffer.allocUnsafe(REC_PREFIX_SIZE);
            let pos = 0;
            let processed = 0;
            while (pos < seg.size) {
                const nr = fs.readSync(fd, prefixBuf, 0, REC_PREFIX_SIZE, HEADER_SIZE + pos);
                if (nr < REC_PREFIX_SIZE)
                    break;
                const status = prefixBuf.readUInt8(0);
                const dataLen = prefixBuf.readUInt32LE(REC_STATUS_SIZE + REC_TXID_SIZE);
                const total = REC_OVERHEAD + dataLen;
                if (pos + total > seg.size)
                    break;
                if (status === 1 /* RecordStatus.ACTIVE */) {
                    fs.writeSync(fd, deleted, 0, 1, HEADER_SIZE + pos);
                    processed++;
                    // Yield ke event loop setiap 1000 active records agar tidak freeze
                    if (processed % 1000 === 0) {
                        await new Promise(r => setImmediate(r));
                    }
                }
                pos += total;
            }
            this.manifest.totalDead += BigInt(seg.live);
            this.manifest.totalLive = 0n;
            seg.dead += seg.live;
            seg.live = 0;
            // Yield antar segment
            await new Promise(r => setImmediate(r));
        }
        this.manifest.updatedAt = Date.now();
    }
    // ── Compaction ────────────────────────────────────────────
    async autoCompact(onPointerMoved) {
        const compacted = [];
        for (const seg of [...this.manifest.segments]) {
            const frag = seg.live > 0 ? seg.dead / (seg.live + seg.dead) : 0;
            if (frag < COMPACTION_RATIO)
                continue;
            log.info(`Compacting segment ${seg.id} (frag ${(frag * 100).toFixed(1)}%)`, {
                collection: this.collection,
            });
            await this._compactSegment(seg.id, onPointerMoved);
            compacted.push(seg.id);
        }
        if (compacted.length > 0)
            await this._saveManifest();
        return compacted;
    }
    async _compactSegment(segId, onPointerMoved) {
        const tmpPath = this._segPath(segId) + '.compact';
        // SECURITY: mode 0o600 — temp compact file berisi data yang sama dengan segment aktif
        const tmpFd = fs.openSync(tmpPath, 'w', 0o600);
        const header = Buffer.alloc(HEADER_SIZE);
        OVN_MAGIC.copy(header, 0);
        fs.writeSync(tmpFd, header, 0, HEADER_SIZE, 0);
        let newOffset = 0;
        // G15: saat compact, data sudah plaintext (dari _scanSegment yang decompresses)
        // Kita perlu re-compress saat menulis ulang
        for await (const { ptr, data } of this._scanSegment(segId)) {
            const payload = this.compressFn ? this.compressFn(data) : data;
            const rec = this._buildRecord(payload, ptr.txId);
            fs.writeSync(tmpFd, rec, 0, rec.length, HEADER_SIZE + newOffset);
            const newPtr = {
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
        const seg = this._seg(segId);
        const deadBefore = BigInt(seg.dead);
        seg.size = newOffset;
        seg.dead = 0;
        this.manifest.totalDead = this.manifest.totalDead >= deadBefore
            ? this.manifest.totalDead - deadBefore : 0n;
        this.manifest.updatedAt = Date.now();
    }
    // ── Stats ─────────────────────────────────────────────────
    get totalLive() { return this.manifest.totalLive; }
    get totalDead() { return this.manifest.totalDead; }
    get segmentCount() { return this.manifest.segments.length; }
    get totalFileSize() {
        return this.manifest.segments.reduce((s, seg) => s + seg.size + HEADER_SIZE, 0);
    }
    get fragmentRatio() {
        const live = Number(this.manifest.totalLive);
        const dead = Number(this.manifest.totalDead);
        const total = live + dead;
        return total > 0 ? dead / total : 0;
    }
    async saveManifest() { await this._saveManifest(); }
    fdatasyncActive() {
        const active = this._activeSegment();
        fs.fdatasyncSync(this._fd(active.id));
    }
    /**
     * v4.0: Bloom filter fast-miss check.
     * Cek apakah id MUNGKIN ada di SALAH SATU segment.
     * false = pasti tidak ada → skip B+ Tree lookup seluruhnya.
     * true  = mungkin ada → lanjutkan ke B+ Tree.
     */
    mightContain(id) {
        for (const bf of this.bloomFilters.values()) {
            if (bf.test(id))
                return true;
        }
        return false;
    }
    // ── Privates ──────────────────────────────────────────────
    _buildRecord(data, txId) {
        const buf = Buffer.allocUnsafe(REC_OVERHEAD + data.length);
        buf.writeUInt8(1 /* RecordStatus.ACTIVE */, 0);
        buf.writeBigUInt64LE(txId, REC_STATUS_SIZE);
        buf.writeUInt32LE(data.length, REC_STATUS_SIZE + REC_TXID_SIZE);
        data.copy(buf, REC_PREFIX_SIZE);
        writeCrc(buf, REC_PREFIX_SIZE + data.length, crc32(buf.subarray(0, REC_PREFIX_SIZE + data.length)));
        return buf;
    }
    _activeSegment() {
        return this.manifest.segments[this.manifest.segments.length - 1];
    }
    _seg(id) {
        return this.manifest.segments.find(s => s.id === id);
    }
    _fd(segId) {
        const fd = this.fds.get(segId);
        if (fd === undefined)
            throw new Error(`[SegmentManager] Segment ${segId} not open`);
        return fd;
    }
    _segPath(id) {
        return path.join(this.dirPath, `${this.collection}.seg-${String(id).padStart(4, '0')}.ovn`);
    }
    _manifestPath() {
        return path.join(this.dirPath, `${this.collection}.${MANIFEST_FILE}`);
    }
    _openSegment(id) {
        this.fds.set(id, fs.openSync(this._segPath(id), 'r+'));
    }
    async _createNewSegment() {
        const id = this.manifest.segments.length;
        const p = this._segPath(id);
        // SECURITY: mode 0o600 — segment file hanya bisa dibaca/ditulis owner
        const fd = fs.openSync(p, 'w+', 0o600);
        const hdr = Buffer.alloc(HEADER_SIZE);
        OVN_MAGIC.copy(hdr, 0);
        fs.writeSync(fd, hdr, 0, HEADER_SIZE, 0);
        fs.fdatasyncSync(fd);
        this.fds.set(id, fd);
        this.manifest.segments.push({ id, path: p, size: 0, live: 0, dead: 0, fragmentation: 0 });
        // v4.0: create bloom filter for new segment
        this.bloomFilters.set(id, new BloomFilter(50_000));
        await this._saveManifest();
        log.debug(`Created segment ${id}`, { collection: this.collection });
    }
    _createNewSegmentSync() {
        const id = this.manifest.segments.length;
        const p = this._segPath(id);
        // SECURITY: mode 0o600 — segment file hanya bisa dibaca/ditulis owner
        const fd = fs.openSync(p, 'w+', 0o600);
        const hdr = Buffer.alloc(HEADER_SIZE);
        OVN_MAGIC.copy(hdr, 0);
        fs.writeSync(fd, hdr, 0, HEADER_SIZE, 0);
        fs.fdatasyncSync(fd);
        this.fds.set(id, fd);
        this.manifest.segments.push({ id, path: p, size: 0, live: 0, dead: 0, fragmentation: 0 });
        // v4.0: create bloom filter for new segment
        this.bloomFilters.set(id, new BloomFilter(50_000));
        const tmp = this._buildManifestContent();
        // SECURITY: mode 0o600 untuk manifest sync juga
        fs.writeFileSync(this._manifestPath(), tmp.content, { encoding: 'utf8', mode: 0o600 });
    }
    /**
     * G17 + SECURITY HARDENING: Build manifest JSON dengan HMAC-SHA256 (se integrityKey
     * di-set) atau SHA-256 plain (backward-compat). Checksum prefixed agar
     * open() bisa membedakan format.
     */
    _buildManifestContent() {
        const base = {
            ...this.manifest,
            totalLive: String(this.manifest.totalLive),
            totalDead: String(this.manifest.totalDead),
        };
        // Hitung checksum dari content tanpa field checksum itu sendiri
        const withoutChecksum = JSON.stringify(base, null, 2);
        const checksum = this._computeManifestChecksum(withoutChecksum);
        const withChecksum = JSON.stringify({ ...base, checksum }, null, 2);
        return { content: withChecksum };
    }
    async _saveManifest() {
        const { content } = this._buildManifestContent();
        const tmp = this._manifestPath() + '.tmp';
        // SECURITY: mode 0o600 — manifest berisi metadata sensitif (ukuran, jumlah record, dll)
        await fsp.writeFile(tmp, content, { encoding: 'utf8', mode: 0o600 });
        await fsp.rename(tmp, this._manifestPath());
    }
    /**
     * v4.0: Build bloom filters from all segments by scanning their records.
     * Called once during open(). O(n) scan per segment but only run at startup.
     */
    async _buildBloomFilters() {
        for (const seg of this.manifest.segments) {
            const bf = new BloomFilter(Math.max(seg.live + seg.dead, 1000));
            this.bloomFilters.set(seg.id, bf);
            if (seg.size === 0)
                continue;
            const fd = this._fd(seg.id);
            const prefixBuf = Buffer.allocUnsafe(REC_PREFIX_SIZE);
            // We need to read document IDs — scan payload
            let pos = 0;
            let yielded = 0;
            while (pos < seg.size) {
                const nr = fs.readSync(fd, prefixBuf, 0, REC_PREFIX_SIZE, HEADER_SIZE + pos);
                if (nr < REC_PREFIX_SIZE)
                    break;
                const status = prefixBuf.readUInt8(0);
                const dataLen = prefixBuf.readUInt32LE(REC_STATUS_SIZE + REC_TXID_SIZE);
                const total = REC_OVERHEAD + dataLen;
                if (pos + total > seg.size)
                    break;
                if (status === 1 /* RecordStatus.ACTIVE */) {
                    // Read just enough to extract _id from JSON start
                    const sniffLen = Math.min(dataLen, 128);
                    const sniffBuf = Buffer.allocUnsafe(sniffLen);
                    fs.readSync(fd, sniffBuf, 0, sniffLen, HEADER_SIZE + pos + REC_PREFIX_SIZE);
                    try {
                        // Decompress if needed for _id extraction
                        let jsonBuf = sniffBuf;
                        if (this.decompressFn) {
                            const fullBuf = Buffer.allocUnsafe(dataLen);
                            fs.readSync(fd, fullBuf, 0, dataLen, HEADER_SIZE + pos + REC_PREFIX_SIZE);
                            jsonBuf = Buffer.from(this.decompressFn(fullBuf)).subarray(0, 128);
                        }
                        const str = jsonBuf.toString('utf8');
                        const match = str.match(/"_id":"([^"]+)"/);
                        if (match)
                            bf.add(match[1]);
                    }
                    catch { /* ignore parse errors */ }
                    yielded++;
                    if (yielded % 5000 === 0) {
                        await new Promise(r => setImmediate(r));
                    }
                }
                pos += total;
            }
        }
        log.debug('Bloom filters built', { collection: this.collection, segments: this.manifest.segments.length });
    }
}
//# sourceMappingURL=segment-manager.js.map