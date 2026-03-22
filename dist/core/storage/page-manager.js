// ============================================================
//  OvnDB v3.0 — Page Manager (Buffer Pool)
//
//  G9 FIX: Dirty-aware eviction — clean pages di-evict dulu,
//  dirty pages hanya di-evict setelah di-flush ke disk.
//  Ini mencegah latency spike yang terjadi ketika LRU murni
//  memilih dirty page untuk di-evict (forced synchronous write).
//
//  Tambahan: reset() untuk support deleteAll() O(1).
// ============================================================
import fs from 'fs';
import { PAGE_SIZE, BUFFER_POOL_SIZE, EVICT_BATCH, PAGE_MAGIC, MAX_CACHE_BYTES, } from '../../types/constants.js';
import { LRUCache } from '../cache/lru-cache.js';
import { crc32 } from '../../utils/crc32.js';
export const PAGE_HEADER_SIZE = 24;
export const PAGE_DATA_SIZE = PAGE_SIZE - PAGE_HEADER_SIZE;
const INDEX_HEADER_SIZE = 128;
// G9: ukuran satu Page object dalam bytes untuk LRU byte-tracking
const PAGE_OBJECT_BYTES = PAGE_DATA_SIZE + 128; // data + header overhead
export class PageManager {
    filePath;
    fd = null;
    pageCount = 0;
    rootPageId = 0;
    // G9: dua list terpisah — clean pages dan dirty pages
    pool;
    _dirtySet = new Set();
    constructor(filePath, poolSize = BUFFER_POOL_SIZE) {
        this.filePath = filePath;
        // G9: estimasi byte pool = poolSize pages × PAGE_OBJECT_BYTES
        // Gunakan max(poolSize * PAGE_OBJECT_BYTES, MAX_CACHE_BYTES / 4)
        const maxPoolBytes = Math.max(poolSize * PAGE_OBJECT_BYTES, MAX_CACHE_BYTES / 4);
        this.pool = new LRUCache(maxPoolBytes, () => PAGE_OBJECT_BYTES);
    }
    async open() {
        const exists = fs.existsSync(this.filePath);
        // SECURITY: mode 0o600 — index file (.ovni) berisi pointer ke semua record
        // di segmen; hanya owner yang boleh baca/tulis
        this.fd = fs.openSync(this.filePath, exists ? 'r+' : 'w+', 0o600);
        if (!exists || fs.fstatSync(this.fd).size < INDEX_HEADER_SIZE) {
            await this._initFile();
        }
        else {
            this._readFileHeader();
        }
    }
    async close() {
        await this.flushDirty();
        this._writeFileHeader();
        if (this.fd !== null) {
            fs.fdatasyncSync(this.fd);
            fs.closeSync(this.fd);
            this.fd = null;
        }
    }
    get rootPage() { return this.rootPageId; }
    set rootPage(id) { this.rootPageId = id; }
    get totalPages() { return this.pageCount; }
    async readPage(pageId) {
        const cached = this.pool.get(pageId);
        if (cached)
            return cached;
        const page = this._readPageFromDisk(pageId);
        this._addToPool(pageId, page);
        return page;
    }
    async allocPage(type) {
        const pageId = this.pageCount++;
        const page = {
            header: { pageId, pageType: type, keyCount: 0, nextPage: 0, prevPage: 0, flags: 0 },
            data: Buffer.alloc(PAGE_DATA_SIZE),
            dirty: true,
        };
        this._writePageToDisk(pageId, page);
        this._addToPool(pageId, page);
        this._dirtySet.add(pageId);
        return { pageId, page };
    }
    markDirty(pageId) {
        const page = this.pool.get(pageId);
        if (page) {
            page.dirty = true;
            this._dirtySet.add(pageId);
        }
    }
    async flushDirty() {
        if (this.fd === null || this._dirtySet.size === 0)
            return;
        for (const pageId of this._dirtySet) {
            const page = this.pool.get(pageId);
            if (page?.dirty) {
                this._writePageToDisk(pageId, page);
                page.dirty = false;
            }
        }
        this._dirtySet.clear();
        fs.fdatasyncSync(this.fd);
    }
    /**
     * G7: reset seluruh page file ke state awal.
     * Dipakai oleh B+ Tree.clear() untuk deleteAll() O(1).
     */
    async reset() {
        this.pool.clear();
        this._dirtySet.clear();
        this.pageCount = 0;
        this.rootPageId = 0;
        if (this.fd !== null) {
            // Truncate file ke hanya header
            fs.ftruncateSync(this.fd, INDEX_HEADER_SIZE);
            this._writeFileHeader();
            fs.fdatasyncSync(this.fd);
        }
    }
    // ── Privates ──────────────────────────────────────────────
    _addToPool(pageId, page) {
        // G9: evict sebelum add jika pool mendekati limit
        if (this.pool.bytes >= this.pool.maxBytes * 0.95) {
            this._evictBatch();
        }
        this.pool.set(pageId, page);
    }
    /**
     * G9: Dirty-aware eviction.
     * Evict clean pages dulu — tidak perlu disk write.
     * Evict dirty pages hanya jika tidak ada clean page tersedia.
     */
    _evictBatch() {
        const toEvict = [];
        let evicted = 0;
        // Pass 1: kumpulkan CLEAN pages dari LRU (tidak perlu disk write)
        for (const [pageId, page] of this.pool.entries()) {
            if (evicted >= EVICT_BATCH)
                break;
            if (!page.dirty) {
                toEvict.push(pageId);
                evicted++;
            }
        }
        // Pass 2: jika belum cukup, ambil dirty pages (flush dulu)
        if (evicted < EVICT_BATCH / 2) {
            const dirtyToFlush = [];
            for (const [pageId, page] of this.pool.entries()) {
                if (evicted + dirtyToFlush.length >= EVICT_BATCH)
                    break;
                if (page.dirty)
                    dirtyToFlush.push(pageId);
            }
            // Flush dirty pages secara batch — satu fdatasync
            for (const pageId of dirtyToFlush) {
                const page = this.pool.get(pageId);
                if (page) {
                    this._writePageToDisk(pageId, page);
                    page.dirty = false;
                    this._dirtySet.delete(pageId);
                    toEvict.push(pageId);
                }
            }
            if (dirtyToFlush.length > 0 && this.fd !== null) {
                fs.fdatasyncSync(this.fd);
            }
        }
        // Hapus dari pool
        for (const pageId of toEvict) {
            this.pool.delete(pageId);
        }
    }
    _readPageFromDisk(pageId) {
        if (this.fd === null)
            throw new Error('[PageManager] File not open');
        const offset = INDEX_HEADER_SIZE + pageId * PAGE_SIZE;
        const buf = Buffer.allocUnsafe(PAGE_SIZE);
        const n = fs.readSync(this.fd, buf, 0, PAGE_SIZE, offset);
        if (n < PAGE_SIZE)
            throw new Error(`[PageManager] Short read page ${pageId}`);
        const storedCrc = buf.readUInt32LE(PAGE_HEADER_SIZE - 4);
        const actualCrc = crc32(buf.subarray(0, PAGE_HEADER_SIZE - 4));
        if (storedCrc !== actualCrc)
            throw new Error(`[PageManager] CRC mismatch page ${pageId}`);
        if (!buf.subarray(0, 4).equals(PAGE_MAGIC))
            throw new Error(`[PageManager] Invalid magic page ${pageId}`);
        const header = {
            pageId: buf.readUInt32LE(4),
            pageType: buf.readUInt8(8),
            keyCount: buf.readUInt16LE(9),
            nextPage: buf.readUInt32LE(11),
            prevPage: buf.readUInt32LE(15),
            flags: buf.readUInt8(19),
        };
        return { header, data: Buffer.from(buf.subarray(PAGE_HEADER_SIZE)), dirty: false };
    }
    _writePageToDisk(pageId, page) {
        if (this.fd === null)
            return;
        const buf = Buffer.allocUnsafe(PAGE_SIZE);
        PAGE_MAGIC.copy(buf, 0);
        buf.writeUInt32LE(pageId, 4);
        buf.writeUInt8(page.header.pageType, 8);
        buf.writeUInt16LE(page.header.keyCount, 9);
        buf.writeUInt32LE(page.header.nextPage, 11);
        buf.writeUInt32LE(page.header.prevPage, 15);
        buf.writeUInt8(page.header.flags, 19);
        buf.writeUInt32LE(crc32(buf.subarray(0, 20)), 20);
        page.data.copy(buf, PAGE_HEADER_SIZE);
        const offset = INDEX_HEADER_SIZE + pageId * PAGE_SIZE;
        fs.writeSync(this.fd, buf, 0, PAGE_SIZE, offset);
        page.dirty = false;
    }
    async _initFile() {
        if (this.fd === null)
            return;
        const header = Buffer.alloc(INDEX_HEADER_SIZE);
        fs.writeSync(this.fd, header, 0, INDEX_HEADER_SIZE, 0);
        this.pageCount = 0;
        this.rootPageId = 0;
        this._writeFileHeader();
    }
    _writeFileHeader() {
        if (this.fd === null)
            return;
        const h = Buffer.alloc(INDEX_HEADER_SIZE);
        let p = 0;
        PAGE_MAGIC.copy(h, p);
        p += 4;
        h.writeUInt32LE(2, p);
        p += 4;
        h.writeUInt32LE(this.rootPageId, p);
        p += 4;
        h.writeUInt32LE(this.pageCount, p);
        p += 4;
        h.writeDoubleBE(Date.now(), p);
        p += 8;
        h.writeUInt32LE(crc32(h.subarray(0, p)), p);
        fs.writeSync(this.fd, h, 0, INDEX_HEADER_SIZE, 0);
    }
    _readFileHeader() {
        if (this.fd === null)
            return;
        const h = Buffer.allocUnsafe(INDEX_HEADER_SIZE);
        fs.readSync(this.fd, h, 0, INDEX_HEADER_SIZE, 0);
        let p = 4;
        p += 4;
        this.rootPageId = h.readUInt32LE(p);
        p += 4;
        this.pageCount = h.readUInt32LE(p);
        p += 4;
    }
}
//# sourceMappingURL=page-manager.js.map