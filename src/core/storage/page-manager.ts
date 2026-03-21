// ============================================================
//  OvnDB v2.0 — Page Manager (Buffer Pool)
//
//  Ini adalah jantung skalabilitas OvnDB v2.
//  B+ Tree sebelumnya (v1) sepenuhnya di memori — artinya 1 miliar
//  record membutuhkan puluhan GB RAM hanya untuk index.
//
//  Page Manager membalik paradigma ini:
//   - B+ Tree nodes disimpan sebagai pages (16KB) di disk
//   - Hanya page yang SEDANG DIAKSES dimuat ke RAM (buffer pool)
//   - Page yang tidak dipakai di-evict dari pool (LRU policy)
//   - Dirty pages (dimodifikasi) di-flush ke disk sebelum eviction
//
//  Dengan ini, index B+ Tree bisa memuat triliunan entry sambil
//  hanya menggunakan memori sebesar buffer pool (default: 256 MB).
//
//  Layout satu file index (.ovni):
//    [128 bytes] File header (magic, version, root page ID, page count)
//    [16KB × N]  Pages — setiap page adalah satu node B+ Tree
// ============================================================

import fs   from 'fs';
import fsp  from 'fs/promises';
import {
  PAGE_SIZE, BUFFER_POOL_SIZE, EVICT_BATCH,
  PAGE_MAGIC, PageType,
} from '../../types/constants.js';
import { LRUCache } from '../cache/lru-cache.js';
import { crc32 }   from '../../utils/crc32.js';

// ── Page Header Layout (pertama 24 bytes dari setiap page) ────
//  [4]  magic     PAGE_MAGIC
//  [4]  pageId    uint32 LE
//  [1]  pageType  PageType
//  [2]  keyCount  uint16 LE (jumlah key dalam node ini)
//  [4]  nextPage  uint32 LE (untuk leaf: pointer ke next leaf, 0 = tidak ada)
//  [4]  prevPage  uint32 LE (untuk leaf: pointer ke prev leaf, 0 = tidak ada)
//  [1]  flags     uint8 (reserved)
//  [4]  crc       uint32 LE (covers seluruh page kecuali CRC itu sendiri)
export const PAGE_HEADER_SIZE = 24;
export const PAGE_DATA_SIZE   = PAGE_SIZE - PAGE_HEADER_SIZE; // 16360 bytes per page untuk data

const INDEX_HEADER_SIZE = 128;

export interface PageHeader {
  pageId:    number;
  pageType:  PageType;
  keyCount:  number;
  nextPage:  number;   // 0 = null
  prevPage:  number;   // 0 = null
  flags:     number;
}

export interface Page {
  header:   PageHeader;
  data:     Buffer;  // PAGE_DATA_SIZE bytes payload
  dirty:    boolean; // perlu di-flush ke disk
}

export class PageManager {
  private readonly filePath:   string;
  private fd:     number | null = null;
  private pageCount: number = 0;
  private rootPageId: number = 0;
  private readonly pool: LRUCache<number, Page>;

  constructor(filePath: string, poolSize = BUFFER_POOL_SIZE) {
    this.filePath = filePath;
    this.pool     = new LRUCache<number, Page>(poolSize);
  }

  // ── Lifecycle ─────────────────────────────────────────────

  async open(): Promise<void> {
    const exists = fs.existsSync(this.filePath);
    this.fd = fs.openSync(this.filePath, exists ? 'r+' : 'w+');
    if (!exists || fs.fstatSync(this.fd).size < INDEX_HEADER_SIZE) {
      await this._initFile();
    } else {
      this._readFileHeader();
    }
  }

  async close(): Promise<void> {
    await this.flushDirty();
    this._writeFileHeader();
    if (this.fd !== null) {
      fs.fdatasyncSync(this.fd);
      fs.closeSync(this.fd);
      this.fd = null;
    }
  }

  get rootPage(): number  { return this.rootPageId; }
  set rootPage(id: number){ this.rootPageId = id; }
  get totalPages(): number{ return this.pageCount; }

  // ── Page I/O ──────────────────────────────────────────────

  /**
   * Baca page dari buffer pool atau disk.
   * Jika pool penuh, evict page LRU yang bersih (atau flush dirty dulu).
   */
  async readPage(pageId: number): Promise<Page> {
    // Cek buffer pool dulu (O(1))
    const cached = this.pool.get(pageId);
    if (cached) return cached;

    // Load dari disk
    const page = this._readPageFromDisk(pageId);
    this._addToPool(pageId, page);
    return page;
  }

  /**
   * Alokasikan page baru di akhir file dan kembalikan ID-nya.
   * Page baru dimulai dengan payload kosong (zeroed).
   */
  async allocPage(type: PageType): Promise<{ pageId: number; page: Page }> {
    const pageId = this.pageCount++;
    const page: Page = {
      header: {
        pageId, pageType: type, keyCount: 0,
        nextPage: 0, prevPage: 0, flags: 0,
      },
      data:  Buffer.alloc(PAGE_DATA_SIZE),
      dirty: true,  // page baru langsung dirty → akan di-flush ke disk
    };
    this._addToPool(pageId, page);
    // Tulis zeroed page ke disk untuk memastikan offset valid
    this._writePageToDisk(pageId, page);
    return { pageId, page };
  }

  /**
   * Tandai page sebagai dirty (sudah dimodifikasi, perlu di-flush).
   * PageManager tidak perlu dipanggil secara eksplisit jika kita
   * memodifikasi page.data langsung karena referensi di-share.
   */
  markDirty(pageId: number): void {
    const page = this.pool.get(pageId);
    if (page) page.dirty = true;
  }

  /** Flush semua dirty page ke disk dalam satu pass. */
  async flushDirty(): Promise<void> {
    if (this.fd === null) return;
    for (const [pageId, page] of this.pool.entries()) {
      if (page.dirty) {
        this._writePageToDisk(pageId, page);
        page.dirty = false;
      }
    }
    fs.fdatasyncSync(this.fd);
  }

  // ── Privates ──────────────────────────────────────────────

  private _addToPool(pageId: number, page: Page): void {
    // Sebelum masuk pool, evict page LRU jika pool sudah penuh
    if (this.pool.size >= this.pool.capacity_) {
      this._evictBatch();
    }
    this.pool.set(pageId, page);
  }

  private _evictBatch(): void {
    // Evict page paling jarang dipakai (LRU) dalam batch
    let evicted = 0;
    for (const [pageId, page] of this.pool.entries()) {
      if (evicted >= EVICT_BATCH) break;
      if (page.dirty) this._writePageToDisk(pageId, page);
      this.pool.delete(pageId);
      evicted++;
    }
    if (this.fd !== null && evicted > 0) fs.fdatasyncSync(this.fd);
  }

  private _readPageFromDisk(pageId: number): Page {
    if (this.fd === null) throw new Error('[PageManager] File not open');
    const offset = INDEX_HEADER_SIZE + pageId * PAGE_SIZE;
    const buf    = Buffer.allocUnsafe(PAGE_SIZE);
    const n      = fs.readSync(this.fd, buf, 0, PAGE_SIZE, offset);
    if (n < PAGE_SIZE) throw new Error(`[PageManager] Short read on page ${pageId}`);

    // Verifikasi CRC (cover semua kecuali 4 byte CRC di akhir header)
    const storedCrc  = buf.readUInt32LE(PAGE_HEADER_SIZE - 4);
    const actualCrc  = crc32(buf.subarray(0, PAGE_HEADER_SIZE - 4));
    if (storedCrc !== actualCrc) throw new Error(`[PageManager] CRC mismatch page ${pageId}`);

    // Verifikasi magic
    if (!buf.subarray(0, 4).equals(PAGE_MAGIC))
      throw new Error(`[PageManager] Invalid magic on page ${pageId}`);

    const header: PageHeader = {
      pageId:   buf.readUInt32LE(4),
      pageType: buf.readUInt8(8) as PageType,
      keyCount: buf.readUInt16LE(9),
      nextPage: buf.readUInt32LE(11),
      prevPage: buf.readUInt32LE(15),
      flags:    buf.readUInt8(19),
    };
    return {
      header,
      data:  Buffer.from(buf.subarray(PAGE_HEADER_SIZE)),
      dirty: false,
    };
  }

  private _writePageToDisk(pageId: number, page: Page): void {
    if (this.fd === null) return;
    const buf = Buffer.allocUnsafe(PAGE_SIZE);

    // Tulis header
    PAGE_MAGIC.copy(buf, 0);
    buf.writeUInt32LE(pageId,              4);
    buf.writeUInt8  (page.header.pageType, 8);
    buf.writeUInt16LE(page.header.keyCount, 9);
    buf.writeUInt32LE(page.header.nextPage, 11);
    buf.writeUInt32LE(page.header.prevPage, 15);
    buf.writeUInt8  (page.header.flags,    19);
    // CRC covers bytes 0–19 (sebelum CRC field itu sendiri)
    buf.writeUInt32LE(crc32(buf.subarray(0, 20)), 20);

    // Tulis payload
    page.data.copy(buf, PAGE_HEADER_SIZE);

    const offset = INDEX_HEADER_SIZE + pageId * PAGE_SIZE;
    fs.writeSync(this.fd, buf, 0, PAGE_SIZE, offset);
    page.dirty = false;
  }

  private async _initFile(): Promise<void> {
    if (this.fd === null) return;
    // Tulis kosong header
    const header = Buffer.alloc(INDEX_HEADER_SIZE);
    fs.writeSync(this.fd, header, 0, INDEX_HEADER_SIZE, 0);
    this.pageCount  = 0;
    this.rootPageId = 0;
    this._writeFileHeader();
  }

  private _writeFileHeader(): void {
    if (this.fd === null) return;
    const h = Buffer.alloc(INDEX_HEADER_SIZE);
    let p = 0;
    PAGE_MAGIC.copy(h, p);                       p += 4;
    h.writeUInt32LE(2, p);                       p += 4; // version
    h.writeUInt32LE(this.rootPageId, p);         p += 4;
    h.writeUInt32LE(this.pageCount, p);          p += 4;
    h.writeDoubleBE(Date.now(), p);              p += 8;
    h.writeUInt32LE(crc32(h.subarray(0, p)), p);
    fs.writeSync(this.fd, h, 0, INDEX_HEADER_SIZE, 0);
  }

  private _readFileHeader(): void {
    if (this.fd === null) return;
    const h = Buffer.allocUnsafe(INDEX_HEADER_SIZE);
    fs.readSync(this.fd, h, 0, INDEX_HEADER_SIZE, 0);
    let p = 4; // skip magic
    p += 4;    // version
    this.rootPageId = h.readUInt32LE(p); p += 4;
    this.pageCount  = h.readUInt32LE(p); p += 4;
  }
}
