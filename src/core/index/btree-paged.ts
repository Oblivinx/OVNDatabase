// ============================================================
//  OvnDB v2.0 — On-disk Paged B+ Tree
//
//  PERBEDAAN KRITIS DARI v1:
//   v1: seluruh tree di RAM → 1B record = ~30 GB RAM hanya index
//   v2: tree di disk, hanya page yang diakses masuk buffer pool →
//       1T record membutuhkan ~256 MB buffer pool (bukan 30 TB)
//
//  Struktur:
//   - Internal node: keys[] + childPageIds[] — navigasi ke bawah
//   - Leaf node: keys[] + RecordPointer[] + linked list (next/prev page)
//   - T = 128 → setiap node bisa punya 255 key → height ≤ 4 untuk 1T records
//
//  Read path:  cache hit O(1), disk O(height × page_io) = ~4 I/O
//  Write path: cari leaf → modifikasi → split jika penuh → mark dirty
//  Range scan: mulai dari leaf → follow nextPage pointer
//
//  Semua operasi disk di-delegate ke PageManager (buffer pool).
// ============================================================

import { PageManager, PAGE_HEADER_SIZE, PAGE_DATA_SIZE } from '../storage/page-manager.js';
import { PageType } from '../../types/constants.js';
import type { RecordPointer } from '../../types/index.js';

// Min-degree T: setiap node (kecuali root) punya minimal T-1 key
const T        = 128;
const MAX_KEYS = 2 * T - 1; // 255 key per node

// ── Serialisasi key-value dalam page data ─────────────────────
//
//  Internal node data layout:
//   [2] keyCount
//   [keyCount × (2 + keyBytes)] key entries: [2 len][bytes key]
//   [keyCount+1 × 4] child page IDs (uint32)
//
//  Leaf node data layout:
//   [2] keyCount
//   [keyCount × (2 + keyBytes + 4+8+4+4+8)] entries:
//     [2 len][key bytes][4 segId][8 offset][4 totalSize][4 dataSize][8 txId]

const POINTER_SIZE = 4 + 8 + 4 + 4 + 8; // 28 bytes per RecordPointer
const CHILD_SIZE   = 4;                  // 4 bytes per child page ID (uint32)

export class PagedBPlusTree {
  private readonly pm: PageManager;
  private _size: bigint = 0n;
  // ID leaf terkiri & terkanan (untuk full scan tanpa traversal)
  private _firstLeafId: number = 0;
  private _lastLeafId:  number = 0;

  constructor(pm: PageManager) {
    this.pm = pm;
  }

  get size(): bigint { return this._size; }

  // ── Inisialisasi ─────────────────────────────────────────

  async init(): Promise<void> {
    if (this.pm.totalPages === 0) {
      // Tree baru — buat root leaf kosong
      const { pageId } = await this.pm.allocPage(PageType.LEAF);
      this.pm.rootPage   = pageId;
      this._firstLeafId  = pageId;
      this._lastLeafId   = pageId;
    } else {
      // Tree sudah ada — cari leaf terkiri (ikuti anak pertama internal node)
      this._firstLeafId = await this._findFirstLeaf(this.pm.rootPage);
      this._lastLeafId  = await this._findLastLeaf(this.pm.rootPage);
    }
  }

  // ── Public API ───────────────────────────────────────────

  async get(key: string): Promise<RecordPointer | undefined> {
    const { leafId, idx } = await this._findInLeaf(key);
    const page = await this.pm.readPage(leafId);
    const { keys, vals } = this._readLeaf(page.data);
    if (idx < keys.length && keys[idx] === key) return vals[idx];
    return undefined;
  }

  async has(key: string): Promise<boolean> {
    return (await this.get(key)) !== undefined;
  }

  async set(key: string, val: RecordPointer): Promise<void> {
    const { leafId, idx } = await this._findInLeaf(key);
    const page = await this.pm.readPage(leafId);
    const { keys, vals } = this._readLeaf(page.data);

    if (idx < keys.length && keys[idx] === key) {
      // Update existing
      vals[idx] = val;
      this._writeLeaf(page.data, page.header, keys, vals);
      this.pm.markDirty(leafId);
    } else {
      // Insert baru
      await this._insert(key, val);
      this._size++;
    }
  }

  async delete(key: string): Promise<boolean> {
    const { leafId, idx } = await this._findInLeaf(key);
    const page = await this.pm.readPage(leafId);
    const { keys, vals } = this._readLeaf(page.data);
    if (idx >= keys.length || keys[idx] !== key) return false;

    keys.splice(idx, 1);
    vals.splice(idx, 1);
    page.header.keyCount--;
    this._writeLeaf(page.data, page.header, keys, vals);
    this.pm.markDirty(leafId);
    this._size--;
    return true;
  }

  /**
   * Range scan — yield semua entry dengan key >= gte && <= lte.
   * Menggunakan linked list antar leaf (nextPage) → O(k) bukan O(n log n).
   */
  async *range(gte?: string, lte?: string): AsyncIterableIterator<[string, RecordPointer]> {
    // Mulai dari leaf yang paling kiri atau leaf yang mengandung gte
    const startLeafId = gte
      ? (await this._findInLeaf(gte)).leafId
      : this._firstLeafId;

    let leafId: number | null = startLeafId;
    while (leafId !== null && leafId !== 0) {
      const page = await this.pm.readPage(leafId);
      const { keys, vals } = this._readLeaf(page.data);
      for (let i = 0; i < keys.length; i++) {
        const k = keys[i]!;
        if (gte && k < gte) continue;
        if (lte && k > lte) return;
        yield [k, vals[i]!];
      }
      leafId = page.header.nextPage || null;
    }
  }

  /** Iterasi semua entry dalam urutan key. */
  async *entries(): AsyncIterableIterator<[string, RecordPointer]> {
    yield* this.range();
  }

  // ── Insert + Split ────────────────────────────────────────

  private async _insert(key: string, val: RecordPointer): Promise<void> {
    const root = await this.pm.readPage(this.pm.rootPage);
    if (root.header.keyCount >= MAX_KEYS) {
      // Root penuh — buat root baru, jadikan root lama anak kiri
      const { pageId: newRootId, page: newRoot } = await this.pm.allocPage(PageType.INTERNAL);
      const oldRootId = this.pm.rootPage;
      // Simpan ID anak pertama di awal data internal node
      const children = [oldRootId];
      this._writeInternal(newRoot.data, newRoot.header, [], children);
      this.pm.markDirty(newRootId);
      await this._splitChild(newRootId, 0, oldRootId);
      this.pm.rootPage = newRootId;
    }
    await this._insertNonFull(this.pm.rootPage, key, val);
  }

  private async _insertNonFull(pageId: number, key: string, val: RecordPointer): Promise<void> {
    const page = await this.pm.readPage(pageId);

    if (page.header.pageType === PageType.LEAF) {
      const { keys, vals } = this._readLeaf(page.data);
      const idx = this._bsearch(keys, key);
      keys.splice(idx, 0, key);
      vals.splice(idx, 0, val);
      page.header.keyCount++;
      this._writeLeaf(page.data, page.header, keys, vals);
      this.pm.markDirty(pageId);
    } else {
      const { keys, children } = this._readInternal(page.data);
      let i = this._bsearch(keys, key);
      if (i < keys.length && keys[i] === key) i++;
      const childId = children[i]!;
      const child   = await this.pm.readPage(childId);

      if (child.header.keyCount >= MAX_KEYS) {
        await this._splitChild(pageId, i, childId);
        // Setelah split, parent sudah diupdate di disk — baca ulang untuk dapat
        // children terbaru (split menambahkan satu child baru di posisi i+1)
        const updated = this._readInternal(page.data);
        if (i < updated.keys.length && key > updated.keys[i]!) i++;
        await this._insertNonFull(updated.children[i]!, key, val);
      } else {
        await this._insertNonFull(childId, key, val);
      }
    }
  }

  private async _splitChild(parentId: number, i: number, childId: number): Promise<void> {
    const parent = await this.pm.readPage(parentId);
    const child  = await this.pm.readPage(childId);

    if (child.header.pageType === PageType.LEAF) {
      const { keys, vals } = this._readLeaf(child.data);
      const mid = Math.floor(keys.length / 2);

      const { pageId: rightId, page: right } = await this.pm.allocPage(PageType.LEAF);
      const rightKeys = keys.splice(mid);
      const rightVals = vals.splice(mid);

      // Update linked list
      right.header.nextPage = child.header.nextPage;
      right.header.prevPage = childId;
      if (child.header.nextPage) {
        const next = await this.pm.readPage(child.header.nextPage);
        next.header.prevPage = rightId;
        this.pm.markDirty(child.header.nextPage);
      } else {
        this._lastLeafId = rightId;
      }
      child.header.nextPage = rightId;

      this._writeLeaf(child.data, child.header, keys, vals);
      this._writeLeaf(right.data, right.header, rightKeys, rightVals);
      this.pm.markDirty(childId);
      this.pm.markDirty(rightId);

      // Separator key = first key di right
      const { keys: pKeys, children: pChildren } = this._readInternal(parent.data);
      pKeys.splice(i, 0, rightKeys[0]!);
      pChildren.splice(i + 1, 0, rightId);
      parent.header.keyCount++;
      this._writeInternal(parent.data, parent.header, pKeys, pChildren);
      this.pm.markDirty(parentId);
    } else {
      const { keys, children } = this._readInternal(child.data);
      const mid     = T - 1;
      const median  = keys.splice(mid, 1)[0]!;
      const rightKeys     = keys.splice(mid);
      const rightChildren = children.splice(mid + 1);

      const { pageId: rightId, page: right } = await this.pm.allocPage(PageType.INTERNAL);
      child.header.keyCount = keys.length;
      right.header.keyCount = rightKeys.length;
      this._writeInternal(child.data, child.header, keys, children);
      this._writeInternal(right.data, right.header, rightKeys, rightChildren);
      this.pm.markDirty(childId);
      this.pm.markDirty(rightId);

      const { keys: pKeys, children: pChildren } = this._readInternal(parent.data);
      pKeys.splice(i, 0, median);
      pChildren.splice(i + 1, 0, rightId);
      parent.header.keyCount++;
      this._writeInternal(parent.data, parent.header, pKeys, pChildren);
      this.pm.markDirty(parentId);
    }
  }

  // ── Navigasi ─────────────────────────────────────────────

  private async _findInLeaf(key: string): Promise<{ leafId: number; idx: number }> {
    let pageId = this.pm.rootPage;
    while (true) {
      const page = await this.pm.readPage(pageId);
      if (page.header.pageType === PageType.LEAF) {
        const { keys } = this._readLeaf(page.data);
        return { leafId: pageId, idx: this._bsearch(keys, key) };
      }
      const { keys, children } = this._readInternal(page.data);
      let i = this._bsearch(keys, key);
      if (i < keys.length && keys[i] === key) i++;
      pageId = children[Math.min(i, children.length - 1)]!;
    }
  }

  private async _findFirstLeaf(pageId: number): Promise<number> {
    const page = await this.pm.readPage(pageId);
    if (page.header.pageType === PageType.LEAF) return pageId;
    const { children } = this._readInternal(page.data);
    return this._findFirstLeaf(children[0]!);
  }

  private async _findLastLeaf(pageId: number): Promise<number> {
    const page = await this.pm.readPage(pageId);
    if (page.header.pageType === PageType.LEAF) return pageId;
    const { children } = this._readInternal(page.data);
    return this._findLastLeaf(children[children.length - 1]!);
  }

  private _bsearch(arr: string[], target: string): number {
    let lo = 0, hi = arr.length;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (arr[mid]! < target) lo = mid + 1; else hi = mid;
    }
    return lo;
  }

  // ── Serialisasi Page Data ─────────────────────────────────

  private _readLeaf(data: Buffer): { keys: string[]; vals: RecordPointer[] } {
    const keys: string[] = [];
    const vals: RecordPointer[] = [];
    if (data.length < 2) return { keys, vals };
    const count = data.readUInt16LE(0);
    let pos = 2;
    for (let i = 0; i < count && pos < data.length; i++) {
      const kLen = data.readUInt16LE(pos); pos += 2;
      if (pos + kLen > data.length) break;
      keys.push(data.toString('utf8', pos, pos + kLen)); pos += kLen;
      if (pos + POINTER_SIZE > data.length) break;
      vals.push({
        segmentId: data.readUInt32LE(pos),
        offset:    Number(data.readBigUInt64LE(pos + 4)),
        totalSize: data.readUInt32LE(pos + 12),
        dataSize:  data.readUInt32LE(pos + 16),
        txId:      data.readBigUInt64LE(pos + 20),
      });
      pos += POINTER_SIZE;
    }
    return { keys, vals };
  }

  private _writeLeaf(data: Buffer, header: { keyCount: number }, keys: string[], vals: RecordPointer[]): void {
    data.fill(0);
    data.writeUInt16LE(keys.length, 0);
    header.keyCount = keys.length;
    let pos = 2;
    for (let i = 0; i < keys.length; i++) {
      const kb = Buffer.from(keys[i]!, 'utf8');
      data.writeUInt16LE(kb.length, pos);          pos += 2;
      kb.copy(data, pos);                          pos += kb.length;
      data.writeUInt32LE(vals[i]!.segmentId, pos); pos += 4;
      data.writeBigUInt64LE(BigInt(vals[i]!.offset), pos); pos += 8;
      data.writeUInt32LE(vals[i]!.totalSize, pos); pos += 4;
      data.writeUInt32LE(vals[i]!.dataSize, pos);  pos += 4;
      data.writeBigUInt64LE(vals[i]!.txId, pos);   pos += 8;
    }
  }

  private _readInternal(data: Buffer): { keys: string[]; children: number[] } {
    const keys: string[] = [];
    const children: number[] = [];
    if (data.length < 2) return { keys, children };
    const keyCount = data.readUInt16LE(0);
    let pos = 2;
    for (let i = 0; i < keyCount && pos < data.length; i++) {
      const kLen = data.readUInt16LE(pos); pos += 2;
      keys.push(data.toString('utf8', pos, pos + kLen)); pos += kLen;
    }
    for (let i = 0; i <= keyCount && pos + CHILD_SIZE <= data.length; i++) {
      children.push(data.readUInt32LE(pos)); pos += CHILD_SIZE;
    }
    return { keys, children };
  }

  private _writeInternal(data: Buffer, header: { keyCount: number }, keys: string[], children: number[]): void {
    data.fill(0);
    data.writeUInt16LE(keys.length, 0);
    header.keyCount = keys.length;
    let pos = 2;
    for (const k of keys) {
      const kb = Buffer.from(k, 'utf8');
      data.writeUInt16LE(kb.length, pos); pos += 2;
      kb.copy(data, pos);                 pos += kb.length;
    }
    for (const c of children) {
      data.writeUInt32LE(c, pos); pos += CHILD_SIZE;
    }
  }
}
