// ============================================================
//  OvnDB v3.0 — On-disk Paged B+ Tree
//
//  G7 FIX: tambah clear() untuk reset tree ke state awal O(1).
//  G8 FIX: entries() sekarang support limit/offset untuk cursor pagination.
//  Semua operasi existing dipertahankan.
// ============================================================
const T = 128;
const MAX_KEYS = 2 * T - 1;
const POINTER_SIZE = 4 + 8 + 4 + 4 + 8;
const CHILD_SIZE = 4;
export class PagedBPlusTree {
    pm;
    _size = 0n;
    _firstLeafId = 0;
    _lastLeafId = 0;
    constructor(pm) { this.pm = pm; }
    get size() { return this._size; }
    async init() {
        if (this.pm.totalPages === 0) {
            const { pageId } = await this.pm.allocPage(2 /* PageType.LEAF */);
            this.pm.rootPage = pageId;
            this._firstLeafId = pageId;
            this._lastLeafId = pageId;
        }
        else {
            this._firstLeafId = await this._findFirstLeaf(this.pm.rootPage);
            this._lastLeafId = await this._findLastLeaf(this.pm.rootPage);
        }
    }
    // ── G7: clear() — reset tree ke state awal O(1) ──────────
    /**
     * Hapus semua entry dan reset tree ke root leaf kosong.
     * O(1) — tidak ada loop per-record.
     * Dipakai oleh StorageEngine.deleteAll().
     */
    async clear() {
        await this.pm.reset(); // truncate file ke header saja
        await this.init(); // buat root leaf baru
        this._size = 0n;
    }
    // ── G8: count() — hitung semua entry ──────────────────────
    async count() { return this._size; }
    // ── Public API ───────────────────────────────────────────
    async get(key) {
        const { leafId, idx } = await this._findInLeaf(key);
        const page = await this.pm.readPage(leafId);
        const { keys, vals } = this._readLeaf(page.data);
        if (idx < keys.length && keys[idx] === key)
            return vals[idx];
        return undefined;
    }
    async has(key) {
        return (await this.get(key)) !== undefined;
    }
    async set(key, val) {
        const { leafId, idx } = await this._findInLeaf(key);
        const page = await this.pm.readPage(leafId);
        const { keys, vals } = this._readLeaf(page.data);
        if (idx < keys.length && keys[idx] === key) {
            vals[idx] = val;
            this._writeLeaf(page.data, page.header, keys, vals);
            this.pm.markDirty(leafId);
        }
        else {
            await this._insert(key, val);
            this._size++;
        }
    }
    async delete(key) {
        const { leafId, idx } = await this._findInLeaf(key);
        const page = await this.pm.readPage(leafId);
        const { keys, vals } = this._readLeaf(page.data);
        if (idx >= keys.length || keys[idx] !== key)
            return false;
        keys.splice(idx, 1);
        vals.splice(idx, 1);
        page.header.keyCount--;
        this._writeLeaf(page.data, page.header, keys, vals);
        this.pm.markDirty(leafId);
        if (this._size > 0n)
            this._size--;
        return true;
    }
    async *range(gte, lte) {
        const startLeafId = gte
            ? (await this._findInLeaf(gte)).leafId
            : this._firstLeafId;
        let leafId = startLeafId;
        while (leafId !== null && leafId !== 0) {
            const page = await this.pm.readPage(leafId);
            const { keys, vals } = this._readLeaf(page.data);
            for (let i = 0; i < keys.length; i++) {
                const k = keys[i];
                if (gte && k < gte)
                    continue;
                if (lte && k > lte)
                    return;
                yield [k, vals[i]];
            }
            leafId = page.header.nextPage || null;
        }
    }
    /**
     * G8: entries() dengan support limit dan after (cursor pagination).
     * Jauh lebih efisien dari meload semua entry lalu slice.
     */
    async *entries(opts) {
        let emitted = 0;
        const limit = opts?.limit ?? Infinity;
        for await (const [k, ptr] of this.range(opts?.gte)) {
            if (emitted >= limit)
                return;
            if (opts?.after && k <= opts.after)
                continue;
            yield [k, ptr];
            emitted++;
        }
    }
    // ── Insert + Split ────────────────────────────────────────
    async _insert(key, val) {
        const root = await this.pm.readPage(this.pm.rootPage);
        if (root.header.keyCount >= MAX_KEYS) {
            const { pageId: newRootId, page: newRoot } = await this.pm.allocPage(1 /* PageType.INTERNAL */);
            const oldRootId = this.pm.rootPage;
            const children = [oldRootId];
            this._writeInternal(newRoot.data, newRoot.header, [], children);
            this.pm.markDirty(newRootId);
            await this._splitChild(newRootId, 0, oldRootId);
            this.pm.rootPage = newRootId;
        }
        await this._insertNonFull(this.pm.rootPage, key, val);
    }
    async _insertNonFull(pageId, key, val) {
        const page = await this.pm.readPage(pageId);
        if (page.header.pageType === 2 /* PageType.LEAF */) {
            const { keys, vals } = this._readLeaf(page.data);
            const idx = this._bsearch(keys, key);
            keys.splice(idx, 0, key);
            vals.splice(idx, 0, val);
            page.header.keyCount++;
            this._writeLeaf(page.data, page.header, keys, vals);
            this.pm.markDirty(pageId);
        }
        else {
            const { keys, children } = this._readInternal(page.data);
            let i = this._bsearch(keys, key);
            if (i < keys.length && keys[i] === key)
                i++;
            const childId = children[i];
            const child = await this.pm.readPage(childId);
            if (child.header.keyCount >= MAX_KEYS) {
                await this._splitChild(pageId, i, childId);
                const updated = this._readInternal(page.data);
                if (i < updated.keys.length && key > updated.keys[i])
                    i++;
                await this._insertNonFull(updated.children[i], key, val);
            }
            else {
                await this._insertNonFull(childId, key, val);
            }
        }
    }
    async _splitChild(parentId, i, childId) {
        const parent = await this.pm.readPage(parentId);
        const child = await this.pm.readPage(childId);
        if (child.header.pageType === 2 /* PageType.LEAF */) {
            const { keys, vals } = this._readLeaf(child.data);
            const mid = Math.floor(keys.length / 2);
            const { pageId: rightId, page: right } = await this.pm.allocPage(2 /* PageType.LEAF */);
            const rightKeys = keys.splice(mid);
            const rightVals = vals.splice(mid);
            right.header.nextPage = child.header.nextPage;
            right.header.prevPage = childId;
            if (child.header.nextPage) {
                const next = await this.pm.readPage(child.header.nextPage);
                next.header.prevPage = rightId;
                this.pm.markDirty(child.header.nextPage);
            }
            else {
                this._lastLeafId = rightId;
            }
            child.header.nextPage = rightId;
            this._writeLeaf(child.data, child.header, keys, vals);
            this._writeLeaf(right.data, right.header, rightKeys, rightVals);
            this.pm.markDirty(childId);
            this.pm.markDirty(rightId);
            const { keys: pKeys, children: pChildren } = this._readInternal(parent.data);
            pKeys.splice(i, 0, rightKeys[0]);
            pChildren.splice(i + 1, 0, rightId);
            parent.header.keyCount++;
            this._writeInternal(parent.data, parent.header, pKeys, pChildren);
            this.pm.markDirty(parentId);
        }
        else {
            const { keys, children } = this._readInternal(child.data);
            const mid = T - 1;
            const median = keys.splice(mid, 1)[0];
            const rightKeys = keys.splice(mid);
            const rightChildren = children.splice(mid + 1);
            const { pageId: rightId, page: right } = await this.pm.allocPage(1 /* PageType.INTERNAL */);
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
    async _findInLeaf(key) {
        let pageId = this.pm.rootPage;
        while (true) {
            const page = await this.pm.readPage(pageId);
            if (page.header.pageType === 2 /* PageType.LEAF */) {
                const { keys } = this._readLeaf(page.data);
                return { leafId: pageId, idx: this._bsearch(keys, key) };
            }
            const { keys, children } = this._readInternal(page.data);
            let i = this._bsearch(keys, key);
            if (i < keys.length && keys[i] === key)
                i++;
            pageId = children[Math.min(i, children.length - 1)];
        }
    }
    async _findFirstLeaf(pageId) {
        const page = await this.pm.readPage(pageId);
        if (page.header.pageType === 2 /* PageType.LEAF */)
            return pageId;
        const { children } = this._readInternal(page.data);
        return this._findFirstLeaf(children[0]);
    }
    async _findLastLeaf(pageId) {
        const page = await this.pm.readPage(pageId);
        if (page.header.pageType === 2 /* PageType.LEAF */)
            return pageId;
        const { children } = this._readInternal(page.data);
        return this._findLastLeaf(children[children.length - 1]);
    }
    _bsearch(arr, target) {
        let lo = 0, hi = arr.length;
        while (lo < hi) {
            const mid = (lo + hi) >> 1;
            if (arr[mid] < target)
                lo = mid + 1;
            else
                hi = mid;
        }
        return lo;
    }
    _readLeaf(data) {
        const keys = [];
        const vals = [];
        if (data.length < 2)
            return { keys, vals };
        const count = data.readUInt16LE(0);
        let pos = 2;
        for (let i = 0; i < count && pos < data.length; i++) {
            const kLen = data.readUInt16LE(pos);
            pos += 2;
            if (pos + kLen > data.length)
                break;
            keys.push(data.toString('utf8', pos, pos + kLen));
            pos += kLen;
            if (pos + POINTER_SIZE > data.length)
                break;
            vals.push({
                segmentId: data.readUInt32LE(pos),
                offset: Number(data.readBigUInt64LE(pos + 4)),
                totalSize: data.readUInt32LE(pos + 12),
                dataSize: data.readUInt32LE(pos + 16),
                txId: data.readBigUInt64LE(pos + 20),
            });
            pos += POINTER_SIZE;
        }
        return { keys, vals };
    }
    _writeLeaf(data, header, keys, vals) {
        data.fill(0);
        data.writeUInt16LE(keys.length, 0);
        header.keyCount = keys.length;
        let pos = 2;
        for (let i = 0; i < keys.length; i++) {
            const kb = Buffer.from(keys[i], 'utf8');
            data.writeUInt16LE(kb.length, pos);
            pos += 2;
            kb.copy(data, pos);
            pos += kb.length;
            data.writeUInt32LE(vals[i].segmentId, pos);
            pos += 4;
            data.writeBigUInt64LE(BigInt(vals[i].offset), pos);
            pos += 8;
            data.writeUInt32LE(vals[i].totalSize, pos);
            pos += 4;
            data.writeUInt32LE(vals[i].dataSize, pos);
            pos += 4;
            data.writeBigUInt64LE(vals[i].txId, pos);
            pos += 8;
        }
    }
    _readInternal(data) {
        const keys = [];
        const children = [];
        if (data.length < 2)
            return { keys, children };
        const keyCount = data.readUInt16LE(0);
        let pos = 2;
        for (let i = 0; i < keyCount && pos < data.length; i++) {
            const kLen = data.readUInt16LE(pos);
            pos += 2;
            keys.push(data.toString('utf8', pos, pos + kLen));
            pos += kLen;
        }
        for (let i = 0; i <= keyCount && pos + CHILD_SIZE <= data.length; i++) {
            children.push(data.readUInt32LE(pos));
            pos += CHILD_SIZE;
        }
        return { keys, children };
    }
    _writeInternal(data, header, keys, children) {
        data.fill(0);
        data.writeUInt16LE(keys.length, 0);
        header.keyCount = keys.length;
        let pos = 2;
        for (const k of keys) {
            const kb = Buffer.from(k, 'utf8');
            data.writeUInt16LE(kb.length, pos);
            pos += 2;
            kb.copy(data, pos);
            pos += kb.length;
        }
        for (const c of children) {
            data.writeUInt32LE(c, pos);
            pos += CHILD_SIZE;
        }
    }
}
//# sourceMappingURL=btree-paged.js.map