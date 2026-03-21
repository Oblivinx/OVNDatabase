// ============================================================
//  SecondaryIndexManager — per-field lookup tables
//
//  Supports:
//   - Exact match: findOne({ phone: '628xxx' }) → O(1)
//   - Range query: find({ level: { $gte: 5 } }) → O(log n + k)
//   - Unique constraint: phone, email, etc.
//
//  Persisted as JSON to {collection}.idx.{field}
// ============================================================
import fsp  from 'fs/promises';
import path from 'path';
import { makeLogger } from '../utils/logger.js';

const log = makeLogger('secondary-index');

export interface IndexDefinition {
  field:  string;
  unique: boolean;
}

interface IndexState {
  // fieldValue → Set<_id>
  // Keys kept sorted in sortedKeys array for range queries
  map:        Map<string, Set<string>>;
  sortedKeys: string[];        // maintained in sorted order
  dirty:      boolean;
  unique:     boolean;
}

export class SecondaryIndexManager {
  private readonly indexes:    Map<string, IndexState> = new Map();
  private readonly dirPath:    string;
  private readonly collection: string;

  constructor(dirPath: string, collection: string) {
    this.dirPath    = dirPath;
    this.collection = collection;
  }

  addIndex(def: IndexDefinition): void {
    if (!this.indexes.has(def.field)) {
      this.indexes.set(def.field, {
        map: new Map(), sortedKeys: [], dirty: false, unique: def.unique,
      });
    }
  }

  async open(): Promise<void> {
    for (const [field] of this.indexes) {
      try {
        const buf = await fsp.readFile(this._idxPath(field));
        this._deserialise(field, buf);
        log.debug(`Loaded index for field "${field}"`, { collection: this.collection });
      } catch {
        // Will be built when data is inserted or via rebuildFromDocs()
      }
    }
  }

  async save(): Promise<void> {
    for (const [field, idx] of this.indexes) {
      if (!idx.dirty) continue;
      await fsp.writeFile(this._idxPath(field), this._serialise(field));
      idx.dirty = false;
      log.debug(`Saved index for "${field}"`, { entries: idx.map.size });
    }
  }

  // ── Mutation hooks ────────────────────────────────────────

  onInsert(doc: Record<string, unknown>): void {
    const id = doc['_id'] as string;

    // ── Phase 1: Validate ALL unique constraints FIRST (atomic check) ────────
    // Jika ada satu pun yang melanggar, batalkan seluruh operasi.
    // Ini mencegah index berubah setengah jalan sebelum error terjadi.
    for (const [field, idx] of this.indexes) {
      if (!idx.unique) continue;
      const val = this._getField(doc, field);
      if (val === undefined || val === null) continue;
      const key = String(val);
      // Izinkan jika id yang sama sudah ada (upsert scenario)
      const existing = idx.map.get(key);
      if (existing && existing.size > 0 && !existing.has(id))
        throw new Error(`[OvnDB] Unique constraint violation: ${this.collection}.${field} = "${key}"`);
    }

    // ── Phase 2: Apply semua — hanya setelah SEMUA unique check lolos ────────
    for (const [field, idx] of this.indexes) {
      const val = this._getField(doc, field);
      if (val === undefined || val === null) continue;
      this._addToIndex(idx, String(val), id);
    }
  }

  onUpdate(oldDoc: Record<string, unknown>, newDoc: Record<string, unknown>): void {
    const id = newDoc['_id'] as string;

    // ── Phase 1: Kumpulkan semua perubahan yang akan dilakukan ────────────────
    type Change = { idx: IndexState; oldKey: string | null; newKey: string | null; field: string };
    const changes: Change[] = [];
    for (const [field, idx] of this.indexes) {
      const oldVal = this._getField(oldDoc, field);
      const newVal = this._getField(newDoc, field);
      const oldKey = oldVal !== undefined && oldVal !== null ? String(oldVal) : null;
      const newKey = newVal !== undefined && newVal !== null ? String(newVal) : null;
      if (oldKey === newKey) continue;
      changes.push({ idx, oldKey, newKey, field });
    }

    // ── Phase 2: Validasi unique SEMUA perubahan sebelum apply ────────────────
    for (const { idx, newKey, field } of changes) {
      if (!idx.unique || newKey === null) continue;
      const existing = idx.map.get(newKey);
      if (existing && existing.size > 0 && !existing.has(id))
        throw new Error(`[OvnDB] Unique constraint violation on update: ${this.collection}.${field} = "${newKey}"`);
    }

    // ── Phase 3: Apply semua perubahan — hanya setelah semua check lolos ──────
    for (const { idx, oldKey, newKey } of changes) {
      if (oldKey !== null) this._removeFromIndex(idx, oldKey, id);
      if (newKey !== null) this._addToIndex(idx, newKey, id);
      idx.dirty = true;
    }
  }

  onDelete(doc: Record<string, unknown>): void {
    const id = doc['_id'] as string;
    for (const [field, idx] of this.indexes) {
      const val = this._getField(doc, field);
      if (val === undefined || val === null) continue;
      this._removeFromIndex(idx, String(val), id);
    }
  }

  // ── Lookup ────────────────────────────────────────────────

  /** Returns array of matching _ids, or null if field not indexed */
  lookup(field: string, value: unknown): string[] | null {
    const idx = this.indexes.get(field);
    if (!idx) return null;
    const ids = idx.map.get(String(value));
    return ids ? [...ids] : [];
  }

  /** Range lookup: gte/lte are string-compared. Suitable for numbers stored as strings too. */
  lookupRange(field: string, gte?: string, lte?: string): string[] {
    const idx = this.indexes.get(field);
    if (!idx) return [];
    const result: string[] = [];
    // Binary search for start position in sortedKeys
    const keys = idx.sortedKeys;
    let start  = 0;
    if (gte !== undefined) {
      let lo = 0, hi = keys.length;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (keys[mid]! < gte) lo = mid + 1; else hi = mid;
      }
      start = lo;
    }
    for (let i = start; i < keys.length; i++) {
      const k = keys[i]!;
      if (lte !== undefined && k > lte) break;
      for (const id of idx.map.get(k)!) result.push(id);
    }
    return result;
  }

  hasIndex(field: string): boolean { return this.indexes.has(field); }

  rebuildFromDocs(docs: Iterable<Record<string, unknown>>): void {
    for (const idx of this.indexes.values()) {
      idx.map.clear();
      idx.sortedKeys = [];
      idx.dirty = true;
    }
    for (const doc of docs) {
      try { this.onInsert(doc); } catch { /* skip unique violations */ }
    }
    // Re-sort after bulk rebuild
    for (const idx of this.indexes.values()) {
      idx.sortedKeys.sort();
    }
  }

  // ── Internals ─────────────────────────────────────────────

  private _addToIndex(idx: IndexState, key: string, id: string): void {
    if (!idx.map.has(key)) {
      idx.map.set(key, new Set());
      // Insert into sorted position
      let lo = 0, hi = idx.sortedKeys.length;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (idx.sortedKeys[mid]! < key) lo = mid + 1; else hi = mid;
      }
      idx.sortedKeys.splice(lo, 0, key);
    }
    idx.map.get(key)!.add(id);
    idx.dirty = true;
  }

  private _removeFromIndex(idx: IndexState, key: string, id: string): void {
    const set = idx.map.get(key);
    if (!set) return;
    set.delete(id);
    if (set.size === 0) {
      idx.map.delete(key);
      const pos = idx.sortedKeys.indexOf(key);
      if (pos >= 0) idx.sortedKeys.splice(pos, 1);
    }
    idx.dirty = true;
  }

  private _idxPath(field: string): string {
    const safe = field.replace(/[^a-zA-Z0-9_]/g, '_');
    return path.join(this.dirPath, `${this.collection}.idx.${safe}`);
  }

  private _serialise(field: string): Buffer {
    const idx = this.indexes.get(field)!;
    const entries: Array<[string, string[]]> = [];
    for (const [k, ids] of idx.map) entries.push([k, [...ids]]);
    return Buffer.from(JSON.stringify({ unique: idx.unique, entries }), 'utf8');
  }

  private _deserialise(field: string, buf: Buffer): void {
    const idx = this.indexes.get(field)!;
    if (!idx) return;
    const data = JSON.parse(buf.toString('utf8')) as {
      unique: boolean; entries: Array<[string, string[]]>;
    };
    idx.map.clear();
    idx.sortedKeys = [];
    for (const [k, ids] of data.entries) {
      idx.map.set(k, new Set(ids));
      idx.sortedKeys.push(k);
    }
    idx.sortedKeys.sort();
  }

  private _getField(doc: Record<string, unknown>, path: string): unknown {
    return path.split('.').reduce<unknown>((o, k) =>
      o !== null && typeof o === 'object' ? (o as Record<string, unknown>)[k] : undefined, doc);
  }
}
