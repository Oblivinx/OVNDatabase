// ============================================================
//  OvnDB v4.0 — Secondary Index Manager (On-disk)
//
//  PERUBAHAN DARI v3:
//   v4: Compound Index — IndexDefinition.field bisa string[]
//       Composite key disimpan sebagai "val1\x00val2\x00val3"
//       (\x00 adalah separator yang tidak mungkin ada di nilai normal)
//       File index: collection.idx.city__role (join dengan __)
//
//  From v2:
//   - Two-phase validation: semua unique check sebelum apply apapun
//   - Sparse index: skip dokumen yang tidak punya field
//   - Partial index: index hanya dokumen yang cocok dengan filter
//   - Dot-notation field paths (misal 'address.city')
// ============================================================

import fsp from 'fs/promises';
import path from 'path';
import type { IndexDefinition, QueryFilter } from '../../types/index.js';
import { matchFilter } from '../query/filter.js';
import { makeLogger } from '../../utils/logger.js';

const log = makeLogger('secondary-index');

/** Separator untuk composite key — NUL byte tidak muncul di JSON value normal */
const COMPOSITE_SEP = '\x00';

interface IndexState {
  map: Map<string, Set<string>>; // compositeKey → Set<_id>
  sortedKeys: string[];                 // diurutkan untuk range query
  dirty: boolean;
  def: IndexDefinition;
}

export class SecondaryIndexManager {
  private readonly indexes: Map<string, IndexState> = new Map();
  private readonly dirPath: string;
  private readonly collection: string;

  constructor(dirPath: string, collection: string) {
    this.dirPath = dirPath;
    this.collection = collection;
  }

  addIndex(def: IndexDefinition): void {
    const key = this._indexKey(def.field);
    if (!this.indexes.has(key)) {
      this.indexes.set(key, {
        map: new Map(),
        sortedKeys: [],
        dirty: false,
        def,
      });
    }
  }

  async open(): Promise<void> {
    for (const [idxKey, state] of this.indexes) {
      try {
        const buf = await fsp.readFile(this._idxPath(state.def.field), 'utf8');
        this._deserialize(state, JSON.parse(buf));
        log.debug(`Loaded index "${idxKey}"`, { collection: this.collection, entries: state.map.size });
      } catch {
        log.debug(`Index "${idxKey}" tidak ditemukan, akan dibangun ulang`, { collection: this.collection });
      }
    }
  }

  async save(): Promise<void> {
    for (const [, state] of this.indexes) {
      if (!state.dirty) continue;
      const data = this._serialize(state);
      const idxPath = this._idxPath(state.def.field);
      // Ensure directory exists (may not exist for new compound indexes)
      await fsp.mkdir(path.dirname(idxPath), { recursive: true });
      const tmp = idxPath + '.tmp';
      // SECURITY: mode 0o600 — index file berisi mapping nilai field → _id
      await fsp.writeFile(tmp, JSON.stringify(data), { encoding: 'utf8', mode: 0o600 });
      await fsp.rename(tmp, idxPath);
      state.dirty = false;
      log.debug(`Saved index "${this._indexKey(state.def.field)}"`, { entries: state.map.size });
    }
  }

  // ── Mutation Hooks ────────────────────────────────────────

  /**
   * Dipanggil setelah insertOne. Two-phase: validasi dulu, apply kemudian.
   * Ini memastikan partial failure tidak mengakibatkan index korup.
   */
  onInsert(doc: Record<string, unknown>): void {
    const id = doc['_id'] as string;

    // Phase 1: validasi semua unique constraint
    for (const [, state] of this.indexes) {
      if (!this._docMatchesPartial(doc, state)) continue;
      if (!state.def.unique) continue;
      const compositeKey = this._getCompositeKey(doc, state.def.field);
      if (compositeKey === null) continue; // sparse: skip null
      const existing = state.map.get(compositeKey);
      if (existing && !existing.has(id))
        throw new Error(`[OvnDB] Unique constraint violation: ${this.collection}.${this._indexKey(state.def.field)} = "${compositeKey}"`);
    }

    // Phase 2: apply semua
    for (const [, state] of this.indexes) {
      if (!this._docMatchesPartial(doc, state)) continue;
      const compositeKey = this._getCompositeKey(doc, state.def.field);
      if (compositeKey === null) {
        if (!state.def.sparse) continue;
        continue;
      }
      this._addToIndex(state, compositeKey, id);
    }
  }

  onUpdate(oldDoc: Record<string, unknown>, newDoc: Record<string, unknown>): void {
    const id = newDoc['_id'] as string;

    type Change = { state: IndexState; oldKey: string | null; newKey: string | null };
    const changes: Change[] = [];

    for (const [, state] of this.indexes) {
      const oldKey = this._getCompositeKey(oldDoc, state.def.field);
      const newKey = this._getCompositeKey(newDoc, state.def.field);
      if (oldKey === newKey) continue;
      changes.push({ state, oldKey, newKey });
    }

    // Phase 1: validasi
    for (const { state, newKey } of changes) {
      if (!state.def.unique || newKey === null) continue;
      const existing = state.map.get(newKey);
      if (existing && !existing.has(id))
        throw new Error(`[OvnDB] Unique constraint violation on update: ${this.collection}.${this._indexKey(state.def.field)} = "${newKey}"`);
    }

    // Phase 2: apply
    for (const { state, oldKey, newKey } of changes) {
      if (oldKey !== null) this._removeFromIndex(state, oldKey, id);
      if (newKey !== null) this._addToIndex(state, newKey, id);
    }
  }

  onDelete(doc: Record<string, unknown>): void {
    const id = doc['_id'] as string;
    for (const [, state] of this.indexes) {
      const compositeKey = this._getCompositeKey(doc, state.def.field);
      if (compositeKey === null) continue;
      this._removeFromIndex(state, compositeKey, id);
    }
  }

  // ── Lookup ────────────────────────────────────────────────

  /**
   * Exact match lookup for single or compound field.
   * - singleField: lookup('email', 'x@y.com')
   * - compoundField: lookup(['city', 'role'], 'Jakarta\x00admin')
   * null = field tidak di-index.
   */
  lookup(field: string | string[], value: unknown): string[] | null {
    const key = this._indexKey(field);
    const state = this.indexes.get(key);
    if (!state) return null;
    const ids = state.map.get(String(value));
    return ids ? [...ids] : [];
  }

  /**
   * Range lookup untuk single-field index.
   * Compound index tidak mendukung range (hanya exact match).
   */
  lookupRange(field: string, gte?: string, lte?: string): string[] {
    const state = this.indexes.get(field);
    if (!state) return [];
    const keys = state.sortedKeys;
    const result: string[] = [];
    let start = 0;
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
      for (const id of state.map.get(k)!) result.push(id);
    }
    return result;
  }

  /**
   * v4.0: Compound lookup — menerima object dengan semua field compound
   * Contoh: lookupCompound(['city', 'role'], { city: 'Jakarta', role: 'admin' })
   */
  lookupCompound(fields: string[], values: Record<string, unknown>): string[] | null {
    const key = this._indexKey(fields);
    const state = this.indexes.get(key);
    if (!state) return null;
    const compositeKey = fields.map(f => String(values[f] ?? '')).join(COMPOSITE_SEP);
    const ids = state.map.get(compositeKey);
    return ids ? [...ids] : [];
  }

  hasIndex(field: string | string[]): boolean {
    return this.indexes.has(this._indexKey(field));
  }

  /** Kembalikan semua definisi index yang terdaftar */
  getIndexDefs(): IndexDefinition[] {
    return [...this.indexes.values()].map(s => s.def);
  }

  /** feat: clearAll — kosongkan semua data di semua index (untuk truncate). */
  clearAll(): void {
    for (const state of this.indexes.values()) {
      state.map.clear();
      state.sortedKeys = [];
      state.dirty = true;
    }
  }

  /** Rebuild semua index dari kumpulan dokumen (misal setelah compaction). */
  rebuildFromDocs(docs: Iterable<Record<string, unknown>>): void {
    for (const state of this.indexes.values()) {
      state.map.clear(); state.sortedKeys = []; state.dirty = true;
    }
    for (const doc of docs) {
      try { this.onInsert(doc); } catch { /* abaikan unique violation saat rebuild */ }
    }
    // Re-sort setelah bulk insert
    for (const state of this.indexes.values()) state.sortedKeys.sort();
  }

  // ── Privates ──────────────────────────────────────────────

  /** Buat canonical key untuk Map lookup dari field (string atau string[]) */
  private _indexKey(field: string | string[]): string {
    return Array.isArray(field) ? field.join('__') : field;
  }

  /**
   * Extract composite key dari dokumen.
   * - Single field: return nilai field sebagai string (atau null jika undefined/null)
   * - Compound field[]: return "val1\x00val2\x00..." (null jika SEMUA field null/undefined)
   */
  private _getCompositeKey(doc: Record<string, unknown>, field: string | string[]): string | null {
    if (!Array.isArray(field)) {
      const val = this._getField(doc, field);
      return val !== undefined && val !== null ? String(val) : null;
    }

    const parts: string[] = [];
    let allNull = true;
    for (const f of field) {
      const val = this._getField(doc, f);
      if (val !== undefined && val !== null) {
        allNull = false;
        parts.push(String(val));
      } else {
        parts.push('');
      }
    }
    if (allNull) return null;
    return parts.join(COMPOSITE_SEP);
  }

  private _addToIndex(state: IndexState, key: string, id: string): void {
    if (!state.map.has(key)) {
      state.map.set(key, new Set());
      // Insert ke posisi sorted
      let lo = 0, hi = state.sortedKeys.length;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (state.sortedKeys[mid]! < key) lo = mid + 1; else hi = mid;
      }
      state.sortedKeys.splice(lo, 0, key);
    }
    state.map.get(key)!.add(id);
    state.dirty = true;
  }

  private _removeFromIndex(state: IndexState, key: string, id: string): void {
    const set = state.map.get(key);
    if (!set) return;
    set.delete(id);
    if (set.size === 0) {
      state.map.delete(key);
      const pos = state.sortedKeys.indexOf(key);
      if (pos >= 0) state.sortedKeys.splice(pos, 1);
    }
    state.dirty = true;
  }

  private _docMatchesPartial(doc: Record<string, unknown>, state: IndexState): boolean {
    if (!state.def.partial) return true;
    return matchFilter(doc, state.def.partial as QueryFilter);
  }

  private _getField(doc: Record<string, unknown>, fieldPath: string): unknown {
    return fieldPath.split('.').reduce<unknown>((o, k) =>
      o !== null && typeof o === 'object' ? (o as Record<string, unknown>)[k] : undefined, doc);
  }

  private _idxPath(field: string | string[]): string {
    const name = this._indexKey(field);
    const safe = name.replace(/[^a-zA-Z0-9_.]/g, '_');
    return path.join(this.dirPath, `${this.collection}.idx.${safe}`);
  }

  private _serialize(state: IndexState): object {
    const entries: [string, string[]][] = [];
    for (const [k, ids] of state.map) entries.push([k, [...ids]]);
    return {
      unique: state.def.unique,
      sparse: state.def.sparse,
      compound: Array.isArray(state.def.field),
      field: state.def.field,
      entries,
    };
  }

  private _deserialize(state: IndexState, data: { unique: boolean; sparse?: boolean; entries: [string, string[]][] }): void {
    state.map.clear(); state.sortedKeys = [];
    for (const [k, ids] of data.entries) {
      state.map.set(k, new Set(ids));
      state.sortedKeys.push(k);
    }
    state.sortedKeys.sort();
  }
}
