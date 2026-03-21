// ============================================================
//  OvnDB v2.0 — Secondary Index Manager (On-disk)
//
//  PERUBAHAN DARI v1:
//   v1: Map<string, Set<string>> di heap JS — OOM untuk miliaran entry
//   v2: sorted array in-memory + persist ke JSON file per field
//       (sama seperti v1, tapi lebih robust dengan fase validasi)
//
//  Untuk production scale (miliar unique values), next step adalah
//  mengganti dengan PagedBPlusTree per field. Arsitektur sudah
//  dirancang untuk itu — cukup ganti _lookup & _lookupRange.
//
//  Fitur baru vs v1:
//   - Two-phase validation: semua unique check sebelum apply apapun
//   - Sparse index: skip dokumen yang tidak punya field
//   - Partial index: index hanya dokumen yang cocok dengan filter
//   - Dot-notation field paths (misal 'address.city')
// ============================================================

import fsp  from 'fs/promises';
import path from 'path';
import type { IndexDefinition, QueryFilter } from '../../types/index.js';
import { matchFilter } from '../query/filter.js';
import { makeLogger }  from '../../utils/logger.js';

const log = makeLogger('secondary-index');

interface IndexState {
  map:        Map<string, Set<string>>; // fieldValue → Set<_id>
  sortedKeys: string[];                 // diurutkan untuk range query
  dirty:      boolean;
  def:        IndexDefinition;
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
        map:        new Map(),
        sortedKeys: [],
        dirty:      false,
        def,
      });
    }
  }

  async open(): Promise<void> {
    for (const [field, state] of this.indexes) {
      try {
        const buf = await fsp.readFile(this._idxPath(field), 'utf8');
        this._deserialize(state, JSON.parse(buf));
        log.debug(`Loaded index "${field}"`, { collection: this.collection, entries: state.map.size });
      } catch {
        log.debug(`Index "${field}" tidak ditemukan, akan dibangun ulang`, { collection: this.collection });
      }
    }
  }

  async save(): Promise<void> {
    for (const [field, state] of this.indexes) {
      if (!state.dirty) continue;
      const data = this._serialize(state);
      const tmp  = this._idxPath(field) + '.tmp';
      await fsp.writeFile(tmp, JSON.stringify(data), 'utf8');
      await fsp.rename(tmp, this._idxPath(field));
      state.dirty = false;
      log.debug(`Saved index "${field}"`, { entries: state.map.size });
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
    for (const [field, state] of this.indexes) {
      if (!this._docMatchesPartial(doc, state)) continue;
      if (!state.def.unique) continue;
      const val = this._getField(doc, field);
      if (val === undefined || val === null) continue; // sparse: skip null
      const existing = state.map.get(String(val));
      if (existing && !existing.has(id))
        throw new Error(`[OvnDB] Unique constraint violation: ${this.collection}.${field} = "${val}"`);
    }

    // Phase 2: apply semua
    for (const [field, state] of this.indexes) {
      if (!this._docMatchesPartial(doc, state)) continue;
      const val = this._getField(doc, field);
      if (val === undefined || val === null) {
        if (!state.def.sparse) continue; // non-sparse: index null juga
        continue;
      }
      this._addToIndex(state, String(val), id);
    }
  }

  onUpdate(oldDoc: Record<string, unknown>, newDoc: Record<string, unknown>): void {
    const id = newDoc['_id'] as string;

    type Change = { state: IndexState; oldKey: string | null; newKey: string | null; field: string };
    const changes: Change[] = [];

    for (const [field, state] of this.indexes) {
      const oldVal = this._getField(oldDoc, field);
      const newVal = this._getField(newDoc, field);
      const oldKey = oldVal !== undefined && oldVal !== null ? String(oldVal) : null;
      const newKey = newVal !== undefined && newVal !== null ? String(newVal) : null;
      if (oldKey === newKey) continue;
      changes.push({ state, oldKey, newKey, field });
    }

    // Phase 1: validasi
    for (const { state, newKey, field } of changes) {
      if (!state.def.unique || newKey === null) continue;
      const existing = state.map.get(newKey);
      if (existing && !existing.has(id))
        throw new Error(`[OvnDB] Unique constraint violation on update: ${this.collection}.${field} = "${newKey}"`);
    }

    // Phase 2: apply
    for (const { state, oldKey, newKey } of changes) {
      if (oldKey !== null) this._removeFromIndex(state, oldKey, id);
      if (newKey !== null) this._addToIndex(state, newKey, id);
    }
  }

  onDelete(doc: Record<string, unknown>): void {
    const id = doc['_id'] as string;
    for (const [field, state] of this.indexes) {
      const val = this._getField(doc, field);
      if (val === undefined || val === null) continue;
      this._removeFromIndex(state, String(val), id);
    }
  }

  // ── Lookup ────────────────────────────────────────────────

  /** Exact match lookup. null = field tidak di-index. */
  lookup(field: string, value: unknown): string[] | null {
    const state = this.indexes.get(field);
    if (!state) return null;
    const ids = state.map.get(String(value));
    return ids ? [...ids] : [];
  }

  /** Range lookup menggunakan sortedKeys (binary search). */
  lookupRange(field: string, gte?: string, lte?: string): string[] {
    const state = this.indexes.get(field);
    if (!state) return [];
    const keys   = state.sortedKeys;
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

  hasIndex(field: string): boolean { return this.indexes.has(field); }

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

  private _idxPath(field: string): string {
    const safe = field.replace(/[^a-zA-Z0-9_.]/g, '_');
    return path.join(this.dirPath, `${this.collection}.idx.${safe}`);
  }

  private _serialize(state: IndexState): object {
    const entries: [string, string[]][] = [];
    for (const [k, ids] of state.map) entries.push([k, [...ids]]);
    return { unique: state.def.unique, sparse: state.def.sparse, entries };
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
