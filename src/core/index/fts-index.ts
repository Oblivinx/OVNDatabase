// ============================================================
//  OvnDB v4.0 — Full-text Search Index (FTS)
//
//  Inverted index: token → Set<_id>
//
//  Fitur:
//  - Simple tokenizer: lowercase, split by non-alphanumeric,
//    filter tokens < 2 chars
//  - Posting list intersection untuk multi-word queries
//  - Persisted sebagai JSON file per field
//
//  Cara pakai:
//    const fts = new FTSIndex(dirPath, 'users', 'name');
//    await fts.open();
//    fts.index('u1', 'Budi Santoso Jakarta');
//    fts.remove('u1', 'Budi Santoso Jakarta');
//    const ids = fts.search('budi jakarta'); // intersection → ['u1']
//    await fts.save();
// ============================================================

import fsp  from 'fs/promises';
import path from 'path';
import { makeLogger } from '../../utils/logger.js';

const log = makeLogger('fts-index');

export class FTSIndex {
  /** inverted index: token → Set of doc _id */
  private readonly posting: Map<string, Set<string>> = new Map();
  /** forward index: _id → Set of tokens (for remove) */
  private readonly forward: Map<string, Set<string>> = new Map();
  private dirty = false;

  private readonly filePath: string;

  constructor(dirPath: string, collection: string, field: string) {
    const safe    = field.replace(/[^a-zA-Z0-9_.]/g, '_');
    this.filePath = path.join(dirPath, `${collection}.fts.${safe}.json`);
  }

  async open(): Promise<void> {
    try {
      const raw  = await fsp.readFile(this.filePath, 'utf8');
      const data = JSON.parse(raw) as { posting: [string, string[]][]; forward: [string, string[]][] };
      for (const [token, ids] of data.posting)  this.posting.set(token, new Set(ids));
      for (const [id,    tks] of data.forward)   this.forward.set(id, new Set(tks));
      log.debug(`FTS index loaded`, { tokens: this.posting.size });
    } catch {
      log.debug('FTS index not found, starting fresh');
    }
  }

  async save(): Promise<void> {
    if (!this.dirty) return;
    const data = {
      posting: [...this.posting.entries()].map(([t, ids]) => [t, [...ids]] as [string, string[]]),
      forward: [...this.forward.entries()].map(([id, tks]) => [id, [...tks]] as [string, string[]]),
    };
    const tmp = this.filePath + '.tmp';
    await fsp.writeFile(tmp, JSON.stringify(data), { encoding: 'utf8', mode: 0o600 });
    await fsp.rename(tmp, this.filePath);
    this.dirty = false;
    log.debug(`FTS index saved`, { tokens: this.posting.size });
  }

  /**
   * Index a document's field value.
   * @param id    doc._id
   * @param text  field value (string)
   */
  index(id: string, text: string): void {
    const tokens = this._tokenize(text);
    if (tokens.length === 0) return;

    // Remove old tokens for this id first (if re-indexing)
    this._removeById(id);

    const tokenSet = new Set<string>();
    for (const token of tokens) {
      let set = this.posting.get(token);
      if (!set) { set = new Set(); this.posting.set(token, set); }
      set.add(id);
      tokenSet.add(token);
    }
    this.forward.set(id, tokenSet);
    this.dirty = true;
  }

  /**
   * Remove a document from the index.
   * @param id doc._id
   */
  remove(id: string): void {
    this._removeById(id);
    this.dirty = true;
  }

  /**
   * Search for documents matching ALL words in the query.
   * Returns array of matching _ids (intersection of posting lists).
   * @param query  Space-separated search terms
   */
  search(query: string): string[] {
    const tokens = this._tokenize(query);
    if (tokens.length === 0) return [];

    // Start with smallest posting list and intersect
    let result: Set<string> | null = null;

    for (const token of tokens) {
      const posting = this.posting.get(token);
      if (!posting || posting.size === 0) return []; // any token missing → no results
      if (!result) {
        result = new Set(posting);
      } else {
        for (const id of result) {
          if (!posting.has(id)) result.delete(id);
        }
      }
      if (result.size === 0) return [];
    }

    return result ? [...result] : [];
  }

  /**
   * Check if this index has any data for the given doc id
   */
  hasDoc(id: string): boolean { return this.forward.has(id); }

  get tokenCount(): number { return this.posting.size; }

  // ── Privates ──────────────────────────────────────────────

  private _tokenize(text: string): string[] {
    if (typeof text !== 'string') return [];
    return text
      .toLowerCase()
      .split(/[^a-z0-9\u00C0-\u024F]+/) // split on non-alphanumeric (supports latin extended)
      .filter(t => t.length >= 2);       // skip very short tokens
  }

  private _removeById(id: string): void {
    const tokens = this.forward.get(id);
    if (!tokens) return;
    for (const token of tokens) {
      const posting = this.posting.get(token);
      if (posting) {
        posting.delete(id);
        if (posting.size === 0) this.posting.delete(token);
      }
    }
    this.forward.delete(id);
  }
}
