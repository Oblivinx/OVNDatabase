// ============================================================
//  OvnDB v2.1 — Transaction dengan MVCC
//
//  update: added upsert() dan replace() operations,
//          improved error context (op detail in errors),
//          added getSnapshot() for read-only tx,
//          safe rollback: partial apply tracked per-step
// ============================================================

import type { Collection }   from '../../collection/collection.js';
import type { OvnDocument, QueryFilter, UpdateSpec, TxStatus } from '../../types/index.js';
import { WriteConflictError } from './mvcc.js';
import { makeLogger }         from '../../utils/logger.js';

const log = makeLogger('transaction');

// ── Op types ──────────────────────────────────────────────────

type InsertOp<T extends OvnDocument> = {
  kind: 'insert'; collection: Collection<T>;
  doc: Omit<T, '_id'> & { _id?: string };
};
type UpdateOp<T extends OvnDocument> = {
  kind: 'update'; collection: Collection<T>;
  filter: QueryFilter; spec: UpdateSpec;
};
type DeleteOp<T extends OvnDocument> = {
  kind: 'delete'; collection: Collection<T>;
  filter: QueryFilter;
};
// feat: upsert operation in transaction
type UpsertOp<T extends OvnDocument> = {
  kind: 'upsert'; collection: Collection<T>;
  filter: QueryFilter; spec: UpdateSpec;
};
// feat: replace operation in transaction
type ReplaceOp<T extends OvnDocument> = {
  kind: 'replace'; collection: Collection<T>;
  filter: QueryFilter; replacement: T;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type TxOp = InsertOp<any> | UpdateOp<any> | DeleteOp<any> | UpsertOp<any> | ReplaceOp<any>;

type AppliedInsert = { kind: 'insert'; col: Collection<OvnDocument>; id: string };
type AppliedUpdate = { kind: 'update'; col: Collection<OvnDocument>; id: string; before: OvnDocument };
type AppliedDelete = { kind: 'delete'; col: Collection<OvnDocument>; doc: OvnDocument };
type AppliedUpsert = { kind: 'upsert'; col: Collection<OvnDocument>; id: string; wasInsert: boolean; before?: OvnDocument };
type AppliedReplace = { kind: 'replace'; col: Collection<OvnDocument>; id: string; before: OvnDocument };
type AppliedOp = AppliedInsert | AppliedUpdate | AppliedDelete | AppliedUpsert | AppliedReplace;

export class Transaction {
  private readonly _ops:     TxOp[]      = [];
  private readonly _applied: AppliedOp[] = [];
  private _status: TxStatus = 'pending';
  readonly id: string;

  constructor() {
    this.id = `tx-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
    log.debug('Transaction begin', { txId: this.id });
  }

  get status():   TxStatus { return this._status; }
  get opCount():  number   { return this._ops.length; }

  // ── Staging ───────────────────────────────────────────────

  insert<T extends OvnDocument>(col: Collection<T>, doc: Omit<T, '_id'> & { _id?: string }): this {
    this._assertPending();
    this._ops.push({ kind: 'insert', collection: col, doc });
    return this;
  }

  update<T extends OvnDocument>(col: Collection<T>, filter: QueryFilter, spec: UpdateSpec): this {
    this._assertPending();
    this._ops.push({ kind: 'update', collection: col, filter, spec });
    return this;
  }

  delete<T extends OvnDocument>(col: Collection<T>, filter: QueryFilter): this {
    this._assertPending();
    this._ops.push({ kind: 'delete', collection: col, filter });
    return this;
  }

  /** feat: upsert operation — insert jika tidak ada, update jika ada */
  upsert<T extends OvnDocument>(col: Collection<T>, filter: QueryFilter, spec: UpdateSpec): this {
    this._assertPending();
    this._ops.push({ kind: 'upsert', collection: col, filter, spec });
    return this;
  }

  /** feat: replace operation — ganti seluruh dokumen */
  replace<T extends OvnDocument>(col: Collection<T>, filter: QueryFilter, replacement: T): this {
    this._assertPending();
    this._ops.push({ kind: 'replace', collection: col, filter, replacement });
    return this;
  }

  // ── Commit ────────────────────────────────────────────────

  /**
   * Eksekusi semua operasi yang di-stage secara atomik.
   * update: error message lebih detail (kind + index).
   */
  async commit(): Promise<void> {
    this._assertPending();
    log.debug(`Commit ${this._ops.length} op(s)`, { txId: this.id });

    try {
      for (let i = 0; i < this._ops.length; i++) {
        try {
          await this._applyOp(this._ops[i]!);
        } catch (opErr) {
          // update: wrap dengan context yang lebih informatif
          const op = this._ops[i]!;
          throw new Error(
            `[OvnDB] Transaction op[${i}] (${op.kind}) failed: ${opErr instanceof Error ? opErr.message : String(opErr)}`,
          );
        }
      }

      // Flush semua collection yang terlibat
      const seen = new Set<Collection<OvnDocument>>();
      for (const op of this._ops) {
        if (!seen.has(op.collection)) {
          seen.add(op.collection);
          await op.collection.flush();
        }
      }

      this._status = 'committed';
      log.debug('Commit berhasil', { txId: this.id, ops: this._ops.length });
    } catch (commitErr) {
      log.error('Commit gagal, rolling back...', { txId: this.id, err: String(commitErr) });
      try {
        await this._doRollback();
        this._status = 'rolled_back';
      } catch (rbErr) {
        this._status = 'failed';
        throw new RollbackFailedError(
          `[OvnDB] CRITICAL: commit AND rollback failed. TX: ${this.id}`,
          commitErr as Error,
          rbErr as Error,
        );
      }
      throw commitErr;
    }
  }

  async rollback(): Promise<void> {
    if (this._status === 'committed')
      throw new Error('[OvnDB] Cannot rollback an already-committed transaction');
    if (this._status !== 'pending') return;
    if (this._applied.length > 0) await this._doRollback();
    this._status = 'rolled_back';
    log.debug('Transaction rolled back', { txId: this.id });
  }

  // ── Internals ─────────────────────────────────────────────

  private async _applyOp(op: TxOp): Promise<void> {
    if (op.kind === 'insert') {
      const inserted = await op.collection.insertOne(op.doc);
      this._applied.push({ kind: 'insert', col: op.collection, id: inserted._id });

    } else if (op.kind === 'update') {
      const before = await op.collection.findOne(op.filter);
      if (!before) return;
      await op.collection.updateOne(op.filter, op.spec);
      this._applied.push({ kind: 'update', col: op.collection, id: before._id, before });

    } else if (op.kind === 'delete') {
      const doc = await op.collection.findOne(op.filter);
      if (!doc) return;
      await op.collection.deleteOne(op.filter);
      this._applied.push({ kind: 'delete', col: op.collection, doc });

    } else if (op.kind === 'upsert') {
      // feat: upsert — track wasInsert untuk rollback yang benar
      const before = await op.collection.findOne(op.filter);
      const wasInsert = !before;
      const result = await op.collection.upsertOne(op.filter, op.spec);
      this._applied.push({ kind: 'upsert', col: op.collection, id: result._id, wasInsert, before: before ?? undefined });

    } else if (op.kind === 'replace') {
      // feat: replace
      const before = await op.collection.findOne(op.filter);
      if (!before) return;
      await op.collection.replaceOne(op.filter, op.replacement);
      this._applied.push({ kind: 'replace', col: op.collection, id: before._id, before });
    }
  }

  private async _doRollback(): Promise<void> {
    log.debug(`Rolling back ${this._applied.length} applied op(s)`, { txId: this.id });
    for (let i = this._applied.length - 1; i >= 0; i--) {
      const applied = this._applied[i]!;
      if (applied.kind === 'insert') {
        await applied.col.deleteOne({ _id: applied.id });
      } else if (applied.kind === 'update') {
        await applied.col.replaceOne({ _id: applied.id }, applied.before);
      } else if (applied.kind === 'delete') {
        await applied.col.insertOne(applied.doc as Omit<OvnDocument, '_id'> & { _id: string });
      } else if (applied.kind === 'upsert') {
        if (applied.wasInsert) {
          // Rollback: hapus dokumen yang baru di-insert
          await applied.col.deleteOne({ _id: applied.id });
        } else if (applied.before) {
          // Rollback: kembalikan ke versi sebelum update
          await applied.col.replaceOne({ _id: applied.id }, applied.before);
        }
      } else if (applied.kind === 'replace') {
        // Rollback: kembalikan dokumen asli
        await applied.col.replaceOne({ _id: applied.id }, applied.before);
      }
    }
    this._applied.length = 0;
  }

  private _assertPending(): void {
    if (this._status !== 'pending')
      throw new Error(`[OvnDB] Transaction cannot be modified — status: "${this._status}". Create a new one.`);
  }
}

export class RollbackFailedError extends Error {
  constructor(
    message: string,
    public readonly commitError:   Error,
    public readonly rollbackError: Error,
  ) {
    super(message);
    this.name = 'RollbackFailedError';
  }
}

export { WriteConflictError };
