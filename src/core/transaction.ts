// ============================================================
//  Transaction — Atomic multi-collection write
//
//  API:
//    const tx = db.beginTransaction();
//    tx.insert(users, { name: 'Budi' });
//    tx.update(wallets, { _id: walletId }, { $inc: { balance: -50 } });
//    tx.delete(sessions, { _id: sessionId });
//    await tx.commit();    // ← semua berhasil, atau
//    await tx.rollback();  // ← semua dibatalkan
//
//  Isolation Level: Read-Committed (snapshot per-operasi)
//  Atomicity: Ya — jika commit gagal di tengah, rollback otomatis
//  Durability: Ya — commit flush semua collection sekaligus
//
//  Cara kerja:
//    1. Semua operasi dikumpulkan ke dalam buffer lokal (tidak langsung ke Collection)
//    2. Saat commit: apply semua operasi ke Collection secara berurutan dalam try-catch
//    3. Jika ada yang gagal: rollback semua yang sudah ter-apply
//    4. Jika rollback gagal: log critical error + rethrow
// ============================================================
import type { Collection }   from '../collection.js';
import type { OvnDocument, QueryFilter, UpdateSpec } from '../types.js';
import { makeLogger }        from '../utils/logger.js';

const log = makeLogger('transaction');

// ── Op types ──────────────────────────────────────────────────
type InsertOp<T extends OvnDocument> = {
  kind: 'insert';
  collection: Collection<T>;
  doc: Omit<T, '_id'> & { _id?: string };
};

type UpdateOp<T extends OvnDocument> = {
  kind: 'update';
  collection: Collection<T>;
  filter: QueryFilter;
  spec: UpdateSpec;
};

type DeleteOp<T extends OvnDocument> = {
  kind: 'delete';
  collection: Collection<T>;
  filter: QueryFilter;
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type TxOp = InsertOp<any> | UpdateOp<any> | DeleteOp<any>;

// What was actually applied — used for rollback
type AppliedInsert  = { kind: 'insert'; collection: Collection<OvnDocument>; id: string };
type AppliedUpdate  = { kind: 'update'; collection: Collection<OvnDocument>; id: string; before: OvnDocument };
type AppliedDelete  = { kind: 'delete'; collection: Collection<OvnDocument>; doc: OvnDocument };
type AppliedOp      = AppliedInsert | AppliedUpdate | AppliedDelete;

export type TxStatus = 'pending' | 'committed' | 'rolled_back' | 'failed';

export class Transaction {
  private readonly _ops:     TxOp[]     = [];
  private readonly _applied: AppliedOp[] = [];
  private _status: TxStatus = 'pending';
  private readonly _id: string;

  constructor() {
    this._id = `tx-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
    log.debug(`Transaction dimulai`, { txId: this._id });
  }

  get status(): TxStatus { return this._status; }
  get id():     string   { return this._id; }
  get opCount(): number  { return this._ops.length; }

  // ── Staging ───────────────────────────────────────────────

  /**
   * Stage sebuah insert. Belum dieksekusi sampai commit().
   */
  insert<T extends OvnDocument>(
    collection: Collection<T>,
    doc: Omit<T, '_id'> & { _id?: string },
  ): this {
    this._assertPending();
    this._ops.push({ kind: 'insert', collection, doc });
    return this;
  }

  /**
   * Stage sebuah update. Belum dieksekusi sampai commit().
   */
  update<T extends OvnDocument>(
    collection: Collection<T>,
    filter: QueryFilter,
    spec: UpdateSpec,
  ): this {
    this._assertPending();
    this._ops.push({ kind: 'update', collection, filter, spec });
    return this;
  }

  /**
   * Stage sebuah delete. Belum dieksekusi sampai commit().
   */
  delete<T extends OvnDocument>(
    collection: Collection<T>,
    filter: QueryFilter,
  ): this {
    this._assertPending();
    this._ops.push({ kind: 'delete', collection, filter });
    return this;
  }

  // ── Commit ────────────────────────────────────────────────

  /**
   * Eksekusi semua operasi yang di-stage secara atomik.
   * Jika ada satu yang gagal, semua yang sudah ter-apply akan di-rollback.
   *
   * @throws jika commit gagal dan rollback berhasil → error asli
   * @throws jika commit gagal DAN rollback juga gagal → RollbackError
   */
  async commit(): Promise<void> {
    this._assertPending();
    log.debug(`Commit ${this._ops.length} operasi`, { txId: this._id });

    try {
      for (const op of this._ops) {
        await this._applyOp(op);
      }

      // Flush semua collection yang terlibat sekaligus
      const seen = new Set<Collection<OvnDocument>>();
      for (const op of this._ops) {
        if (!seen.has(op.collection)) {
          seen.add(op.collection);
          await op.collection.flush();
        }
      }

      this._status = 'committed';
      log.debug(`Commit berhasil`, { txId: this._id, ops: this._ops.length });

    } catch (commitErr) {
      log.error(`Commit gagal, melakukan rollback...`, {
        txId: this._id,
        err:  String(commitErr),
      });

      try {
        await this._doRollback();
        this._status = 'rolled_back';
      } catch (rollbackErr) {
        this._status = 'failed';
        // Ini adalah kondisi paling berbahaya — data mungkin inkonsisten
        const msg =
          `[OvnDB] CRITICAL: commit gagal DAN rollback gagal!\n` +
          `  Transaction: ${this._id}\n` +
          `  Commit error: ${String(commitErr)}\n` +
          `  Rollback error: ${String(rollbackErr)}\n` +
          `Data mungkin dalam keadaan inkonsisten. Periksa file dan WAL.`;
        log.error(msg);
        throw new RollbackFailedError(msg, commitErr as Error, rollbackErr as Error);
      }

      throw commitErr;
    }
  }

  /**
   * Batalkan semua operasi yang di-stage (sebelum commit).
   * Setelah rollback(), transaction tidak bisa digunakan lagi.
   */
  async rollback(): Promise<void> {
    if (this._status === 'committed') {
      throw new Error(`[OvnDB] Tidak bisa rollback transaction yang sudah committed`);
    }
    if (this._status !== 'pending') {
      log.warn(`rollback() dipanggil pada status "${this._status}"`, { txId: this._id });
      return;
    }

    if (this._applied.length > 0) {
      await this._doRollback();
    }
    this._status = 'rolled_back';
    log.debug(`Transaction di-rollback`, { txId: this._id });
  }

  // ── Internals ─────────────────────────────────────────────

  private async _applyOp(op: TxOp): Promise<void> {
    if (op.kind === 'insert') {
      const inserted = await op.collection.insertOne(op.doc);
      this._applied.push({ kind: 'insert', collection: op.collection, id: inserted._id });

    } else if (op.kind === 'update') {
      // Baca kondisi sebelum update untuk keperluan rollback
      const before = await op.collection.findOne(op.filter);
      if (!before) return; // dokumen tidak ada — skip, bukan error

      await op.collection.updateOne(op.filter, op.spec);
      this._applied.push({ kind: 'update', collection: op.collection, id: before._id, before });

    } else if (op.kind === 'delete') {
      const doc = await op.collection.findOne(op.filter);
      if (!doc) return; // dokumen tidak ada — skip

      await op.collection.deleteOne(op.filter);
      this._applied.push({ kind: 'delete', collection: op.collection, doc });
    }
  }

  /**
   * Membalik semua operasi yang sudah ter-apply, dalam urutan terbalik.
   */
  private async _doRollback(): Promise<void> {
    log.debug(`Rolling back ${this._applied.length} operasi`, { txId: this._id });

    // Balik urutan — LIFO
    for (let i = this._applied.length - 1; i >= 0; i--) {
      const applied = this._applied[i]!;

      if (applied.kind === 'insert') {
        // Hapus dokumen yang tadi di-insert
        await applied.collection.deleteOne({ _id: applied.id });

      } else if (applied.kind === 'update') {
        // Kembalikan ke nilai sebelumnya dengan replace seluruh dokumen
        await applied.collection.replaceOne({ _id: applied.id }, applied.before);

      } else if (applied.kind === 'delete') {
        // Re-insert dokumen yang tadi dihapus, termasuk _id aslinya
        await applied.collection.insertOne(applied.doc as Omit<OvnDocument, '_id'> & { _id: string });
      }
    }

    this._applied.length = 0;
  }

  private _assertPending(): void {
    if (this._status !== 'pending') {
      throw new Error(
        `[OvnDB] Transaction tidak bisa dimodifikasi — status saat ini: "${this._status}"\n` +
        `Buat Transaction baru dengan db.beginTransaction()`
      );
    }
  }
}

// ── Custom error ──────────────────────────────────────────────
export class RollbackFailedError extends Error {
  constructor(
    message: string,
    public readonly commitError: Error,
    public readonly rollbackError: Error,
  ) {
    super(message);
    this.name = 'RollbackFailedError';
  }
}
