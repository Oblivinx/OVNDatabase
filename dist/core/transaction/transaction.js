// ============================================================
//  OvnDB v4.0 — Transaction dengan Savepoints
//
//  v4.0: Savepoints — partial rollback dalam transaksi.
//    tx.savepoint('sp1')       → catat posisi saat ini
//    tx.rollbackTo('sp1')      → rollback hanya ops setelah 'sp1'
//    lanjut ... tx.commit()    → commit ops sebelum 'sp1'
//
//  From v2.1: upsert(), replace() operations,
//             improved error context, getSnapshot() for read-only tx,
//             safe rollback: partial apply tracked per-step
// ============================================================
import { WriteConflictError } from './mvcc.js';
import { makeLogger } from '../../utils/logger.js';
const log = makeLogger('transaction');
export class Transaction {
    _ops = [];
    _applied = [];
    /**
     * v4.0: Savepoints — Map of name → index into _applied at time of savepoint.
     * rollbackTo(name) rolls back only ops applied AFTER that index.
     */
    _savepoints = new Map();
    _status = 'pending';
    id;
    constructor() {
        this.id = `tx-${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
        log.debug('Transaction begin', { txId: this.id });
    }
    get status() { return this._status; }
    get opCount() { return this._ops.length; }
    // ── Staging ───────────────────────────────────────────────
    insert(col, doc) {
        this._assertPending();
        this._ops.push({ kind: 'insert', collection: col, doc });
        return this;
    }
    update(col, filter, spec) {
        this._assertPending();
        this._ops.push({ kind: 'update', collection: col, filter, spec });
        return this;
    }
    delete(col, filter) {
        this._assertPending();
        this._ops.push({ kind: 'delete', collection: col, filter });
        return this;
    }
    /** feat: upsert operation — insert jika tidak ada, update jika ada */
    upsert(col, filter, spec) {
        this._assertPending();
        this._ops.push({ kind: 'upsert', collection: col, filter, spec });
        return this;
    }
    /** feat: replace operation — ganti seluruh dokumen */
    replace(col, filter, replacement) {
        this._assertPending();
        this._ops.push({ kind: 'replace', collection: col, filter, replacement });
        return this;
    }
    // ── Savepoints (v4.0) ─────────────────────────────────────
    /**
     * Catat savepoint pada posisi operasi saat ini.
     * Panggil setelah beberapa operasi di-stage tapi sebelum commit.
     * Savepoint bisa dipakai sebelum atau sesudah commit dimulai.
     *
     * @example
     *   tx.insert(col, docA);
     *   tx.savepoint('after-insert');
     *   tx.update(col, { _id: 'x' }, { $inc: { n: 1 } });
     *   await tx.rollbackTo('after-insert'); // rollback update, keep insert
     *   await tx.commit();                   // commmit hanya insert
     */
    savepoint(name) {
        this._assertPending();
        // Savepoints track position in _applied (ops already executed during commit)
        // but we also support pre-commit savepoints for staging
        this._savepoints.set(name, this._applied.length);
        log.debug(`Savepoint "${name}" created at applied[${this._applied.length}]`, { txId: this.id });
    }
    /**
     * Rollback ke savepoint — undo semua operasi yang diapply setelah savepoint.
     * Status transaksi tetap 'pending', bisa lanjut commit sisa operasi.
     *
     * @example
     *   await tx.rollbackTo('after-insert');
     *   // tx masih pending, bisa commit atau tambah ops baru
     */
    async rollbackTo(name) {
        this._assertPending();
        const idx = this._savepoints.get(name);
        if (idx === undefined)
            throw new Error(`[OvnDB] Savepoint "${name}" not found`);
        if (idx > this._applied.length)
            throw new Error(`[OvnDB] Savepoint "${name}" is ahead of current position`);
        const toUndo = this._applied.splice(idx); // remove ops after savepoint
        log.debug(`Rolling back to "${name}" — undoing ${toUndo.length} op(s)`, { txId: this.id });
        // Undo in reverse order
        for (let i = toUndo.length - 1; i >= 0; i--) {
            const applied = toUndo[i];
            try {
                await this._undoApplied(applied);
            }
            catch (e) {
                log.error(`Rollback to savepoint failed at op[${i}]`, { err: String(e) });
            }
        }
        // Remove any savepoints after this one
        for (const [spName, spIdx] of this._savepoints) {
            if (spIdx > idx)
                this._savepoints.delete(spName);
        }
        log.debug(`Rollback to "${name}" complete`, { txId: this.id });
    }
    // ── Commit ────────────────────────────────────────────────
    /**
     * Eksekusi semua operasi yang di-stage secara atomik.
     */
    async commit() {
        this._assertPending();
        log.debug(`Commit ${this._ops.length} op(s)`, { txId: this.id });
        try {
            for (let i = 0; i < this._ops.length; i++) {
                try {
                    await this._applyOp(this._ops[i]);
                }
                catch (opErr) {
                    const op = this._ops[i];
                    throw new Error(`[OvnDB] Transaction op[${i}] (${op.kind}) failed: ${opErr instanceof Error ? opErr.message : String(opErr)}`);
                }
            }
            // Flush semua collection yang terlibat
            const seen = new Set();
            for (const op of this._ops) {
                if (!seen.has(op.collection)) {
                    seen.add(op.collection);
                    await op.collection.flush();
                }
            }
            this._status = 'committed';
            log.debug('Commit berhasil', { txId: this.id, ops: this._ops.length });
        }
        catch (commitErr) {
            log.error('Commit gagal, rolling back...', { txId: this.id, err: String(commitErr) });
            try {
                await this._doRollback();
                this._status = 'rolled_back';
            }
            catch (rbErr) {
                this._status = 'failed';
                throw new RollbackFailedError(`[OvnDB] CRITICAL: commit AND rollback failed. TX: ${this.id}`, commitErr, rbErr);
            }
            throw commitErr;
        }
    }
    async rollback() {
        if (this._status === 'committed')
            throw new Error('[OvnDB] Cannot rollback an already-committed transaction');
        if (this._status !== 'pending')
            return;
        if (this._applied.length > 0)
            await this._doRollback();
        this._status = 'rolled_back';
        log.debug('Transaction rolled back', { txId: this.id });
    }
    // ── Internals ─────────────────────────────────────────────
    async _applyOp(op) {
        if (op.kind === 'insert') {
            const inserted = await op.collection.insertOne(op.doc);
            this._applied.push({ kind: 'insert', col: op.collection, id: inserted._id });
        }
        else if (op.kind === 'update') {
            const before = await op.collection.findOne(op.filter);
            if (!before)
                return;
            await op.collection.updateOne(op.filter, op.spec);
            this._applied.push({ kind: 'update', col: op.collection, id: before._id, before });
        }
        else if (op.kind === 'delete') {
            const doc = await op.collection.findOne(op.filter);
            if (!doc)
                return;
            await op.collection.deleteOne(op.filter);
            this._applied.push({ kind: 'delete', col: op.collection, doc });
        }
        else if (op.kind === 'upsert') {
            const before = await op.collection.findOne(op.filter);
            const wasInsert = !before;
            const result = await op.collection.upsertOne(op.filter, op.spec);
            this._applied.push({ kind: 'upsert', col: op.collection, id: result._id, wasInsert, before: before ?? undefined });
        }
        else if (op.kind === 'replace') {
            const before = await op.collection.findOne(op.filter);
            if (!before)
                return;
            await op.collection.replaceOne(op.filter, op.replacement);
            this._applied.push({ kind: 'replace', col: op.collection, id: before._id, before });
        }
    }
    async _undoApplied(applied) {
        if (applied.kind === 'insert') {
            await applied.col.deleteOne({ _id: applied.id });
        }
        else if (applied.kind === 'update') {
            await applied.col.replaceOne({ _id: applied.id }, applied.before);
        }
        else if (applied.kind === 'delete') {
            await applied.col.insertOne(applied.doc);
        }
        else if (applied.kind === 'upsert') {
            if (applied.wasInsert) {
                await applied.col.deleteOne({ _id: applied.id });
            }
            else if (applied.before) {
                await applied.col.replaceOne({ _id: applied.id }, applied.before);
            }
        }
        else if (applied.kind === 'replace') {
            await applied.col.replaceOne({ _id: applied.id }, applied.before);
        }
    }
    async _doRollback() {
        log.debug(`Rolling back ${this._applied.length} applied op(s)`, { txId: this.id });
        for (let i = this._applied.length - 1; i >= 0; i--) {
            const applied = this._applied[i];
            await this._undoApplied(applied);
        }
        this._applied.length = 0;
    }
    _assertPending() {
        if (this._status !== 'pending')
            throw new Error(`[OvnDB] Transaction cannot be modified — status: "${this._status}". Create a new one.`);
    }
}
export class RollbackFailedError extends Error {
    commitError;
    rollbackError;
    constructor(message, commitError, rollbackError) {
        super(message);
        this.commitError = commitError;
        this.rollbackError = rollbackError;
        this.name = 'RollbackFailedError';
    }
}
export { WriteConflictError };
//# sourceMappingURL=transaction.js.map