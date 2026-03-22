import type { Collection } from '../../collection/collection.js';
import type { OvnDocument, QueryFilter, UpdateSpec, TxStatus } from '../../types/index.js';
import { WriteConflictError } from './mvcc.js';
export declare class Transaction {
    private readonly _ops;
    private readonly _applied;
    /**
     * v4.0: Savepoints — Map of name → index into _applied at time of savepoint.
     * rollbackTo(name) rolls back only ops applied AFTER that index.
     */
    private readonly _savepoints;
    private _status;
    readonly id: string;
    constructor();
    get status(): TxStatus;
    get opCount(): number;
    insert<T extends OvnDocument>(col: Collection<T>, doc: Omit<T, '_id'> & {
        _id?: string;
    }): this;
    update<T extends OvnDocument>(col: Collection<T>, filter: QueryFilter, spec: UpdateSpec): this;
    delete<T extends OvnDocument>(col: Collection<T>, filter: QueryFilter): this;
    /** feat: upsert operation — insert jika tidak ada, update jika ada */
    upsert<T extends OvnDocument>(col: Collection<T>, filter: QueryFilter, spec: UpdateSpec): this;
    /** feat: replace operation — ganti seluruh dokumen */
    replace<T extends OvnDocument>(col: Collection<T>, filter: QueryFilter, replacement: T): this;
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
    savepoint(name: string): void;
    /**
     * Rollback ke savepoint — undo semua operasi yang diapply setelah savepoint.
     * Status transaksi tetap 'pending', bisa lanjut commit sisa operasi.
     *
     * @example
     *   await tx.rollbackTo('after-insert');
     *   // tx masih pending, bisa commit atau tambah ops baru
     */
    rollbackTo(name: string): Promise<void>;
    /**
     * Eksekusi semua operasi yang di-stage secara atomik.
     */
    commit(): Promise<void>;
    rollback(): Promise<void>;
    private _applyOp;
    private _undoApplied;
    private _doRollback;
    private _assertPending;
}
export declare class RollbackFailedError extends Error {
    readonly commitError: Error;
    readonly rollbackError: Error;
    constructor(message: string, commitError: Error, rollbackError: Error);
}
export { WriteConflictError };
//# sourceMappingURL=transaction.d.ts.map