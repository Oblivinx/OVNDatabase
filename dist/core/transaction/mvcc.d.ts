import type { TxSnapshot } from '../../types/index.js';
export declare class MVCCManager {
    /** Monotonically increasing transaction ID counter. */
    private _nextTxId;
    /** Set semua txId yang sudah committed. Digunakan untuk snapshot visibility check. */
    private readonly _committed;
    /** Monotonically increasing commit sequence counter — tracks actual commit order. */
    private _commitSeq;
    /** Map txId → commit sequence number */
    private readonly _commitSeqMap;
    /** Map txId → waktu commit (untuk garbage collection committed set). */
    private readonly _commitTime;
    /** Active (belum commit) transactions. */
    private readonly _active;
    /** Write intent log: txId → Set<key yang di-write oleh txId ini>. */
    private readonly _writeIntents;
    /**
     * Mulai transaksi baru. Kembalikan snapshot yang akan digunakan
     * untuk menentukan record mana yang "visible" bagi transaksi ini.
     *
     * Snapshot diambil saat begin, bukan saat execute operasi pertama.
     * Ini adalah Snapshot Isolation — transaksi selalu melihat snapshot
     * yang konsisten dari titik ia dimulai.
     */
    begin(): TxSnapshot;
    /**
     * Commit transaksi. Periksa write conflict terlebih dahulu.
     * @throws WriteConflictError jika ada key yang konflik dengan tx lain
     */
    commit(txId: bigint): void;
    /**
     * Abort (rollback) transaksi. Tidak perlu undo ke disk —
     * cukup hapus dari active map. Writer lain tidak pernah melihat
     * data dari tx yang di-abort (karena txId tidak masuk committed set).
     */
    abort(txId: bigint): void;
    /**
     * Apakah record dengan txId `recordTxId` terlihat oleh snapshot `snap`?
     *
     * Sebuah record visible jika txId-nya ada di snapshot.visibleTxIds
     * (artinya sudah committed sebelum snapshot diambil) ATAU txId-nya
     * adalah txId milik transaksi itu sendiri (bisa lihat write sendiri).
     */
    isVisible(recordTxId: bigint, snap: TxSnapshot): boolean;
    /**
     * Versi sederhana untuk read tanpa transaksi eksplisit:
     * hanya tampilkan record yang sudah committed.
     */
    isCommitted(recordTxId: bigint): boolean;
    /** Catat bahwa txId menulis ke key tertentu (untuk conflict detection). */
    recordWrite(txId: bigint, key: string): void;
    /** Kembalikan txId baru untuk write tanpa explicit transaction (auto-commit). */
    autoCommitTxId(): bigint;
    get nextTxId(): bigint;
    /**
     * Bersihkan committed set dari txId lama yang tidak diperlukan lagi.
     * Sebuah txId tidak diperlukan jika tidak ada active transaction
     * yang menggunakan snapshot dari sebelum txId itu committed.
     *
     * Dipanggil setiap 1000 commit.
     */
    private _maybeGCCommitted;
}
export declare class WriteConflictError extends Error {
    constructor(message: string);
}
//# sourceMappingURL=mvcc.d.ts.map