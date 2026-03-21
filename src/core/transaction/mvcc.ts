// ============================================================
//  OvnDB v2.0 — MVCC (Multi-Version Concurrency Control)
//
//  MENGAPA MVCC LEBIH BAIK DARI COMPENSATING WRITES (v1):
//
//   v1 Transaction: rollback dengan "undo" operasi satu per satu
//    → Masalah: antara commit gagal dan rollback selesai, data bisa
//      terlihat setengah-jalan oleh reader lain
//    → Masalah: rollback bisa gagal juga → RollbackFailedError
//
//   v2 MVCC:
//    → Setiap write diberi txId (monotonically increasing BigInt)
//    → Setiap read melihat snapshot: hanya versi dengan txId yang
//      sudah committed SEBELUM snapshot diambil
//    → Rollback: cukup tandai txId sebagai ABORTED — tidak perlu
//      undo write ke disk
//    → Reader tidak pernah melihat data dari transaksi yang belum commit
//
//  Isolation level: Snapshot Isolation
//   - Reader tidak diblokir oleh writer
//   - Writer tidak diblokir oleh reader  
//   - Write conflict: dua transaksi menulis key yang sama → yang kedua
//     akan mendapat WriteConflictError saat commit
//
//  Implementasi disederhanakan untuk embedded mode:
//   - txId counter: AtomicBigInt (karena JS single-threaded, ini cukup)
//   - Committed set: Set<bigint> dari semua txId yang sudah commit
//   - Active transactions: Map<txId, TxSnapshot>
// ============================================================

import type { TxSnapshot } from '../../types/index.js';
import { makeLogger } from '../../utils/logger.js';

const log = makeLogger('mvcc');

export class MVCCManager {
  /** Monotonically increasing transaction ID counter. */
  private _nextTxId: bigint = 1n;

  /** Set semua txId yang sudah committed. Digunakan untuk snapshot visibility check. */
  private readonly _committed: Set<bigint> = new Set([0n]); // 0n = system transactions

  /** Monotonically increasing commit sequence counter — tracks actual commit order. */
  private _commitSeq: bigint = 0n;
  /** Map txId → commit sequence number */
  private readonly _commitSeqMap: Map<bigint, bigint> = new Map();
  /** Map txId → waktu commit (untuk garbage collection committed set). */
  private readonly _commitTime: Map<bigint, number> = new Map();

  /** Active (belum commit) transactions. */
  private readonly _active: Map<bigint, TxSnapshot> = new Map();

  /** Write intent log: txId → Set<key yang di-write oleh txId ini>. */
  private readonly _writeIntents: Map<bigint, Set<string>> = new Map();

  // ── Snapshot ──────────────────────────────────────────────

  /**
   * Mulai transaksi baru. Kembalikan snapshot yang akan digunakan
   * untuk menentukan record mana yang "visible" bagi transaksi ini.
   *
   * Snapshot diambil saat begin, bukan saat execute operasi pertama.
   * Ini adalah Snapshot Isolation — transaksi selalu melihat snapshot
   * yang konsisten dari titik ia dimulai.
   */
  begin(): TxSnapshot {
    const txId = this._nextTxId++;
    const snapshot: TxSnapshot = {
      txId,
      startTime:    Date.now(),
      // Salin committed set saat ini — inilah yang akan visible bagi tx ini
      visibleTxIds: new Set(this._committed),
    };
    this._active.set(txId, snapshot);
    this._writeIntents.set(txId, new Set());
    log.debug(`TX begin`, { txId: String(txId) });
    return snapshot;
  }

  /**
   * Commit transaksi. Periksa write conflict terlebih dahulu.
   * @throws WriteConflictError jika ada key yang konflik dengan tx lain
   */
  commit(txId: bigint): void {
    const snapshot = this._active.get(txId);
    if (!snapshot) throw new Error(`[MVCC] Unknown transaction: ${txId}`);

    const intents = this._writeIntents.get(txId) ?? new Set<string>();

    // Periksa write conflict:
    // Conflict terjadi jika ada tx lain yang:
    //  1. Sudah committed (ada di _committed), DAN
    //  2. Commit SETELAH snapshot kita (tidak ada di visibleTxIds), DAN
    //  3. Menulis key yang sama dengan kita
    for (const otherTxId of this._committed) {
      if (otherTxId === txId) continue; // jangan cek diri sendiri
      // Jika otherTxId ada di visibleTxIds, berarti sudah committed sebelum
      // snapshot kita → tidak conflict (kita sudah tahu tentang tulisannya)
      if (snapshot.visibleTxIds.has(otherTxId)) continue;
      // otherTxId commit setelah snapshot kita → cek overlap key
      const otherIntents = this._writeIntents.get(otherTxId);
      if (otherIntents) {
        for (const key of intents) {
          if (otherIntents.has(key)) {
            this._active.delete(txId);
            this._writeIntents.delete(txId);
            throw new WriteConflictError(
              `[MVCC] Write conflict on key "${key}" between tx ${txId} and ${otherTxId}`
            );
          }
        }
      }
    }

    // Commit berhasil
    const commitSeq = ++this._commitSeq;
    this._committed.add(txId);
    this._commitSeqMap.set(txId, commitSeq);
    this._commitTime.set(txId, Date.now());
    this._active.delete(txId);
    log.debug(`TX committed`, { txId: String(txId), writes: intents.size });
    this._maybeGCCommitted();
  }

  /**
   * Abort (rollback) transaksi. Tidak perlu undo ke disk —
   * cukup hapus dari active map. Writer lain tidak pernah melihat
   * data dari tx yang di-abort (karena txId tidak masuk committed set).
   */
  abort(txId: bigint): void {
    this._active.delete(txId);
    this._writeIntents.delete(txId);
    log.debug(`TX aborted`, { txId: String(txId) });
  }

  // ── Visibility ────────────────────────────────────────────

  /**
   * Apakah record dengan txId `recordTxId` terlihat oleh snapshot `snap`?
   *
   * Sebuah record visible jika txId-nya ada di snapshot.visibleTxIds
   * (artinya sudah committed sebelum snapshot diambil) ATAU txId-nya
   * adalah txId milik transaksi itu sendiri (bisa lihat write sendiri).
   */
  isVisible(recordTxId: bigint, snap: TxSnapshot): boolean {
    if (recordTxId === snap.txId) return true; // bisa lihat write sendiri
    return snap.visibleTxIds.has(recordTxId);
  }

  /**
   * Versi sederhana untuk read tanpa transaksi eksplisit:
   * hanya tampilkan record yang sudah committed.
   */
  isCommitted(recordTxId: bigint): boolean {
    return this._committed.has(recordTxId);
  }

  // ── Write Intent Tracking ─────────────────────────────────

  /** Catat bahwa txId menulis ke key tertentu (untuk conflict detection). */
  recordWrite(txId: bigint, key: string): void {
    this._writeIntents.get(txId)?.add(key);
  }

  /** Kembalikan txId baru untuk write tanpa explicit transaction (auto-commit). */
  autoCommitTxId(): bigint {
    const txId = this._nextTxId++;
    this._committed.add(txId);
    this._commitTime.set(txId, Date.now());
    return txId;
  }

  get nextTxId(): bigint { return this._nextTxId; }

  // ── Garbage Collection ────────────────────────────────────

  /**
   * Bersihkan committed set dari txId lama yang tidak diperlukan lagi.
   * Sebuah txId tidak diperlukan jika tidak ada active transaction
   * yang menggunakan snapshot dari sebelum txId itu committed.
   *
   * Dipanggil setiap 1000 commit.
   */
  private _maybeGCCommitted(): void {
    if (this._committed.size < 1000) return;
    let oldestActive = this._nextTxId;
    for (const snap of this._active.values()) {
      if (snap.txId < oldestActive) oldestActive = snap.txId;
    }
    const cutoff = Date.now() - 60_000;
    let gcCount  = 0;
    for (const txId of this._committed) {
      if (txId < oldestActive && (this._commitTime.get(txId) ?? 0) < cutoff) {
        this._committed.delete(txId);
        this._commitTime.delete(txId);
        this._writeIntents.delete(txId);
        gcCount++;
      }
    }
    if (gcCount > 0) log.debug(`MVCC GC: removed ${gcCount} old txIds`);
  }
}

export class WriteConflictError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'WriteConflictError';
  }
}
