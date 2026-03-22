import { WalOp } from '../../types/constants.js';
export interface WalEntry {
    seqno: bigint;
    op: WalOp;
    key: string;
    data: Buffer;
    txId: bigint;
}
export declare class WAL {
    private readonly dirPath;
    private readonly colName;
    private filePath;
    private fd;
    private seqno;
    private writePos;
    private _pendingCount;
    private group;
    private groupTimer;
    private _committing;
    constructor(dirPath: string, collectionName: string);
    open(): Promise<WalEntry[]>;
    close(): Promise<void>;
    private static readonly WAL_KEY_MAX_LEN;
    private static readonly WAL_DATA_MAX_LEN;
    append(op: WalOp, key: string, data?: Buffer, txId?: bigint): Promise<void>;
    checkpoint(): Promise<void>;
    get pending(): number;
    private _doGroupCommit;
    /**
     * G5: Rotasi WAL — crash-safe dengan atomic rename.
     * Urutan: buat .new → sync → tutup lama → rename lama→.bak → rename .new→aktif
     * Jika crash di langkah manapun, data tidak hilang:
     *   - Crash sebelum rename: .new dibuang saat restart, .wal lama masih valid
     *   - Crash setelah rename: .wal baru ada dan valid
     */
    private _rotateWal;
    /**
     * G6: Replay dengan TX_ABORT awareness.
     * Operasi dari transaksi yang di-abort TIDAK di-replay.
     *
     * SECURITY: replay juga memvalidasi keyLen dan dataLen sebelum alokasi
     * untuk mencegah corrupt WAL menyebabkan giant buffer allocation / OOM.
     */
    private _replay;
}
//# sourceMappingURL=wal.d.ts.map