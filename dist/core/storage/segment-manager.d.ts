import type { RecordPointer } from '../../types/index.js';
export declare class SegmentManager {
    private readonly dirPath;
    private readonly collection;
    private manifest;
    private fds;
    private _closed;
    private readonly bloomFilters;
    compressFn?: (buf: Buffer) => Buffer;
    decompressFn?: (buf: Buffer) => Buffer;
    /**
     * SECURITY: integrityKey untuk HMAC-SHA256 manifest verification.
     * Jika di-set, manifest checksum dihitung sebagai HMAC bukan plain SHA-256.
     * HMAC mendeteksi modifikasi disengaja oleh pihak yang bisa menulis ke
     * data dir tapi tidak mengetahui key. Plain SHA-256 hanya mendeteksi
     * korupsi acak (flipbit, partial write, dll).
     *
     * Untuk database terenkripsi: set ini ke Buffer 32-byte yang di-derive
     * dari passphrase yang sama (gunakan HKDF atau sub-key dari CryptoLayer).
     * Contoh di CollectionV2 / OvnDB.open():
     *   engine.segments.integrityKey = crypto.hkdfSync(
     *     'sha256', masterKey, salt, Buffer.from('ovndb-manifest-hmac'), 32
     *   );
     */
    integrityKey?: Buffer;
    constructor(dirPath: string, collection: string);
    /**
     * Hitung checksum manifest: HMAC-SHA256 jika integrityKey tersedia,
     * SHA-256 plain jika tidak. Kedua mode di-tag dengan prefix agar
     * open() bisa membedakan format lama (sha256:) dari baru (hmac:).
     */
    private _computeManifestChecksum;
    /**
     * Verifikasi checksum dari manifest yang sudah dimuat.
     * Menangani: format lama (hex string saja), sha256: prefix, hmac: prefix.
     * Gagal-tertutup: jika format tidak dikenal → throw.
     */
    private _verifyManifestChecksum;
    open(): Promise<void>;
    close(): Promise<void>;
    writeRecord(data: Buffer, txId: bigint): RecordPointer;
    deleteRecord(ptr: RecordPointer): void;
    readRecord(ptr: RecordPointer): Buffer | null;
    /**
     * G16: scanAll dengan support partial scan (cursor pagination).
     * @param opts.fromSegment  Mulai dari segment ID ini (inklusif)
     * @param opts.fromOffset   Mulai dari offset ini (hanya berlaku pada fromSegment)
     */
    scanAll(decryptFn?: (b: Buffer) => Buffer, opts?: {
        fromSegment?: number;
        fromOffset?: number;
    }): AsyncIterableIterator<{
        ptr: RecordPointer;
        data: Buffer;
    }>;
    _scanSegment(segId: number, decryptFn?: (b: Buffer) => Buffer, startOffset?: number): AsyncIterableIterator<{
        ptr: RecordPointer;
        data: Buffer;
    }>;
    /**
     * Tandai semua record sebagai DELETED — versi sync (untuk collection kecil).
     * Dipakai oleh StorageEngine.deleteAll().
     *
     * PERINGATAN: Untuk collection besar (>50K records), gunakan markAllDeletedAsync()
     * agar event loop tidak ter-freeze selama proses berlangsung.
     */
    markAllDeleted(): void;
    /**
     * Versi async dari markAllDeleted() — tidak memblokir event loop.
     * Yield ke event loop setiap 1000 records dan antar segment.
     * Gunakan ini untuk collection besar (>50K dokumen) via deleteAll().
     */
    markAllDeletedAsync(): Promise<void>;
    autoCompact(onPointerMoved: (oldPtr: RecordPointer, newPtr: RecordPointer) => void): Promise<number[]>;
    private _compactSegment;
    get totalLive(): bigint;
    get totalDead(): bigint;
    get segmentCount(): number;
    get totalFileSize(): number;
    get fragmentRatio(): number;
    saveManifest(): Promise<void>;
    fdatasyncActive(): void;
    /**
     * v4.0: Bloom filter fast-miss check.
     * Cek apakah id MUNGKIN ada di SALAH SATU segment.
     * false = pasti tidak ada → skip B+ Tree lookup seluruhnya.
     * true  = mungkin ada → lanjutkan ke B+ Tree.
     */
    mightContain(id: string): boolean;
    private _buildRecord;
    private _activeSegment;
    private _seg;
    private _fd;
    private _segPath;
    private _manifestPath;
    private _openSegment;
    private _createNewSegment;
    private _createNewSegmentSync;
    /**
     * G17 + SECURITY HARDENING: Build manifest JSON dengan HMAC-SHA256 (se integrityKey
     * di-set) atau SHA-256 plain (backward-compat). Checksum prefixed agar
     * open() bisa membedakan format.
     */
    private _buildManifestContent;
    private _saveManifest;
    /**
     * v4.0: Build bloom filters from all segments by scanning their records.
     * Called once during open(). O(n) scan per segment but only run at startup.
     */
    private _buildBloomFilters;
}
//# sourceMappingURL=segment-manager.d.ts.map