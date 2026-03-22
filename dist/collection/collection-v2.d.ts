import { Collection } from './collection.js';
import type { CryptoLayer } from '../crypto/crypto-layer.js';
import type { StorageEngine } from '../core/storage/storage-engine.js';
import type { IndexDefinition, OvnDocument } from '../types/index.js';
export interface CollectionV2Options {
    crypto?: CryptoLayer;
    indexes?: IndexDefinition[];
}
export interface KeyRotationResult {
    /** Dokumen yang berhasil di-re-encrypt */
    rotated: number;
    /** Dokumen yang gagal (data tetap dengan key lama) */
    failed: number;
    /** Durasi dalam ms */
    elapsedMs: number;
}
export declare class CollectionV2<T extends OvnDocument = OvnDocument> extends Collection<T> {
    private _crypto?;
    readonly isEncrypted: boolean;
    constructor(name: string, engine: StorageEngine, opts?: CollectionV2Options);
    init(opts?: CollectionV2Options): Promise<void>;
    protected _serialize(doc: T): Buffer;
    protected _parse(buf: Buffer): T | null;
    /**
     * G14: Re-encrypt semua record dengan key baru.
     * Proses berlangsung tanpa downtime — collection tetap bisa dibaca/ditulis.
     *
     * Setelah selesai, decryptFn dan _crypto diperbarui ke key baru sehingga
     * semua operasi berikutnya langsung menggunakan key baru.
     *
     * @param newCrypto  CryptoLayer dengan key baru
     * @param batchSize  Dokumen per flush (default 500)
     *
     * @example
     *   const oldCrypto = await cryptoFromPassphrase(oldPass, dir, 0);
     *   const newCrypto = await cryptoFromPassphrase(newPass, dir, 1);
     *   const col = await db.collectionV2('secrets', { crypto: oldCrypto });
     *   const result = await col.rotateEncryptionKey(newCrypto);
     *   console.log(`Rotated: ${result.rotated}, Failed: ${result.failed}`);
     */
    rotateEncryptionKey(newCrypto: CryptoLayer, batchSize?: number): Promise<KeyRotationResult>;
    private _flushRotationBatch;
}
//# sourceMappingURL=collection-v2.d.ts.map