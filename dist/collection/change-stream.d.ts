import { EventEmitter } from 'events';
import type { OvnDocument, ChangeEvent, QueryFilter } from '../types/index.js';
export interface WatchOptions {
    /** Filter dokumen — hanya emit event untuk dokumen yang cocok. */
    filter?: QueryFilter;
    /** Jika true, emit full document bahkan untuk update (default: true). */
    fullDocument?: boolean;
}
export declare class ChangeStream<T extends OvnDocument = OvnDocument> extends EventEmitter {
    private readonly filter?;
    private readonly fullDoc;
    private _closed;
    constructor(opts?: WatchOptions);
    /**
     * Internal: dipanggil oleh Collection setiap ada perubahan.
     * Tidak untuk dipanggil langsung oleh user.
     */
    _emit(event: ChangeEvent<T>): void;
    /** Tutup stream — tidak akan emit event lagi setelah ini. */
    close(): void;
    get isClosed(): boolean;
    on(event: 'change', listener: (e: ChangeEvent<T>) => void): this;
    on(event: 'insert', listener: (e: ChangeEvent<T>) => void): this;
    on(event: 'update', listener: (e: ChangeEvent<T>) => void): this;
    on(event: 'delete', listener: (e: ChangeEvent<T>) => void): this;
    on(event: 'close', listener: () => void): this;
}
/** Registry semua ChangeStream yang aktif untuk satu collection. */
export declare class ChangeStreamRegistry<T extends OvnDocument = OvnDocument> {
    private readonly streams;
    /** Buat stream baru dan daftarkan ke registry. */
    create(opts?: WatchOptions): ChangeStream<T>;
    /** Emit event ke semua stream yang aktif. */
    emit(event: ChangeEvent<T>): void;
    /** Tutup semua stream (saat collection di-drop atau DB ditutup). */
    closeAll(): void;
    get count(): number;
}
//# sourceMappingURL=change-stream.d.ts.map