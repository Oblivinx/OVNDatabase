import type { Collection } from '../collection/collection.js';
import type { OvnDocument } from '../types/index.js';
export type RelationMap = Record<string, string>;
export declare class RelationManager {
    private readonly _cols;
    /**
     * Daftarkan collection agar bisa di-resolve lewat nama.
     */
    register<T extends OvnDocument>(name: string, col: Collection<T>): this;
    /**
     * Resolve foreign keys dari satu dokumen.
     *
     * @param doc        Dokumen sumber
     * @param relations  Map field → nama collection
     * @returns Dokumen baru dengan field foreign key diganti objek resolved
     *
     * @example
     *   const full = await rel.populate(msg, { userId: 'users', groupId: 'groups' });
     */
    populate<T extends OvnDocument>(doc: T, relations: RelationMap): Promise<T & Record<string, unknown>>;
    /**
     * Resolve foreign keys dari banyak dokumen secara efisien.
     * ID yang sama hanya di-fetch satu kali (deduplication).
     *
     * @param docs       Array dokumen sumber
     * @param relations  Map field → nama collection
     * @returns Array dokumen dengan field foreign key di-populate
     *
     * @example
     *   const populated = await rel.populateMany(messages, { userId: 'users' });
     */
    populateMany<T extends OvnDocument>(docs: T[], relations: RelationMap): Promise<Array<T & Record<string, unknown>>>;
    /** Hapus semua registrasi collection. */
    clear(): void;
    get registeredCollections(): string[];
    private _col;
}
//# sourceMappingURL=relation-manager.d.ts.map