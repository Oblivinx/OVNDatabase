import type { IndexDefinition } from '../../types/index.js';
export declare class SecondaryIndexManager {
    private readonly indexes;
    private readonly dirPath;
    private readonly collection;
    constructor(dirPath: string, collection: string);
    addIndex(def: IndexDefinition): void;
    open(): Promise<void>;
    save(): Promise<void>;
    /**
     * Dipanggil setelah insertOne. Two-phase: validasi dulu, apply kemudian.
     * Ini memastikan partial failure tidak mengakibatkan index korup.
     */
    onInsert(doc: Record<string, unknown>): void;
    onUpdate(oldDoc: Record<string, unknown>, newDoc: Record<string, unknown>): void;
    onDelete(doc: Record<string, unknown>): void;
    /**
     * Exact match lookup for single or compound field.
     * - singleField: lookup('email', 'x@y.com')
     * - compoundField: lookup(['city', 'role'], 'Jakarta\x00admin')
     * null = field tidak di-index.
     */
    lookup(field: string | string[], value: unknown): string[] | null;
    /**
     * Range lookup untuk single-field index.
     * Compound index tidak mendukung range (hanya exact match).
     */
    lookupRange(field: string, gte?: string, lte?: string): string[];
    /**
     * v4.0: Compound lookup — menerima object dengan semua field compound
     * Contoh: lookupCompound(['city', 'role'], { city: 'Jakarta', role: 'admin' })
     */
    lookupCompound(fields: string[], values: Record<string, unknown>): string[] | null;
    hasIndex(field: string | string[]): boolean;
    /** Kembalikan semua definisi index yang terdaftar */
    getIndexDefs(): IndexDefinition[];
    /** feat: clearAll — kosongkan semua data di semua index (untuk truncate). */
    clearAll(): void;
    /** Rebuild semua index dari kumpulan dokumen (misal setelah compaction). */
    rebuildFromDocs(docs: Iterable<Record<string, unknown>>): void;
    /** Buat canonical key untuk Map lookup dari field (string atau string[]) */
    private _indexKey;
    /**
     * Extract composite key dari dokumen.
     * - Single field: return nilai field sebagai string (atau null jika undefined/null)
     * - Compound field[]: return "val1\x00val2\x00..." (null jika SEMUA field null/undefined)
     */
    private _getCompositeKey;
    private _addToIndex;
    private _removeFromIndex;
    private _docMatchesPartial;
    private _getField;
    private _idxPath;
    private _serialize;
    private _deserialize;
}
//# sourceMappingURL=secondary-index.d.ts.map