export declare class FTSIndex {
    /** inverted index: token → Set of doc _id */
    private readonly posting;
    /** forward index: _id → Set of tokens (for remove) */
    private readonly forward;
    private dirty;
    private readonly filePath;
    constructor(dirPath: string, collection: string, field: string);
    open(): Promise<void>;
    save(): Promise<void>;
    /**
     * Index a document's field value.
     * @param id    doc._id
     * @param text  field value (string)
     */
    index(id: string, text: string): void;
    /**
     * Remove a document from the index.
     * @param id doc._id
     */
    remove(id: string): void;
    /**
     * Search for documents matching ALL words in the query.
     * Returns array of matching _ids (intersection of posting lists).
     * @param query  Space-separated search terms
     */
    search(query: string): string[];
    /**
     * Check if this index has any data for the given doc id
     */
    hasDoc(id: string): boolean;
    get tokenCount(): number;
    private _tokenize;
    private _removeById;
}
//# sourceMappingURL=fts-index.d.ts.map