import { PageType } from '../../types/constants.js';
export declare const PAGE_HEADER_SIZE = 24;
export declare const PAGE_DATA_SIZE: number;
export interface PageHeader {
    pageId: number;
    pageType: PageType;
    keyCount: number;
    nextPage: number;
    prevPage: number;
    flags: number;
}
export interface Page {
    header: PageHeader;
    data: Buffer;
    dirty: boolean;
}
export declare class PageManager {
    private readonly filePath;
    private fd;
    private pageCount;
    private rootPageId;
    private readonly pool;
    private readonly _dirtySet;
    constructor(filePath: string, poolSize?: number);
    open(): Promise<void>;
    close(): Promise<void>;
    get rootPage(): number;
    set rootPage(id: number);
    get totalPages(): number;
    readPage(pageId: number): Promise<Page>;
    allocPage(type: PageType): Promise<{
        pageId: number;
        page: Page;
    }>;
    markDirty(pageId: number): void;
    flushDirty(): Promise<void>;
    /**
     * G7: reset seluruh page file ke state awal.
     * Dipakai oleh B+ Tree.clear() untuk deleteAll() O(1).
     */
    reset(): Promise<void>;
    private _addToPool;
    /**
     * G9: Dirty-aware eviction.
     * Evict clean pages dulu — tidak perlu disk write.
     * Evict dirty pages hanya jika tidak ada clean page tersedia.
     */
    private _evictBatch;
    private _readPageFromDisk;
    private _writePageToDisk;
    private _initFile;
    private _writeFileHeader;
    private _readFileHeader;
}
//# sourceMappingURL=page-manager.d.ts.map