export declare class BloomFilter {
    private readonly bits;
    private readonly k;
    private readonly m;
    /**
     * @param capacity    Expected jumlah keys (digunakan untuk size bit array optimal)
     * @param fpr         Target false positive rate (default 0.01 = 1%)
     */
    constructor(capacity: number, fpr?: number);
    /** Buat BloomFilter dari raw buffer (deserialisasi) */
    static fromBuffer(buf: Buffer<ArrayBuffer>, k: number): BloomFilter;
    /** Serialize ke Buffer untuk penyimpanan */
    toBuffer(): Buffer;
    get hashCount(): number;
    get bitCount(): number;
    add(key: string): void;
    /**
     * Test apakah key MUNGKIN ada.
     * false = pasti tidak ada (tidak perlu disk lookup)
     * true  = mungkin ada (lanjutkan B+ Tree lookup)
     */
    test(key: string): boolean;
    /** Estimasi jumlah elemen (approx) */
    get approximateCount(): number;
    /**
     * FNV-1a inspired hash with seed for multiple independent hashes.
     * Tidak pakai crypto untuk performa — bloom filter OK dengan non-crypto hash.
     */
    private _hash;
}
//# sourceMappingURL=bloom-filter.d.ts.map