/**
 * Generate ID unik sortable (24 hex chars).
 * Monotonically non-decreasing — ID yang lebih baru >= ID yang lebih lama.
 */
export declare function generateId(): string;
/** Ekstrak timestamp (epoch ms) dari ID yang di-generate oleh generateId(). */
export declare function idToTimestamp(id: string): number;
/** Validasi apakah string adalah ID yang valid (24 hex chars). */
export declare function isValidId(id: string): boolean;
//# sourceMappingURL=id-generator.d.ts.map