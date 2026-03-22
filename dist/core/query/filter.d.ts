import type { QueryFilter, UpdateSpec } from '../../types/index.js';
/**
 * Periksa apakah dokumen `doc` cocok dengan filter `filter`.
 * @returns true jika cocok, false jika tidak
 */
export declare function matchFilter(doc: Record<string, unknown>, filter: QueryFilter): boolean;
/** Ambil nilai field dari dokumen, support dot notation (a.b.c). */
export declare function getFieldValue(doc: Record<string, unknown>, path: string): unknown;
/**
 * Terapkan UpdateSpec ke dokumen dan kembalikan dokumen baru.
 * Tidak memodifikasi dokumen asli (immutable).
 */
export declare function applyUpdate(doc: Record<string, unknown>, spec: UpdateSpec): Record<string, unknown>;
export declare function getNestedField(obj: Record<string, unknown>, path: string): unknown;
export declare function setNestedField(obj: Record<string, unknown>, path: string, val: unknown): void;
export declare function deleteNestedField(obj: Record<string, unknown>, path: string): void;
/**
 * Terapkan projection ke dokumen.
 * Aturan: 1 = include, 0 = exclude.
 * _id selalu include kecuali secara eksplisit di-exclude.
 */
export declare function applyProjection(doc: Record<string, unknown>, projection?: Record<string, 0 | 1>): Record<string, unknown>;
//# sourceMappingURL=filter.d.ts.map