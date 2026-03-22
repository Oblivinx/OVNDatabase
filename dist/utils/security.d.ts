/** Ukuran dokumen maksimum setelah JSON.stringify (16 MB) */
export declare const MAX_DOCUMENT_BYTES: number;
/** Kedalaman nesting filter / update / dokumen maksimum */
export declare const MAX_QUERY_DEPTH = 12;
/** Jumlah key dalam satu level filter / update spec maksimum */
export declare const MAX_FILTER_KEYS = 64;
/** Panjang pattern $regex maksimum (karakter) */
export declare const MAX_REGEX_PATTERN_LEN = 512;
/** Jumlah elemen $in / $nin maksimum */
export declare const MAX_IN_ARRAY_SIZE = 1000;
/** Jumlah stage pipeline aggregation maksimum */
export declare const MAX_PIPELINE_STAGES = 32;
/** Panjang nama collection maksimum */
export declare const MAX_COLLECTION_NAME = 64;
/** Panjang field path (dot-notation) maksimum */
export declare const MAX_FIELD_PATH_LEN = 256;
/** Panjang document _id maksimum */
export declare const MAX_ID_LEN = 128;
/**
 * Periksa apakah satu segmen path bersifat berbahaya.
 * Dipakai sebelum setiap operasi set/get/delete nested field.
 */
export declare function isDangerousKey(key: string): boolean;
/**
 * Validasi field path dot-notation (misal: "user.address.city").
 * Lempar Error jika ada segmen yang berbahaya, path terlalu panjang,
 * atau mengandung karakter tak diizinkan.
 *
 * Masalah: path seperti "__proto__.polluted" memanipulasi Object.prototype
 * karena setNestedField melakukan traversal tanpa memeriksa kunci.
 */
export declare function validateFieldPath(fieldPath: string): void;
export declare function validateCollectionName(name: string): void;
/**
 * Pastikan `target` berada di dalam `base` — cegah path traversal
 * pada operasi backup dan manipulasi file.
 *
 * Masalah: backup("../../sensitive") bisa menulis ke luar data dir.
 */
export declare function assertPathInside(base: string, target: string, label?: string): void;
/**
 * Validasi ukuran dokumen setelah serialisasi.
 * Mencegah single-document yang sangat besar memblokir I/O.
 */
export declare function validateDocumentSize(buf: Buffer): void;
/**
 * Validasi query filter sebelum dieksekusi:
 * 1. Depth maksimum (cegah stack overflow / DoS via deeply nested)
 * 2. Key count per level (cegah filter dengan ratusan kondisi)
 * 3. Prototype pollution via key name
 * 4. $regex length dan keamanan
 * 5. $in / $nin array size
 * 6. $where dilarang (arbitrary code execution)
 *
 * @param filter  Filter yang akan divalidasi
 * @param depth   Kedalaman saat ini (internal rekursi)
 */
export declare function validateQueryFilter(filter: unknown, depth?: number): void;
/**
 * Validasi pattern regex sebelum dikompilasi.
 * Mencegah:
 * 1. Pattern terlalu panjang (ReDoS risk makin besar)
 * 2. Pattern dengan nested quantifier berbahaya (ReDoS)
 * 3. Non-string / non-RegExp value
 *
 * Catatan: deteksi ReDoS 100% tidak mungkin tanpa static analysis penuh,
 * tapi batas panjang + blokir pola paling umum sudah mengurangi risiko
 * signifikan untuk use-case embedded.
 */
export declare function validateRegex(pattern: unknown): void;
/**
 * Validasi UpdateSpec sebelum diaplikasikan.
 * - Semua field key di semua operator divalidasi dari prototype pollution
 * - $push $each dibatasi ukurannya
 */
export declare function validateUpdateSpec(spec: Record<string, unknown>, depth?: number): void;
/**
 * Validasi bahwa dokumen tidak mengandung key berbahaya
 * di level manapun. Dilakukan pada insert dan replace.
 */
export declare function validateDocumentKeys(doc: unknown, depth?: number): void;
/**
 * Validasi _id jika disupply user (bukan generated).
 * Cegah _id yang terlalu panjang atau mengandung null byte.
 */
export declare function validateDocumentId(id: unknown): void;
//# sourceMappingURL=security.d.ts.map