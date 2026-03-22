// ============================================================
//  OvnDB — Security Utilities
//  Terpusat, dipakai oleh semua layer: query, storage, collection.
//
//  Prinsip: fail-closed, whitelist bukan blacklist.
//  Setiap validasi melempar Error dengan pesan yang tidak
//  membocorkan informasi sensitif ke luar sistem.
// ============================================================
import path from 'path';
// ── Limits ────────────────────────────────────────────────────
/** Ukuran dokumen maksimum setelah JSON.stringify (16 MB) */
export const MAX_DOCUMENT_BYTES = 16 * 1024 * 1024;
/** Kedalaman nesting filter / update / dokumen maksimum */
export const MAX_QUERY_DEPTH = 12;
/** Jumlah key dalam satu level filter / update spec maksimum */
export const MAX_FILTER_KEYS = 64;
/** Panjang pattern $regex maksimum (karakter) */
export const MAX_REGEX_PATTERN_LEN = 512;
/** Jumlah elemen $in / $nin maksimum */
export const MAX_IN_ARRAY_SIZE = 1_000;
/** Jumlah stage pipeline aggregation maksimum */
export const MAX_PIPELINE_STAGES = 32;
/** Panjang nama collection maksimum */
export const MAX_COLLECTION_NAME = 64;
/** Panjang field path (dot-notation) maksimum */
export const MAX_FIELD_PATH_LEN = 256;
/** Panjang document _id maksimum */
export const MAX_ID_LEN = 128;
// ── Prototype Pollution Guard ────────────────────────────────
/**
 * Set kunci yang TIDAK PERNAH boleh muncul di field path atau
 * sebagai key dalam dokumen / filter / update spec.
 * Hardcoded agar tidak bisa dimanipulasi runtime.
 */
const DANGEROUS_KEYS = new Set([
    '__proto__',
    'constructor',
    'prototype',
    '__defineGetter__',
    '__defineSetter__',
    '__lookupGetter__',
    '__lookupSetter__',
]);
/**
 * Periksa apakah satu segmen path bersifat berbahaya.
 * Dipakai sebelum setiap operasi set/get/delete nested field.
 */
export function isDangerousKey(key) {
    return DANGEROUS_KEYS.has(key);
}
/**
 * Validasi field path dot-notation (misal: "user.address.city").
 * Lempar Error jika ada segmen yang berbahaya, path terlalu panjang,
 * atau mengandung karakter tak diizinkan.
 *
 * Masalah: path seperti "__proto__.polluted" memanipulasi Object.prototype
 * karena setNestedField melakukan traversal tanpa memeriksa kunci.
 */
export function validateFieldPath(fieldPath) {
    if (typeof fieldPath !== 'string')
        throw new Error('[OvnDB] Field path harus string');
    if (fieldPath.length === 0)
        throw new Error('[OvnDB] Field path tidak boleh kosong');
    if (fieldPath.length > MAX_FIELD_PATH_LEN)
        throw new Error(`[OvnDB] Field path terlalu panjang (maks ${MAX_FIELD_PATH_LEN})`);
    const parts = fieldPath.split('.');
    for (const part of parts) {
        if (part.length === 0)
            throw new Error('[OvnDB] Field path tidak boleh mengandung segment kosong (..)');
        if (isDangerousKey(part))
            throw new Error(`[OvnDB] Field path mengandung kunci terlarang: "${part}"`);
    }
}
// ── Collection Name Validation ───────────────────────────────
/**
 * Whitelist karakter untuk nama collection.
 * Hanya huruf, angka, underscore, dash — tidak ada slash, dot ganda, dsb.
 *
 * Masalah yang dicegah: path traversal via nama seperti "../other"
 * yang akan membuat StorageEngine membuka direktori di luar data dir.
 */
const COLLECTION_NAME_RE = /^[a-zA-Z0-9_-]{1,64}$/;
export function validateCollectionName(name) {
    if (typeof name !== 'string')
        throw new Error('[OvnDB] Nama collection harus string');
    if (!COLLECTION_NAME_RE.test(name))
        throw new Error(`[OvnDB] Nama collection tidak valid: hanya huruf, angka, _ dan - ` +
            `diperbolehkan (1-${MAX_COLLECTION_NAME} karakter)`);
}
// ── Path Traversal Guard ──────────────────────────────────────
/**
 * Pastikan `target` berada di dalam `base` — cegah path traversal
 * pada operasi backup dan manipulasi file.
 *
 * Masalah: backup("../../sensitive") bisa menulis ke luar data dir.
 */
export function assertPathInside(base, target, label = 'path') {
    const resolvedBase = path.resolve(base);
    const resolvedTarget = path.resolve(target);
    // resolvedTarget harus berawalan resolvedBase + path.sep (atau sama persis)
    if (!resolvedTarget.startsWith(resolvedBase + path.sep) && resolvedTarget !== resolvedBase) {
        throw new Error(`[OvnDB] ${label} berada di luar direktori yang diizinkan`);
    }
}
// ── Document Size Guard ───────────────────────────────────────
/**
 * Validasi ukuran dokumen setelah serialisasi.
 * Mencegah single-document yang sangat besar memblokir I/O.
 */
export function validateDocumentSize(buf) {
    if (buf.length > MAX_DOCUMENT_BYTES) {
        throw new Error(`[OvnDB] Dokumen melebihi ukuran maksimum (${buf.length} > ${MAX_DOCUMENT_BYTES} bytes)`);
    }
}
// ── Query Filter Validation ───────────────────────────────────
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
export function validateQueryFilter(filter, depth = 0) {
    if (depth > MAX_QUERY_DEPTH)
        throw new Error(`[OvnDB] Query filter terlalu dalam (maks ${MAX_QUERY_DEPTH} level)`);
    if (filter === null || typeof filter !== 'object')
        return;
    if (Array.isArray(filter)) {
        for (const item of filter)
            validateQueryFilter(item, depth + 1);
        return;
    }
    const obj = filter;
    // FIX: Detect __proto__ injection — when { '__proto__': x } is used in an object literal,
    // the prototype is modified, not a property. Detect via prototype chain check.
    // If proto is not Object.prototype and not null, the filter was created with __proto__ pollution.
    const proto = Object.getPrototypeOf(obj);
    if (proto !== Object.prototype && proto !== null) {
        throw new Error('[OvnDB] Filter mengandung kunci terlarang: "__proto__" (prototype injection detected)');
    }
    // FIX: use Reflect.ownKeys to catch __proto__ which Object.keys() misses
    const keys = Reflect.ownKeys(obj).filter(k => typeof k === 'string');
    if (keys.length > MAX_FILTER_KEYS)
        throw new Error(`[OvnDB] Filter memiliki terlalu banyak key (maks ${MAX_FILTER_KEYS})`);
    for (const key of keys) {
        // Prototype pollution via filter key
        if (isDangerousKey(key))
            throw new Error(`[OvnDB] Filter mengandung kunci terlarang: "${key}"`);
        const val = obj[key];
        // $where — arbitrary function execution, dilarang sepenuhnya
        if (key === '$where')
            throw new Error('[OvnDB] Operator $where tidak diperbolehkan');
        // Field path yang mengandung dot-notation juga harus aman
        if (!key.startsWith('$')) {
            const parts = key.split('.');
            for (const part of parts) {
                if (isDangerousKey(part))
                    throw new Error(`[OvnDB] Field path mengandung kunci terlarang: "${part}"`);
            }
        }
        // Validasi $regex
        if (key === '$regex') {
            validateRegex(val);
        }
        // Validasi $in / $nin size
        if ((key === '$in' || key === '$nin') && Array.isArray(val)) {
            if (val.length > MAX_IN_ARRAY_SIZE)
                throw new Error(`[OvnDB] ${key} terlalu banyak elemen (${val.length} > maks ${MAX_IN_ARRAY_SIZE})`);
        }
        // Rekursi untuk operator logis dan nested object
        if (key === '$and' || key === '$or' || key === '$nor') {
            if (!Array.isArray(val))
                throw new Error(`[OvnDB] ${key} harus berupa array`);
            if (val.length > MAX_FILTER_KEYS)
                throw new Error(`[OvnDB] ${key} terlalu banyak kondisi`);
            for (const sub of val)
                validateQueryFilter(sub, depth + 1);
        }
        else if (key === '$not' || key === '$elemMatch') {
            validateQueryFilter(val, depth + 1);
        }
        else if (val !== null && typeof val === 'object' && !Array.isArray(val)) {
            validateQueryFilter(val, depth + 1);
        }
    }
}
// ── Regex Safety ──────────────────────────────────────────────
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
export function validateRegex(pattern) {
    if (pattern instanceof RegExp)
        return; // sudah dikompilasi, aman
    if (typeof pattern !== 'string')
        throw new Error('[OvnDB] $regex harus berupa string atau RegExp');
    if (pattern.length > MAX_REGEX_PATTERN_LEN)
        throw new Error(`[OvnDB] $regex pattern terlalu panjang (${pattern.length} > maks ${MAX_REGEX_PATTERN_LEN})`);
    // Cegah nested quantifier yang paling sering jadi ReDoS
    // Contoh: (a+)+ atau (a|aa)+ atau (.*)* 
    const REDOS_PATTERNS = [
        /\(\.\*\)\*/, // (.*)* 
        /\(\.\+\)\+/, // (.+)+
        /\([^)]*[+*][^)]*\)[+*]/, // group dengan quantifier diikuti quantifier lagi
        /\{[0-9]+,[0-9]*\}\{/, // berturut-turut quantifier curly
    ];
    for (const danger of REDOS_PATTERNS) {
        if (danger.test(pattern))
            throw new Error('[OvnDB] $regex pattern mengandung pola yang berpotensi ReDoS');
    }
}
// ── Update Spec Validation ────────────────────────────────────
/**
 * Validasi UpdateSpec sebelum diaplikasikan.
 * - Semua field key di semua operator divalidasi dari prototype pollution
 * - $push $each dibatasi ukurannya
 */
export function validateUpdateSpec(spec, depth = 0) {
    if (depth > MAX_QUERY_DEPTH)
        throw new Error('[OvnDB] UpdateSpec terlalu dalam');
    const KNOWN_OPS = new Set([
        '$set', '$unset', '$inc', '$push', '$pull', '$addToSet',
        '$rename', '$mul', '$min', '$max', '$setOnInsert',
    ]);
    for (const op of Object.keys(spec)) {
        if (!KNOWN_OPS.has(op))
            throw new Error(`[OvnDB] Operator update tidak dikenal: "${op}"`);
        const opVal = spec[op];
        if (opVal === null || typeof opVal !== 'object')
            continue;
        // FIX: Detect __proto__ injection via prototype chain (when { '__proto__': x } sets proto)
        const opProto = Object.getPrototypeOf(opVal);
        if (opProto !== Object.prototype && opProto !== null) {
            throw new Error(`[OvnDB] Update spec mengandung kunci terlarang: "__proto__" (prototype injection detected)`);
        }
        // FIX: use Reflect.ownKeys to catch __proto__ which Object.keys() misses
        const fieldKeys = Reflect.ownKeys(opVal)
            .filter(k => typeof k === 'string');
        for (const fieldKey of fieldKeys) {
            if (isDangerousKey(fieldKey))
                throw new Error(`[OvnDB] Update spec mengandung kunci terlarang: "${fieldKey}"`);
            // Validasi tiap segmen dot-notation
            for (const part of fieldKey.split('.')) {
                if (isDangerousKey(part))
                    throw new Error(`[OvnDB] Field path update mengandung kunci terlarang: "${part}"`);
            }
        }
        // $push $each size limit
        if (op === '$push') {
            for (const [, v] of Object.entries(opVal)) {
                if (v !== null && typeof v === 'object' && '$each' in v) {
                    const each = v['$each'];
                    if (Array.isArray(each) && each.length > MAX_IN_ARRAY_SIZE)
                        throw new Error(`[OvnDB] $push.$each terlalu banyak elemen (maks ${MAX_IN_ARRAY_SIZE})`);
                }
            }
        }
    }
}
// ── Document Key Validation ───────────────────────────────────
/**
 * Validasi bahwa dokumen tidak mengandung key berbahaya
 * di level manapun. Dilakukan pada insert dan replace.
 */
export function validateDocumentKeys(doc, depth = 0) {
    if (depth > MAX_QUERY_DEPTH)
        return; // terlalu dalam — size limit menangani ini
    if (doc === null || typeof doc !== 'object')
        return;
    if (Array.isArray(doc)) {
        for (const item of doc)
            validateDocumentKeys(item, depth + 1);
        return;
    }
    // FIX: Detect __proto__ injection via prototype chain
    const proto = Object.getPrototypeOf(doc);
    if (proto !== Object.prototype && proto !== null) {
        throw new Error('[OvnDB] Dokumen mengandung kunci terlarang: "__proto__" (prototype injection detected)');
    }
    // FIX: use Reflect.ownKeys to catch __proto__ which Object.keys() misses
    for (const key of Reflect.ownKeys(doc)) {
        if (typeof key !== 'string')
            continue;
        if (isDangerousKey(key))
            throw new Error(`[OvnDB] Dokumen mengandung kunci terlarang: "${key}"`);
        validateDocumentKeys(doc[key], depth + 1);
    }
}
// ── ID Validation ─────────────────────────────────────────────
/**
 * Validasi _id jika disupply user (bukan generated).
 * Cegah _id yang terlalu panjang atau mengandung null byte.
 */
export function validateDocumentId(id) {
    if (typeof id !== 'string')
        throw new Error('[OvnDB] _id harus berupa string');
    if (id.length === 0)
        throw new Error('[OvnDB] _id tidak boleh kosong');
    if (id.length > MAX_ID_LEN)
        throw new Error(`[OvnDB] _id terlalu panjang (maks ${MAX_ID_LEN})`);
    if (id.includes('\0'))
        throw new Error('[OvnDB] _id tidak boleh mengandung null byte');
}
//# sourceMappingURL=security.js.map