// ============================================================
//  OvnDB v2.0 — Query Filter Engine
//
//  Implementasi semua operator MongoDB-style:
//   Comparison: $eq $ne $gt $gte $lt $lte $in $nin
//   Logical:    $and $or $nor $not
//   Element:    $exists
//   Evaluation: $regex
//   Array:      $size $all $elemMatch
//
//  Design: pure functions, tidak ada side effects, mudah di-test.
//
//  v3.1 FIXES:
//  - $regex: compile sekali per unique pattern via _regexCache (fix ~10x slowdown di regex queries)
// ============================================================
import { isDangerousKey, validateRegex } from '../../utils/security.js';
// FIX: Cache compiled RegExp agar tidak di-compile ulang setiap match call.
// Full scan 1M dokumen dengan $regex tanpa cache = 1M RegExp object allocations.
// Keyed by "pattern::flags" string. WeakMap tidak cocok karena key harus string.
const _regexCache = new Map();
function _getOrCompileRegex(pattern, flags) {
    if (pattern instanceof RegExp)
        return pattern;
    // SECURITY: validasi pattern sebelum kompilasi — cegah ReDoS dan pattern terlalu panjang
    validateRegex(pattern);
    const cacheKey = `${pattern}::${flags ?? ''}`;
    let re = _regexCache.get(cacheKey);
    if (!re) {
        re = new RegExp(pattern, flags);
        // Batas cache 512 entries — cegah memory leak untuk query patterns yang sangat beragam
        if (_regexCache.size >= 512) {
            const firstKey = _regexCache.keys().next().value;
            _regexCache.delete(firstKey);
        }
        _regexCache.set(cacheKey, re);
    }
    return re;
}
// ── Filter Matching ──────────────────────────────────────────
/**
 * Periksa apakah dokumen `doc` cocok dengan filter `filter`.
 * @returns true jika cocok, false jika tidak
 */
export function matchFilter(doc, filter) {
    for (const [key, condition] of Object.entries(filter)) {
        // SECURITY: blokir prototype pollution via filter key
        if (isDangerousKey(key))
            throw new Error(`[OvnDB] Filter mengandung kunci terlarang: "${key}"`);
        // SECURITY: $where — arbitrary code execution, dilarang
        if (key === '$where')
            throw new Error('[OvnDB] Operator $where tidak diperbolehkan');
        // v4.0: $text adalah handled by _scanWithPlan before matchFilter — skip here
        if (key === '$text')
            continue;
        if (key === '$and') {
            if (!condition.every(f => matchFilter(doc, f)))
                return false;
        }
        else if (key === '$or') {
            if (!condition.some(f => matchFilter(doc, f)))
                return false;
        }
        else if (key === '$nor') {
            if (condition.some(f => matchFilter(doc, f)))
                return false;
        }
        else if (key === '$not') {
            if (matchFilter(doc, condition))
                return false;
        }
        else {
            // SECURITY: validasi tiap segmen dot-notation field path
            for (const part of key.split('.')) {
                if (isDangerousKey(part))
                    throw new Error(`[OvnDB] Field path mengandung kunci terlarang: "${part}"`);
            }
            const fieldVal = getFieldValue(doc, key);
            if (!matchField(fieldVal, condition))
                return false;
        }
    }
    return true;
}
/** Ambil nilai field dari dokumen, support dot notation (a.b.c). */
export function getFieldValue(doc, path) {
    const parts = path.split('.');
    let val = doc;
    for (const part of parts) {
        // SECURITY: blokir prototype pollution traversal
        if (isDangerousKey(part))
            return undefined;
        if (val === null || typeof val !== 'object')
            return undefined;
        val = val[part];
    }
    return val;
}
/** Periksa apakah nilai field cocok dengan kondisi (scalar atau operator). */
function matchField(val, condition) {
    if (condition === null || typeof condition !== 'object') {
        // Perbandingan langsung
        return val === condition;
    }
    const ops = condition;
    if ('$eq' in ops && !(val === ops.$eq))
        return false;
    if ('$ne' in ops && !(val !== ops.$ne))
        return false;
    if ('$gt' in ops && !(typeof val === typeof ops.$gt && val > ops.$gt))
        return false;
    if ('$gte' in ops && !(typeof val === typeof ops.$gte && val >= ops.$gte))
        return false;
    if ('$lt' in ops && !(typeof val === typeof ops.$lt && val < ops.$lt))
        return false;
    if ('$lte' in ops && !(typeof val === typeof ops.$lte && val <= ops.$lte))
        return false;
    if ('$in' in ops) {
        const arr = ops.$in;
        if (!Array.isArray(arr))
            return false;
        if (arr.length > 1_000)
            throw new Error('[OvnDB] $in terlalu banyak elemen (maks 1000)');
        if (!arr.includes(val))
            return false;
    }
    if ('$nin' in ops) {
        const arr = ops.$nin;
        if (!Array.isArray(arr))
            return false;
        if (arr.length > 1_000)
            throw new Error('[OvnDB] $nin terlalu banyak elemen (maks 1000)');
        if (arr.includes(val))
            return false;
    }
    if ('$exists' in ops && (val !== undefined) !== ops.$exists)
        return false;
    if ('$regex' in ops) {
        if (typeof val !== 'string')
            return false;
        // FIX: pakai cache — tidak compile RegExp baru setiap call
        const re = _getOrCompileRegex(ops.$regex, ops['$options']);
        if (!re.test(val))
            return false;
    }
    if ('$size' in ops) {
        if (!Array.isArray(val) || val.length !== ops.$size)
            return false;
    }
    if ('$all' in ops) {
        if (!Array.isArray(val))
            return false;
        for (const item of ops.$all) {
            if (!val.includes(item))
                return false;
        }
    }
    if ('$elemMatch' in ops) {
        if (!Array.isArray(val))
            return false;
        const spec = ops.$elemMatch;
        const isScalarMatch = Object.keys(spec).every(k => k.startsWith('$'));
        if (isScalarMatch) {
            if (!val.some(elem => matchField(elem, spec)))
                return false;
        }
        else {
            if (!val.some(elem => matchFilter(elem, spec)))
                return false;
        }
    }
    // G10: $type — cek tipe nilai field
    if ('$type' in ops) {
        const expected = ops['$type'];
        const actual = val === null
            ? 'null'
            : Array.isArray(val)
                ? 'array'
                : typeof val;
        if (actual !== expected)
            return false;
    }
    // G10: $where — DILARANG: arbitrary code execution risk
    // Operator ini memungkinkan caller mengeksekusi fungsi arbitrer dengan akses
    // ke nilai field dokumen. Ini adalah celah eksekusi kode yang tidak dapat ditoleransi
    // di database embedded. Operator dihapus dari implementasi.
    if ('$where' in ops) {
        throw new Error('[OvnDB] Operator $where tidak diperbolehkan');
    }
    // G10: $mod — modulo check: [divisor, remainder]
    if ('$mod' in ops) {
        const [divisor, remainder] = ops['$mod'];
        if (typeof val !== 'number' || val % divisor !== remainder)
            return false;
    }
    // G10: $not — negasi dari kondisi
    if ('$not' in ops) {
        const inner = ops['$not'];
        if (matchField(val, inner))
            return false;
    }
    return true;
}
// ── Update Application ────────────────────────────────────────
/**
 * Terapkan UpdateSpec ke dokumen dan kembalikan dokumen baru.
 * Tidak memodifikasi dokumen asli (immutable).
 */
export function applyUpdate(doc, spec) {
    const result = { ...doc };
    // $set — set field ke nilai baru
    if (spec.$set) {
        for (const [k, v] of Object.entries(spec.$set)) {
            setNestedField(result, k, v);
        }
    }
    // $unset — hapus field
    if (spec.$unset) {
        for (const k of Object.keys(spec.$unset)) {
            deleteNestedField(result, k);
        }
    }
    // $inc — tambah nilai numerik (bisa negatif untuk pengurangan)
    if (spec.$inc) {
        for (const [k, v] of Object.entries(spec.$inc)) {
            const cur = getNestedField(result, k);
            setNestedField(result, k, (typeof cur === 'number' ? cur : 0) + v);
        }
    }
    // $mul — kalikan nilai numerik
    if (spec.$mul) {
        for (const [k, v] of Object.entries(spec.$mul)) {
            const cur = getNestedField(result, k);
            setNestedField(result, k, (typeof cur === 'number' ? cur : 0) * v);
        }
    }
    // $push — tambah ke array (support $each, $sort, $slice)
    if (spec.$push) {
        for (const [k, v] of Object.entries(spec.$push)) {
            const cur = getNestedField(result, k);
            const arr = Array.isArray(cur) ? [...cur] : [];
            if (v !== null && typeof v === 'object' && '$each' in v) {
                const pushSpec = v;
                arr.push(...pushSpec.$each);
                if (pushSpec.$sort) {
                    const [sortField, sortDir] = Object.entries(pushSpec.$sort)[0];
                    arr.sort((a, b) => {
                        const av = a[sortField];
                        const bv = b[sortField];
                        return (av > bv ? 1 : -1) * sortDir;
                    });
                }
                if (typeof pushSpec.$slice === 'number')
                    arr.splice(pushSpec.$slice);
            }
            else {
                arr.push(v);
            }
            setNestedField(result, k, arr);
        }
    }
    // $pull — hapus elemen dari array yang cocok dengan kondisi
    if (spec.$pull) {
        for (const [k, v] of Object.entries(spec.$pull)) {
            const cur = getNestedField(result, k);
            if (Array.isArray(cur)) {
                const filtered = cur.filter(item => {
                    if (v !== null && typeof v === 'object') {
                        return !matchFilter(item, v);
                    }
                    return item !== v;
                });
                setNestedField(result, k, filtered);
            }
        }
    }
    // $addToSet — push hanya jika belum ada (set semantics)
    if (spec.$addToSet) {
        for (const [k, v] of Object.entries(spec.$addToSet)) {
            const cur = getNestedField(result, k);
            const arr = Array.isArray(cur) ? [...cur] : [];
            if (!arr.some(item => JSON.stringify(item) === JSON.stringify(v))) {
                arr.push(v);
            }
            setNestedField(result, k, arr);
        }
    }
    // $rename — rename field
    if (spec.$rename) {
        for (const [oldKey, newKey] of Object.entries(spec.$rename)) {
            const val = getNestedField(result, oldKey);
            deleteNestedField(result, oldKey);
            setNestedField(result, newKey, val);
        }
    }
    // $min — set ke nilai minimum
    if (spec.$min) {
        for (const [k, v] of Object.entries(spec.$min)) {
            const cur = getNestedField(result, k);
            if (typeof cur !== 'number' || v < cur) {
                setNestedField(result, k, v);
            }
        }
    }
    // $max — set ke nilai maksimum
    if (spec.$max) {
        for (const [k, v] of Object.entries(spec.$max)) {
            const cur = getNestedField(result, k);
            if (typeof cur !== 'number' || v > cur) {
                setNestedField(result, k, v);
            }
        }
    }
    return result;
}
// ── Dot Notation Helpers ─────────────────────────────────────
export function getNestedField(obj, path) {
    return path.split('.').reduce((o, k) => {
        // SECURITY: blokir prototype pollution traversal
        if (isDangerousKey(k))
            return undefined;
        return o !== null && typeof o === 'object' ? o[k] : undefined;
    }, obj);
}
export function setNestedField(obj, path, val) {
    const parts = path.split('.');
    let cur = obj;
    for (let i = 0; i < parts.length - 1; i++) {
        const p = parts[i];
        // SECURITY: tolak kunci berbahaya di setiap segmen path
        if (isDangerousKey(p))
            throw new Error(`[OvnDB] Field path mengandung kunci terlarang: "${p}"`);
        if (typeof cur[p] !== 'object' || cur[p] === null)
            cur[p] = {};
        cur = cur[p];
    }
    const lastKey = parts[parts.length - 1];
    // SECURITY: tolak kunci berbahaya pada segmen akhir
    if (isDangerousKey(lastKey))
        throw new Error(`[OvnDB] Field path mengandung kunci terlarang: "${lastKey}"`);
    cur[lastKey] = val;
}
export function deleteNestedField(obj, path) {
    const parts = path.split('.');
    let cur = obj;
    for (let i = 0; i < parts.length - 1; i++) {
        const p = parts[i];
        // SECURITY: tolak kunci berbahaya di setiap segmen path
        if (isDangerousKey(p))
            return;
        if (typeof cur[p] !== 'object' || cur[p] === null)
            return;
        cur = cur[p];
    }
    const lastKey = parts[parts.length - 1];
    // SECURITY: tolak kunci berbahaya pada segmen akhir
    if (isDangerousKey(lastKey))
        return;
    delete cur[lastKey];
}
// ── Projection ────────────────────────────────────────────────
/**
 * Terapkan projection ke dokumen.
 * Aturan: 1 = include, 0 = exclude.
 * _id selalu include kecuali secara eksplisit di-exclude.
 */
export function applyProjection(doc, projection) {
    if (!projection || Object.keys(projection).length === 0)
        return doc;
    const values = Object.values(projection);
    const isInclude = values.some(v => v === 1);
    const result = {};
    if (isInclude) {
        // Inclusion mode: hanya field yang di-set 1
        if (projection['_id'] !== 0)
            result['_id'] = doc['_id'];
        for (const [k, v] of Object.entries(projection)) {
            if (v === 1 && k in doc)
                result[k] = doc[k];
        }
    }
    else {
        // Exclusion mode: semua field kecuali yang di-set 0
        for (const [k, v] of Object.entries(doc)) {
            if (projection[k] !== 0)
                result[k] = v;
        }
    }
    return result;
}
//# sourceMappingURL=filter.js.map