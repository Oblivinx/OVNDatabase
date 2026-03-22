// ============================================================
//  OvnDB v4.0 — Import / Export
//
//  Supported formats:
//  - NDJSON (Newline-Delimited JSON) — satu dokumen per baris,
//    streaming, efficient untuk dataset besar
//  - JSON array — seluruh dataset dalam satu array
//
//  Cara pakai:
//    await exportTo(col, './dump.ndjson');
//    await exportTo(col, './dump.json', { format: 'json' });
//    const result = await importFrom(col, './dump.ndjson');
//    const result = await importFrom(col, './dump.json', { format: 'json' });
// ============================================================
import fsp from 'fs/promises';
import fs from 'fs';
import readline from 'readline';
import { validateCollectionName } from '../utils/security.js';
import { applyProjection } from '../core/query/filter.js';
import { makeLogger } from '../utils/logger.js';
const log = makeLogger('import-export');
/**
 * Export all documents in a collection to a file.
 * Default format: NDJSON (one JSON per line, streaming).
 *
 * @param col       Collection to export from
 * @param filePath  Output file path
 * @param opts      Export options
 */
export async function exportTo(col, filePath, opts = {}) {
    const format = opts.format ?? 'ndjson';
    // SECURITY: basic path sanity (no collection traversal required here, just ensure writable)
    const resolved = filePath; // caller responsible for path safety
    let count = 0;
    if (format === 'ndjson') {
        const fd = await fsp.open(resolved, 'w');
        const ws = fd.createWriteStream({ encoding: 'utf8' });
        try {
            const docs = await col.find({});
            for (const doc of docs) {
                const out = opts.projection ? applyProjection(doc, opts.projection) : doc;
                ws.write(JSON.stringify(out) + '\n');
                count++;
            }
        }
        finally {
            await new Promise((resolve, reject) => ws.end((err) => err ? reject(err) : resolve()));
            await fd.close();
        }
    }
    else {
        // JSON array format — load all then write
        const docs = await col.find({});
        const out = opts.projection
            ? docs.map(d => applyProjection(d, opts.projection))
            : docs;
        await fsp.writeFile(resolved, JSON.stringify(out, null, 2), { encoding: 'utf8' });
        count = docs.length;
    }
    log.info(`Exported ${count} docs`, { format, path: resolved });
    return count;
}
/**
 * Import documents from a file into a collection.
 * Default format: NDJSON (auto-detected from .ndjson extension if not specified).
 *
 * @param col       Collection to import into
 * @param filePath  Input file path
 * @param opts      Import options
 * @returns         Import result with counts and errors
 */
export async function importFrom(col, filePath, opts = {}) {
    const continueOnError = opts.continueOnError !== false;
    const upsert = opts.upsert === true;
    // Auto-detect format from extension if not specified
    const format = opts.format ?? (filePath.endsWith('.json') && !filePath.endsWith('.ndjson') ? 'json' : 'ndjson');
    const result = { total: 0, inserted: 0, skipped: 0, errors: [] };
    if (format === 'ndjson') {
        await importNdjson(col, filePath, upsert, continueOnError, result);
    }
    else {
        await importJson(col, filePath, upsert, continueOnError, result);
    }
    log.info(`Imported ${result.inserted}/${result.total} docs`, { format, path: filePath });
    return result;
}
// ── Internal helpers ─────────────────────────────────────────
async function importNdjson(col, filePath, upsert, continueOnError, result) {
    const stream = fs.createReadStream(filePath, { encoding: 'utf8' });
    const rl = readline.createInterface({ input: stream, crlfDelay: Infinity });
    let lineNum = 0;
    for await (const line of rl) {
        const trimmed = line.trim();
        if (!trimmed)
            continue;
        result.total++;
        lineNum++;
        try {
            const doc = JSON.parse(trimmed);
            await insertOrUpsert(col, doc, upsert, result);
        }
        catch (err) {
            const errMsg = err instanceof Error ? err.message : String(err);
            result.errors.push({ index: lineNum, error: errMsg });
            result.skipped++;
            if (!continueOnError)
                throw err;
        }
    }
}
async function importJson(col, filePath, upsert, continueOnError, result) {
    const raw = await fsp.readFile(filePath, 'utf8');
    const docs = JSON.parse(raw);
    if (!Array.isArray(docs))
        throw new Error('[OvnDB] importFrom: JSON format expects an array of documents');
    result.total = docs.length;
    for (let i = 0; i < docs.length; i++) {
        try {
            await insertOrUpsert(col, docs[i], upsert, result);
        }
        catch (err) {
            const errMsg = err instanceof Error ? err.message : String(err);
            result.errors.push({ index: i, error: errMsg });
            result.skipped++;
            if (!continueOnError)
                throw err;
        }
    }
}
async function insertOrUpsert(col, doc, upsert, result) {
    if (upsert && doc._id) {
        await col.upsertOne({ _id: doc._id }, { $set: doc });
        result.inserted++;
    }
    else {
        await col.insertOne(doc);
        result.inserted++;
    }
}
export { validateCollectionName }; // re-export for convenience
//# sourceMappingURL=import-export.js.map