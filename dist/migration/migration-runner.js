// ============================================================
//  OvnDB v3.0 — MigrationRunner (G18)
//
//  System migrasi schema untuk evolusi dokumen yang sudah ada.
//  Mendukung: dry-run, batch cursor, progress callback, continueOnError.
// ============================================================
import { validateQueryFilter } from '../utils/security.js';
import { makeLogger } from '../utils/logger.js';
const log = makeLogger('migration');
export class MigrationRunner {
    col;
    constructor(collection) { this.col = collection; }
    /**
     * Migrate dokumen dari versi lama ke targetVersion.
     *
     * @example
     *   const runner = new MigrationRunner(users);
     *   await runner.migrate(2, (doc) => ({
     *     ...doc,
     *     fullName: `${doc.firstName} ${doc.lastName}`,
     *   }));
     */
    async migrate(targetVersion, transformer, options = {}) {
        // SECURITY: validasi targetVersion — harus bilangan bulat positif
        if (!Number.isInteger(targetVersion) || targetVersion < 1)
            throw new Error('[MigrationRunner] targetVersion harus bilangan bulat >= 1');
        const { batchSize: rawBatch = 500, dryRun = false, filter = {}, onProgress, continueOnError = false, timeoutMs: rawTimeout = 5_000, } = options;
        // SECURITY: clamp batchSize — cegah nilai 0 (infinite loop) atau sangat besar (OOM)
        const batchSize = Math.min(Math.max(Math.floor(rawBatch), 1), 10_000);
        // SECURITY: clamp timeoutMs — cegah timeout 0 (race condition) atau negatif
        const timeoutMs = Math.max(rawTimeout, 100);
        // SECURITY: validasi filter yang disupply user sebelum dipakai dalam migrationFilter
        validateQueryFilter(filter);
        const startTime = Date.now();
        const result = {
            migrated: 0, failed: 0, skipped: 0, total: 0,
            elapsedMs: 0, errors: [], dryRun, dryRunCount: 0,
        };
        const migrationFilter = {
            $and: [
                filter,
                {
                    $or: [
                        { _schemaVersion: { $exists: false } },
                        { _schemaVersion: { $lt: targetVersion } },
                    ],
                },
            ],
        };
        log.info('Migration started', { col: this.col.name, targetVersion, dryRun, batchSize });
        if (dryRun) {
            result.dryRunCount = await this.col.countDocuments(migrationFilter);
            result.total = result.dryRunCount;
            result.elapsedMs = Date.now() - startTime;
            log.info('Dry run complete', { count: result.dryRunCount });
            return result;
        }
        let after;
        let batchNum = 0;
        while (true) {
            const batch = await this.col.find(migrationFilter, {
                limit: batchSize,
                after,
                sort: { _id: 1 },
            });
            if (batch.length === 0)
                break;
            batchNum++;
            result.total += batch.length;
            for (const doc of batch) {
                try {
                    const transformed = await Promise.race([
                        Promise.resolve(transformer(doc)),
                        new Promise((_, reject) => setTimeout(() => reject(new Error(`Transform timeout ${timeoutMs}ms`)), timeoutMs)),
                    ]);
                    const updated = { ...transformed, _schemaVersion: targetVersion };
                    await this.col.replaceOne({ _id: doc._id }, updated);
                    result.migrated++;
                }
                catch (err) {
                    const errMsg = err instanceof Error ? err.message : String(err);
                    result.failed++;
                    result.errors.push({ id: doc._id, error: errMsg });
                    log.error('Migration error', { id: doc._id, err: errMsg });
                    if (!continueOnError) {
                        result.elapsedMs = Date.now() - startTime;
                        throw new Error(`[MigrationRunner] Stopped at "${doc._id}" after ${result.migrated} ok: ${errMsg}`);
                    }
                }
            }
            await this.col.flush();
            after = batch[batch.length - 1]?._id;
            onProgress?.({
                migrated: result.migrated, failed: result.failed,
                skipped: result.skipped, total: result.total,
                elapsed: Date.now() - startTime,
            });
            log.debug(`Batch ${batchNum} done`, { migrated: result.migrated, failed: result.failed });
        }
        result.elapsedMs = Date.now() - startTime;
        log.info('Migration complete', {
            col: this.col.name, targetVersion,
            migrated: result.migrated, failed: result.failed, elapsedMs: result.elapsedMs,
        });
        return result;
    }
    /** Hitung dokumen yang perlu di-migrate ke targetVersion. */
    async countPending(targetVersion, filter = {}) {
        return this.col.countDocuments({
            $and: [
                filter,
                {
                    $or: [
                        { _schemaVersion: { $exists: false } },
                        { _schemaVersion: { $lt: targetVersion } },
                    ],
                },
            ],
        });
    }
    /** Cek apakah semua dokumen sudah pada targetVersion. */
    async isComplete(targetVersion, filter = {}) {
        return (await this.countPending(targetVersion, filter)) === 0;
    }
}
//# sourceMappingURL=migration-runner.js.map