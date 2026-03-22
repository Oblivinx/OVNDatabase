// ============================================================
//  OvnDB v3.0 — MigrationRunner (G18)
//
//  System migrasi schema untuk evolusi dokumen yang sudah ada.
//  Mendukung: dry-run, batch cursor, progress callback, continueOnError.
// ============================================================

import type { Collection }  from '../collection/collection.js';
import type { OvnDocument, QueryFilter } from '../types/index.js';
import { makeLogger }       from '../utils/logger.js';

const log = makeLogger('migration');

export interface MigrationOptions {
  /** Ukuran batch per iterasi. @default 500 */
  batchSize?:       number;
  /** Jika true, hanya hitung dokumen tanpa mengubah data. */
  dryRun?:          boolean;
  /** Filter tambahan pada dokumen yang akan di-migrate. */
  filter?:          QueryFilter;
  /** Callback setiap batch selesai. */
  onProgress?:      (p: MigrationProgress) => void;
  /** Lanjutkan meskipun ada error per-dokumen. @default false */
  continueOnError?: boolean;
  /** Timeout per-dokumen transformer (ms). @default 5000 */
  timeoutMs?:       number;
}

export interface MigrationProgress {
  migrated: number;
  failed:   number;
  skipped:  number;
  total:    number;
  elapsed:  number;
}

export interface MigrationResult {
  migrated:    number;
  failed:      number;
  skipped:     number;
  total:       number;
  elapsedMs:   number;
  errors:      Array<{ id: string; error: string }>;
  dryRun:      boolean;
  dryRunCount: number;
}

export class MigrationRunner<T extends OvnDocument = OvnDocument> {
  private readonly col: Collection<T>;
  constructor(collection: Collection<T>) { this.col = collection; }

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
  async migrate(
    targetVersion: number,
    transformer:   (doc: T) => T | Promise<T>,
    options:       MigrationOptions = {},
  ): Promise<MigrationResult> {
    const {
      batchSize       = 500,
      dryRun          = false,
      filter          = {},
      onProgress,
      continueOnError = false,
      timeoutMs       = 5_000,
    } = options;

    const startTime = Date.now();
    const result: MigrationResult = {
      migrated: 0, failed: 0, skipped: 0, total: 0,
      elapsedMs: 0, errors: [], dryRun, dryRunCount: 0,
    };

    const migrationFilter: QueryFilter = {
      $and: [
        filter,
        {
          $or: [
            { _schemaVersion: { $exists: false } } as QueryFilter,
            { _schemaVersion: { $lt: targetVersion } } as QueryFilter,
          ],
        },
      ],
    };

    log.info('Migration started', { col: this.col.name, targetVersion, dryRun, batchSize });

    if (dryRun) {
      result.dryRunCount = await this.col.countDocuments(migrationFilter);
      result.total       = result.dryRunCount;
      result.elapsedMs   = Date.now() - startTime;
      log.info('Dry run complete', { count: result.dryRunCount });
      return result;
    }

    let after: string | undefined;
    let batchNum = 0;

    while (true) {
      const batch = await this.col.find(migrationFilter, {
        limit: batchSize,
        after,
        sort: { _id: 1 },
      });
      if (batch.length === 0) break;
      batchNum++;
      result.total += batch.length;

      for (const doc of batch) {
        try {
          const transformed = await Promise.race([
            Promise.resolve(transformer(doc)),
            new Promise<never>((_, reject) =>
              setTimeout(() => reject(new Error(`Transform timeout ${timeoutMs}ms`)), timeoutMs),
            ),
          ]);
          const updated = { ...transformed, _schemaVersion: targetVersion } as T;
          await this.col.replaceOne({ _id: doc._id }, updated);
          result.migrated++;
        } catch (err) {
          const errMsg = err instanceof Error ? err.message : String(err);
          result.failed++;
          result.errors.push({ id: doc._id, error: errMsg });
          log.error('Migration error', { id: doc._id, err: errMsg });
          if (!continueOnError) {
            result.elapsedMs = Date.now() - startTime;
            throw new Error(
              `[MigrationRunner] Stopped at "${doc._id}" after ${result.migrated} ok: ${errMsg}`,
            );
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
  async countPending(targetVersion: number, filter: QueryFilter = {}): Promise<number> {
    return this.col.countDocuments({
      $and: [
        filter,
        {
          $or: [
            { _schemaVersion: { $exists: false } } as QueryFilter,
            { _schemaVersion: { $lt: targetVersion } } as QueryFilter,
          ],
        },
      ],
    });
  }

  /** Cek apakah semua dokumen sudah pada targetVersion. */
  async isComplete(targetVersion: number, filter: QueryFilter = {}): Promise<boolean> {
    return (await this.countPending(targetVersion, filter)) === 0;
  }
}
