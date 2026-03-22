import type { Collection } from '../collection/collection.js';
import type { OvnDocument, QueryFilter } from '../types/index.js';
export interface MigrationOptions {
    /** Ukuran batch per iterasi. @default 500 */
    batchSize?: number;
    /** Jika true, hanya hitung dokumen tanpa mengubah data. */
    dryRun?: boolean;
    /** Filter tambahan pada dokumen yang akan di-migrate. */
    filter?: QueryFilter;
    /** Callback setiap batch selesai. */
    onProgress?: (p: MigrationProgress) => void;
    /** Lanjutkan meskipun ada error per-dokumen. @default false */
    continueOnError?: boolean;
    /** Timeout per-dokumen transformer (ms). @default 5000 */
    timeoutMs?: number;
}
export interface MigrationProgress {
    migrated: number;
    failed: number;
    skipped: number;
    total: number;
    elapsed: number;
}
export interface MigrationResult {
    migrated: number;
    failed: number;
    skipped: number;
    total: number;
    elapsedMs: number;
    errors: Array<{
        id: string;
        error: string;
    }>;
    dryRun: boolean;
    dryRunCount: number;
}
export declare class MigrationRunner<T extends OvnDocument = OvnDocument> {
    private readonly col;
    constructor(collection: Collection<T>);
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
    migrate(targetVersion: number, transformer: (doc: T) => T | Promise<T>, options?: MigrationOptions): Promise<MigrationResult>;
    /** Hitung dokumen yang perlu di-migrate ke targetVersion. */
    countPending(targetVersion: number, filter?: QueryFilter): Promise<number>;
    /** Cek apakah semua dokumen sudah pada targetVersion. */
    isComplete(targetVersion: number, filter?: QueryFilter): Promise<boolean>;
}
//# sourceMappingURL=migration-runner.d.ts.map