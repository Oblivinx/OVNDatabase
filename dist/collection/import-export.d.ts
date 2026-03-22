import type { Collection } from './collection.js';
import type { OvnDocument, ExportOptions, ImportOptions, ImportResult } from '../types/index.js';
import { validateCollectionName } from '../utils/security.js';
/**
 * Export all documents in a collection to a file.
 * Default format: NDJSON (one JSON per line, streaming).
 *
 * @param col       Collection to export from
 * @param filePath  Output file path
 * @param opts      Export options
 */
export declare function exportTo<T extends OvnDocument>(col: Collection<T>, filePath: string, opts?: ExportOptions): Promise<number>;
/**
 * Import documents from a file into a collection.
 * Default format: NDJSON (auto-detected from .ndjson extension if not specified).
 *
 * @param col       Collection to import into
 * @param filePath  Input file path
 * @param opts      Import options
 * @returns         Import result with counts and errors
 */
export declare function importFrom<T extends OvnDocument>(col: Collection<T>, filePath: string, opts?: ImportOptions): Promise<ImportResult>;
export { validateCollectionName };
//# sourceMappingURL=import-export.d.ts.map