// ============================================================
//  OvnDB v2.1 — Document, Query DSL, & Storage Types
//  update: BulkWriteOp, BulkWriteResult, DBStatus, CursorOptions,
//          $setOnInsert in UpdateSpec
// ============================================================

export interface OvnDocument {
  _id: string;
  [key: string]: unknown;
}

export type ForeignKey<_C extends string = string> = string;
export type ForeignKeyArray<_C extends string = string> = string[];

export interface RecordPointer {
  segmentId: number;
  offset:    number;
  totalSize: number;
  dataSize:  number;
  txId:      bigint;
}

export interface SegmentMeta {
  id:      number;
  path:    string;
  size:    number;
  live:    number;
  dead:    number;
  fragmentation: number;
}

export interface CollectionManifest {
  version:    number;
  collection: string;
  flags:      number;
  segments:   SegmentMeta[];
  createdAt:  number;
  updatedAt:  number;
  totalLive:  bigint;
  totalDead:  bigint;
}

export interface PendingWrite {
  id:     string;
  data:   Buffer;
  op:     import('./constants.js').WalOp;
  txId:   bigint;
}

export type Scalar = string | number | boolean | null;

export interface FieldOps {
  $eq?:      Scalar;
  $ne?:      Scalar;
  $gt?:      number | string;
  $gte?:     number | string;
  $lt?:      number | string;
  $lte?:     number | string;
  $in?:      Scalar[];
  $nin?:     Scalar[];
  $exists?:  boolean;
  $regex?:   string | RegExp;
  $size?:    number;
  $all?:     Scalar[];
  $elemMatch?: QueryFilter;
}

export interface QueryFilter {
  [field: string]: Scalar | FieldOps | QueryFilter[] | QueryFilter | undefined;
  $and?: QueryFilter[];
  $or?:  QueryFilter[];
  $nor?: QueryFilter[];
  $not?: QueryFilter;
}

export interface QueryOptions {
  limit?:      number;
  skip?:       number;
  sort?:       Record<string, 1 | -1>;
  projection?: Record<string, 0 | 1>;
  after?:      string;
  hint?:       string;
  explain?:    boolean;
}

export interface UpdateSpec {
  $set?:         Record<string, unknown>;
  $unset?:       Record<string, 1>;
  $inc?:         Record<string, number>;
  $push?:        Record<string, unknown | { $each: unknown[]; $sort?: Record<string,1|-1>; $slice?: number }>;
  $pull?:        Record<string, unknown>;
  $addToSet?:    Record<string, unknown>;
  $rename?:      Record<string, string>;
  $mul?:         Record<string, number>;
  $min?:         Record<string, number | string>;
  $max?:         Record<string, number | string>;
  /** feat: only applied during upsert when a new document is created */
  $setOnInsert?: Record<string, unknown>;
  [key: string]: unknown;
}

export type AggregationStage =
  | { $match:    QueryFilter }
  | { $project:  Record<string, 0 | 1 | string | Record<string, unknown>> }
  | { $sort:     Record<string, 1 | -1> }
  | { $limit:    number }
  | { $skip:     number }
  | { $group:    { _id: unknown; [field: string]: unknown } }
  | { $unwind:   string | { path: string; preserveNullAndEmptyArrays?: boolean } }
  | { $lookup:   { from: string; localField: string; foreignField: string; as: string } }
  | { $count:    string }
  | { $addFields: Record<string, unknown> }
  | { $replaceRoot: { newRoot: unknown } };

export type ScanType = 'primaryKey' | 'indexScan' | 'fullCollection';

export interface QueryPlan {
  planType:          ScanType;
  indexField?:       string;
  estimatedDocs:     number;
  estimatedCost:     number;
  indexCardinality?: number;
}

export interface OvnStats {
  collection:     string;
  totalLive:      bigint;
  totalDead:      bigint;
  segmentCount:   number;
  totalFileSize:  number;
  fragmentRatio:  number;
  cacheSize:      number;
  cacheHitRate:   number;
  indexCount:     number;
  walPending:     number;
  bufferPoolUsed: number;
}

export interface IndexDefinition {
  field:    string;
  unique:   boolean;
  sparse?:  boolean;
  partial?: QueryFilter;
}

export type ChangeOperationType = 'insert' | 'update' | 'delete' | 'drop';

export interface ChangeEvent<T extends OvnDocument = OvnDocument> {
  operationType:  ChangeOperationType;
  documentKey:    { _id: string };
  fullDocument?:  T;
  updateDescription?: {
    updatedFields:  Record<string, unknown>;
    removedFields:  string[];
  };
  timestamp:      number;
  txId:           bigint;
}

export type TxStatus = 'pending' | 'committed' | 'rolled_back' | 'failed';

export interface TxSnapshot {
  txId:         bigint;
  startTime:    number;
  visibleTxIds: Set<bigint>;
}

// ── feat: BulkWrite ──────────────────────────────────────────

export type BulkWriteOp<T extends OvnDocument = OvnDocument> =
  | { op: 'insertOne';  doc: Omit<T, '_id'> & { _id?: string } }
  | { op: 'updateOne';  filter: QueryFilter; spec: UpdateSpec }
  | { op: 'updateMany'; filter: QueryFilter; spec: UpdateSpec }
  | { op: 'deleteOne';  filter: QueryFilter }
  | { op: 'deleteMany'; filter: QueryFilter }
  | { op: 'upsertOne';  filter: QueryFilter; spec: UpdateSpec }
  | { op: 'replaceOne'; filter: QueryFilter; replacement: Omit<T, '_id'> };

export interface BulkWriteResult {
  ops:           number;
  insertedCount: number;
  updatedCount:  number;
  deletedCount:  number;
  upsertedCount: number;
  replacedCount: number;
  insertedIds:   string[];
  errors:        Array<{ index: number; error: string }>;
}

// ── feat: DB-level status ────────────────────────────────────

export interface CollectionStatus {
  name:          string;
  totalLive:     bigint;
  totalDead:     bigint;
  segmentCount:  number;
  totalFileSize: number;
  fragmentRatio: number;
  encrypted:     boolean;
  indexCount:    number;
  cacheHitRate:  number;
}

export interface DBStatus {
  path:        string;
  openedAt:    number;
  collections: CollectionStatus[];
  totalSize:   number;
  isHealthy:   boolean;
}

export interface CursorOptions<T extends OvnDocument = OvnDocument> {
  filter?:     QueryFilter;
  sort?:       Record<string, 1 | -1>;
  projection?: Record<string, 0 | 1>;
  batchSize?:  number;
}
