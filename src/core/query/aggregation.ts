// ============================================================
//  OvnDB v2.0 — Aggregation Pipeline
//
//  MongoDB-compatible aggregation stages:
//   $match    — filter dokumen (sama seperti find filter)
//   $project  — reshape dokumen, include/exclude/rename/compute field
//   $group    — group by key, compute $sum $avg $min $max $count $push $first $last
//   $sort     — sort hasil
//   $limit    — batasi jumlah dokumen
//   $skip     — lewati N dokumen pertama
//   $unwind   — expand array field → satu dokumen per elemen
//   $lookup   — left join dengan collection lain
//   $count    — hitung total dokumen, simpan ke field
//   $addFields — tambah field baru ke dokumen
//   $replaceRoot — ganti root dokumen
//
//  Setiap stage adalah pure function yang menerima dan mengembalikan
//  array dokumen → mudah di-compose, mudah di-test.
// ============================================================

import type {
  AggregationStage, OvnDocument, QueryFilter,
} from '../../types/index.js';
import { matchFilter, applyProjection, getFieldValue, setNestedField } from './filter.js';

export type AggFn = (docs: Record<string, unknown>[]) => Record<string, unknown>[];

/**
 * Compile pipeline (array of stages) menjadi satu fungsi yang bisa
 * dijalankan terhadap array dokumen.
 *
 * @param pipeline   Array of aggregation stages
 * @param lookupResolver  Fungsi untuk resolve $lookup (collection name → docs)
 */
export function compilePipeline(
  pipeline: AggregationStage[],
  lookupResolver?: (collection: string) => Promise<Record<string, unknown>[]>,
): (docs: Record<string, unknown>[]) => Promise<Record<string, unknown>[]> {
  return async (docs: Record<string, unknown>[]) => {
    let result = docs;
    for (const stage of pipeline) {
      result = await applyStage(stage, result, lookupResolver);
    }
    return result;
  };
}

async function applyStage(
  stage: AggregationStage,
  docs: Record<string, unknown>[],
  lookupResolver?: (c: string) => Promise<Record<string, unknown>[]>,
): Promise<Record<string, unknown>[]> {

  if ('$match' in stage) {
    return docs.filter(d => matchFilter(d, stage.$match as QueryFilter));
  }

  if ('$project' in stage) {
    return docs.map(d => applyProjectStage(d, stage.$project as Record<string, unknown>));
  }

  if ('$addFields' in stage) {
    return docs.map(d => applyAddFields(d, stage.$addFields as Record<string, unknown>));
  }

  if ('$group' in stage) {
    return applyGroup(docs, stage.$group as Record<string, unknown>);
  }

  if ('$sort' in stage) {
    return applySort(docs, stage.$sort as Record<string, 1 | -1>);
  }

  if ('$limit' in stage) {
    return docs.slice(0, stage.$limit as number);
  }

  if ('$skip' in stage) {
    return docs.slice(stage.$skip as number);
  }

  if ('$unwind' in stage) {
    return applyUnwind(docs, (stage as Record<string, unknown>).$unwind as string | { path: string; preserveNullAndEmptyArrays?: boolean });
  }

  if ('$count' in stage) {
    return [{ [stage.$count as string]: docs.length }];
  }

  if ('$replaceRoot' in stage) {
    const spec = stage.$replaceRoot as { newRoot: unknown };
    return docs.map(d => {
      if (typeof spec.newRoot === 'string') {
        return (getFieldValue(d, spec.newRoot.replace('$', '')) ?? {}) as Record<string, unknown>;
      }
      return spec.newRoot as Record<string, unknown>;
    });
  }

  if ('$lookup' in stage && lookupResolver) {
    return applyLookup(docs, stage.$lookup as {
      from: string; localField: string; foreignField: string; as: string;
    }, lookupResolver);
  }

  return docs;
}

// ── $project ─────────────────────────────────────────────────

function applyProjectStage(
  doc: Record<string, unknown>,
  spec: Record<string, unknown>,
): Record<string, unknown> {
  const result: Record<string, unknown> = {};

  for (const [field, value] of Object.entries(spec)) {
    if (value === 0) continue; // exclude

    if (value === 1) {
      // Simple include
      if (field in doc) result[field] = doc[field];
    } else if (typeof value === 'string' && value.startsWith('$')) {
      // Field reference: { newName: '$oldName' }
      result[field] = getFieldValue(doc, value.slice(1));
    } else if (typeof value === 'object' && value !== null) {
      // Expression: { field: { $expr... } }
      result[field] = evaluateExpression(value as Record<string, unknown>, doc);
    } else {
      result[field] = value;
    }
  }

  // Selalu include _id kecuali eksplisit di-exclude
  if (spec['_id'] !== 0 && '_id' in doc) result['_id'] = doc['_id'];
  return result;
}

// ── $addFields ───────────────────────────────────────────────

function applyAddFields(
  doc: Record<string, unknown>,
  spec: Record<string, unknown>,
): Record<string, unknown> {
  const result = { ...doc };
  for (const [field, value] of Object.entries(spec)) {
    if (typeof value === 'string' && value.startsWith('$')) {
      setNestedField(result, field, getFieldValue(doc, value.slice(1)));
    } else if (typeof value === 'object' && value !== null) {
      setNestedField(result, field, evaluateExpression(value as Record<string, unknown>, doc));
    } else {
      setNestedField(result, field, value);
    }
  }
  return result;
}

// ── $group ───────────────────────────────────────────────────

function applyGroup(
  docs: Record<string, unknown>[],
  spec: Record<string, unknown>,
): Record<string, unknown>[] {
  const groupKey = spec['_id'];
  const groups   = new Map<string, {
    key:  unknown;
    docs: Record<string, unknown>[];
  }>();

  for (const doc of docs) {
    let keyVal: unknown;
    if (typeof groupKey === 'string' && groupKey.startsWith('$')) {
      keyVal = getFieldValue(doc, groupKey.slice(1));
    } else if (typeof groupKey === 'object' && groupKey !== null) {
      // Compound group key: { city: '$city', status: '$status' }
      const compound: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(groupKey as Record<string, unknown>)) {
        compound[k] = typeof v === 'string' && v.startsWith('$')
          ? getFieldValue(doc, v.slice(1)) : v;
      }
      keyVal = compound;
    } else {
      keyVal = groupKey; // null = group semua
    }

    const serialized = JSON.stringify(keyVal);
    if (!groups.has(serialized)) {
      groups.set(serialized, { key: keyVal, docs: [] });
    }
    groups.get(serialized)!.docs.push(doc);
  }

  return [...groups.values()].map(({ key, docs: grpDocs }) => {
    const out: Record<string, unknown> = { _id: key };
    for (const [field, expr] of Object.entries(spec)) {
      if (field === '_id') continue;
      out[field] = evaluateAccumulator(expr as Record<string, unknown>, grpDocs);
    }
    return out;
  });
}

function evaluateAccumulator(
  expr: Record<string, unknown>,
  docs: Record<string, unknown>[],
): unknown {
  if ('$sum' in expr) {
    const field = expr.$sum;
    if (typeof field === 'number') return field * docs.length; // $sum: 1 → count
    const path = typeof field === 'string' && field.startsWith('$') ? field.slice(1) : null;
    return docs.reduce((s, d) => {
      const v = path ? getFieldValue(d, path) : 0;
      return s + (typeof v === 'number' ? v : 0);
    }, 0);
  }
  if ('$avg' in expr) {
    const path  = ((expr.$avg as string) ?? '').slice(1);
    const nums  = docs.map(d => getFieldValue(d, path)).filter(v => typeof v === 'number') as number[];
    return nums.length ? nums.reduce((s, v) => s + v, 0) / nums.length : null;
  }
  if ('$min' in expr) {
    const path = ((expr.$min as string) ?? '').slice(1);
    const vals = docs.map(d => getFieldValue(d, path)).filter(v => v !== undefined) as number[];
    return vals.length ? Math.min(...vals) : null;
  }
  if ('$max' in expr) {
    const path = ((expr.$max as string) ?? '').slice(1);
    const vals = docs.map(d => getFieldValue(d, path)).filter(v => v !== undefined) as number[];
    return vals.length ? Math.max(...vals) : null;
  }
  if ('$count' in expr) {
    return docs.length;
  }
  if ('$push' in expr) {
    const field = expr.$push;
    const path  = typeof field === 'string' && field.startsWith('$') ? field.slice(1) : null;
    return path ? docs.map(d => getFieldValue(d, path)) : docs.map(() => field);
  }
  if ('$first' in expr) {
    const path = ((expr.$first as string) ?? '').slice(1);
    return docs.length ? getFieldValue(docs[0]!, path) : null;
  }
  if ('$last' in expr) {
    const path = ((expr.$last as string) ?? '').slice(1);
    return docs.length ? getFieldValue(docs[docs.length - 1]!, path) : null;
  }
  if ('$addToSet' in expr) {
    const path = ((expr.$addToSet as string) ?? '').slice(1);
    const set  = new Set(docs.map(d => JSON.stringify(getFieldValue(d, path))));
    return [...set].map(v => JSON.parse(v));
  }
  return null;
}

// ── $sort ────────────────────────────────────────────────────

function applySort(docs: Record<string, unknown>[], sort: Record<string, 1 | -1>): Record<string, unknown>[] {
  const entries = Object.entries(sort);
  return [...docs].sort((a, b) => {
    for (const [field, dir] of entries) {
      const av = getFieldValue(a, field);
      const bv = getFieldValue(b, field);
      let cmp  = 0;
      if (av === null || av === undefined) cmp = -1;
      else if (bv === null || bv === undefined) cmp = 1;
      else if ((av as number) < (bv as number)) cmp = -1;
      else if ((av as number) > (bv as number)) cmp = 1;
      if (cmp !== 0) return cmp * dir;
    }
    return 0;
  });
}

// ── $unwind ──────────────────────────────────────────────────

function applyUnwind(
  docs: Record<string, unknown>[],
  spec: string | { path: string; preserveNullAndEmptyArrays?: boolean },
): Record<string, unknown>[] {
  const path    = typeof spec === 'string' ? spec : spec.path;
  const preserve = typeof spec === 'object' ? (spec.preserveNullAndEmptyArrays ?? false) : false;
  const field   = path.startsWith('$') ? path.slice(1) : path;
  const result: Record<string, unknown>[] = [];

  for (const doc of docs) {
    const val = getFieldValue(doc, field);
    if (!Array.isArray(val) || val.length === 0) {
      if (preserve) result.push({ ...doc, [field]: val ?? null });
      continue;
    }
    for (const elem of val) {
      result.push({ ...doc, [field]: elem });
    }
  }
  return result;
}

// ── $lookup ──────────────────────────────────────────────────

async function applyLookup(
  docs: Record<string, unknown>[],
  spec: { from: string; localField: string; foreignField: string; as: string },
  resolver: (c: string) => Promise<Record<string, unknown>[]>,
): Promise<Record<string, unknown>[]> {
  const foreignDocs = await resolver(spec.from);
  const index       = new Map<unknown, Record<string, unknown>[]>();

  for (const fd of foreignDocs) {
    const key = getFieldValue(fd, spec.foreignField);
    const serialized = JSON.stringify(key);
    if (!index.has(serialized)) index.set(serialized, []);
    index.get(serialized)!.push(fd);
  }

  return docs.map(doc => {
    const localVal  = getFieldValue(doc, spec.localField);
    const matched   = index.get(JSON.stringify(localVal)) ?? [];
    return { ...doc, [spec.as]: matched };
  });
}

// ── Expression Evaluator ─────────────────────────────────────

function evaluateExpression(
  expr: Record<string, unknown>,
  doc: Record<string, unknown>,
): unknown {
  if ('$concat' in expr) {
    return (expr.$concat as unknown[]).map(part =>
      typeof part === 'string' && part.startsWith('$')
        ? String(getFieldValue(doc, part.slice(1)) ?? '')
        : String(part)
    ).join('');
  }
  if ('$add' in expr) {
    return (expr.$add as unknown[]).reduce<number>((sum, part) => {
      const v = typeof part === 'string' && part.startsWith('$')
        ? getFieldValue(doc, part.slice(1)) : part;
      return sum + (typeof v === 'number' ? v : 0);
    }, 0);
  }
  if ('$subtract' in expr) {
    const [a, b] = expr.$subtract as [unknown, unknown];
    const av = typeof a === 'string' && a.startsWith('$') ? getFieldValue(doc, a.slice(1)) : a;
    const bv = typeof b === 'string' && b.startsWith('$') ? getFieldValue(doc, b.slice(1)) : b;
    return (av as number) - (bv as number);
  }
  if ('$multiply' in expr) {
    return (expr.$multiply as unknown[]).reduce<number>((prod, part) => {
      const v = typeof part === 'string' && part.startsWith('$')
        ? getFieldValue(doc, part.slice(1)) : part;
      return prod * (typeof v === 'number' ? v : 1);
    }, 1);
  }
  if ('$cond' in expr) {
    const cond = expr.$cond as { if: QueryFilter; then: unknown; else: unknown };
    const test = matchFilter(doc, cond.if as QueryFilter);
    return test ? cond.then : cond.else;
  }
  if ('$ifNull' in expr) {
    const [field, fallback] = expr.$ifNull as [string, unknown];
    const val = typeof field === 'string' && field.startsWith('$')
      ? getFieldValue(doc, field.slice(1)) : field;
    return val ?? fallback;
  }
  return null;
}
