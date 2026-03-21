// ============================================================
//  SchemaValidator — Validasi dokumen sebelum masuk ke storage
//
//  API:
//    const schema = new SchemaValidator({
//      name:   { type: 'string',  required: true,  maxLength: 100 },
//      phone:  { type: 'string',  required: true,  pattern: /^628\d{8,12}$/ },
//      points: { type: 'number',  required: false, min: 0, max: 1_000_000 },
//      active: { type: 'boolean', required: true },
//      tags:   { type: 'array',   required: false },
//      meta:   { type: 'object',  required: false },
//    });
//
//    const result = schema.validate(doc);
//    if (!result.ok) console.error(result.errors);
//
//  Design:
//    - Tidak ada dependency eksternal, murni TypeScript
//    - Fast path: jika tidak ada schema → langsung pass
//    - Soft mode: validate() mengembalikan errors (tidak throw)
//    - Hard mode: validateOrThrow() — throw ValidationError
// ============================================================
import { makeLogger } from '../utils/logger.js';

const log = makeLogger('schema');

// ── Field descriptor ──────────────────────────────────────────
export type FieldType = 'string' | 'number' | 'boolean' | 'array' | 'object' | 'any';

export interface FieldSchema {
  type:       FieldType;
  required?:  boolean;           // default: false
  nullable?:  boolean;           // allow null (default: false)

  // String options
  minLength?: number;
  maxLength?: number;
  pattern?:  RegExp | string;
  enum?:     string[];

  // Number options
  min?:      number;
  max?:      number;
  integer?:  boolean;            // harus bilangan bulat

  // Array options
  minItems?: number;
  maxItems?: number;
  items?:    FieldSchema;        // validasi tiap elemen

  // Nested object
  properties?: SchemaDefinition;

  // Custom validator
  custom?:   (val: unknown) => string | null; // return null = OK, string = error message
}

export type SchemaDefinition = Record<string, FieldSchema>;

export interface ValidationResult {
  ok:     boolean;
  errors: string[];
}

// ── Validator ─────────────────────────────────────────────────
export class SchemaValidator {
  private readonly schema: SchemaDefinition;

  constructor(schema: SchemaDefinition) {
    this.schema = schema;
    log.debug(`Schema dibuat dengan ${Object.keys(schema).length} field`);
  }

  /**
   * Validasi dokumen. Mengembalikan { ok, errors }.
   * Tidak melempar exception.
   */
  validate(doc: Record<string, unknown>): ValidationResult {
    const errors: string[] = [];

    for (const [field, rule] of Object.entries(this.schema)) {
      const val    = doc[field];
      const errs   = this._validateField(field, val, rule);
      errors.push(...errs);
    }

    return { ok: errors.length === 0, errors };
  }

  /**
   * Validasi dan lempar ValidationError jika gagal.
   * Gunakan ini di insertOne/updateOne untuk strict mode.
   */
  validateOrThrow(doc: Record<string, unknown>, context = 'document'): void {
    const result = this.validate(doc);
    if (!result.ok) {
      throw new ValidationError(
        `[OvnDB] Validasi ${context} gagal:\n` +
        result.errors.map(e => `  • ${e}`).join('\n'),
        result.errors,
      );
    }
  }

  /**
   * Hanya validasi field-field yang ada di partial (untuk update).
   * Field yang tidak ada di partial di-skip.
   */
  validatePartial(partial: Record<string, unknown>): ValidationResult {
    const errors: string[] = [];

    for (const [field, val] of Object.entries(partial)) {
      const rule = this.schema[field];
      if (!rule) continue; // field tidak dikenali schema — skip (bukan error)
      const errs = this._validateField(field, val, { ...rule, required: false });
      errors.push(...errs);
    }

    return { ok: errors.length === 0, errors };
  }

  // ── Internals ─────────────────────────────────────────────

  private _validateField(field: string, val: unknown, rule: FieldSchema): string[] {
    const errors: string[] = [];

    // Null / undefined check
    if (val === undefined || val === null) {
      if (val === null && rule.nullable) return []; // null diizinkan
      if (rule.required) {
        errors.push(`Field "${field}" wajib diisi`);
      }
      return errors; // kalau tidak required, field boleh tidak ada
    }

    // Type check
    if (rule.type !== 'any') {
      const typeErr = this._checkType(field, val, rule.type);
      if (typeErr) {
        errors.push(typeErr);
        return errors; // type salah → skip rule turunannya
      }
    }

    // String-specific rules
    if (rule.type === 'string' && typeof val === 'string') {
      if (rule.minLength !== undefined && val.length < rule.minLength)
        errors.push(`Field "${field}" minimal ${rule.minLength} karakter (saat ini: ${val.length})`);
      if (rule.maxLength !== undefined && val.length > rule.maxLength)
        errors.push(`Field "${field}" maksimal ${rule.maxLength} karakter (saat ini: ${val.length})`);
      if (rule.pattern) {
        const rx = rule.pattern instanceof RegExp ? rule.pattern : new RegExp(rule.pattern);
        if (!rx.test(val))
          errors.push(`Field "${field}" tidak sesuai pola "${rule.pattern}"`);
      }
      if (rule.enum && !rule.enum.includes(val))
        errors.push(`Field "${field}" harus salah satu dari: ${rule.enum.map(e => `"${e}"`).join(', ')}`);
    }

    // Number-specific rules
    if (rule.type === 'number' && typeof val === 'number') {
      if (rule.min !== undefined && val < rule.min)
        errors.push(`Field "${field}" minimal ${rule.min} (saat ini: ${val})`);
      if (rule.max !== undefined && val > rule.max)
        errors.push(`Field "${field}" maksimal ${rule.max} (saat ini: ${val})`);
      if (rule.integer && !Number.isInteger(val))
        errors.push(`Field "${field}" harus bilangan bulat (saat ini: ${val})`);
      if (!Number.isFinite(val))
        errors.push(`Field "${field}" harus angka valid (tidak boleh NaN atau Infinity)`);
    }

    // Array-specific rules
    if (rule.type === 'array' && Array.isArray(val)) {
      if (rule.minItems !== undefined && val.length < rule.minItems)
        errors.push(`Field "${field}" minimal ${rule.minItems} item (saat ini: ${val.length})`);
      if (rule.maxItems !== undefined && val.length > rule.maxItems)
        errors.push(`Field "${field}" maksimal ${rule.maxItems} item (saat ini: ${val.length})`);
      if (rule.items) {
        val.forEach((item, i) => {
          const itemErrs = this._validateField(`${field}[${i}]`, item, rule.items!);
          errors.push(...itemErrs);
        });
      }
    }

    // Object-specific rules (nested schema)
    if (rule.type === 'object' && rule.properties && typeof val === 'object' && val !== null && !Array.isArray(val)) {
      const nested = new SchemaValidator(rule.properties);
      const result = nested.validate(val as Record<string, unknown>);
      errors.push(...result.errors.map(e => `${field}.${e}`));
    }

    // Custom validator
    if (rule.custom) {
      const customErr = rule.custom(val);
      if (customErr) errors.push(`Field "${field}": ${customErr}`);
    }

    return errors;
  }

  private _checkType(field: string, val: unknown, type: FieldType): string | null {
    switch (type) {
      case 'string':  return typeof val === 'string'  ? null : `Field "${field}" harus string (dapat: ${typeof val})`;
      case 'number':  return typeof val === 'number'  ? null : `Field "${field}" harus number (dapat: ${typeof val})`;
      case 'boolean': return typeof val === 'boolean' ? null : `Field "${field}" harus boolean (dapat: ${typeof val})`;
      case 'array':   return Array.isArray(val)       ? null : `Field "${field}" harus array (dapat: ${typeof val})`;
      case 'object':
        return (typeof val === 'object' && val !== null && !Array.isArray(val))
          ? null
          : `Field "${field}" harus object (dapat: ${Array.isArray(val) ? 'array' : typeof val})`;
      default: return null;
    }
  }
}

// ── Custom error ──────────────────────────────────────────────
export class ValidationError extends Error {
  constructor(
    message: string,
    public readonly validationErrors: string[],
  ) {
    super(message);
    this.name = 'ValidationError';
  }
}

// ── Preset schema factories ───────────────────────────────────

/**
 * Helper untuk membuat field schema umum dengan cepat.
 *
 * Contoh:
 *   const schema = new SchemaValidator({
 *     name:  field.string({ required: true, maxLength: 100 }),
 *     phone: field.string({ required: true, pattern: /^628\d+$/ }),
 *     age:   field.number({ min: 0, max: 150, integer: true }),
 *   });
 */
export const field = {
  string: (opts: Omit<FieldSchema, 'type'> = {}): FieldSchema => ({ type: 'string', ...opts }),
  number: (opts: Omit<FieldSchema, 'type'> = {}): FieldSchema => ({ type: 'number', ...opts }),
  boolean:(opts: Omit<FieldSchema, 'type'> = {}): FieldSchema => ({ type: 'boolean', ...opts }),
  array:  (opts: Omit<FieldSchema, 'type'> = {}): FieldSchema => ({ type: 'array',  ...opts }),
  object: (opts: Omit<FieldSchema, 'type'> = {}): FieldSchema => ({ type: 'object', ...opts }),
  any:    (opts: Omit<FieldSchema, 'type'> = {}): FieldSchema => ({ type: 'any',    ...opts }),

  /** Phone Indonesia: harus dimulai dengan 628 */
  phoneID: (opts: Omit<FieldSchema, 'type' | 'pattern'> = {}): FieldSchema => ({
    type: 'string', pattern: /^628\d{7,13}$/, ...opts,
    custom: (v) => {
      if (typeof v !== 'string') return null;
      if (v.length < 10 || v.length > 16) return `Nomor HP tidak valid (panjang: ${v.length})`;
      return null;
    },
  }),

  /** Timestamp: harus angka positif */
  timestamp: (opts: Omit<FieldSchema, 'type' | 'min'> = {}): FieldSchema => ({
    type: 'number', min: 0, integer: true, ...opts,
  }),
};
