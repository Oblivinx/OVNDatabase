// ============================================================
//  OvnDB v2.0 — SchemaValidator
//
//  Validasi dokumen sebelum masuk storage. Tidak ada dependency
//  eksternal — pure TypeScript.
//
//  Fitur:
//   - Type checking (string, number, boolean, array, object, any)
//   - Required fields
//   - Nullable fields
//   - String: minLength, maxLength, pattern (RegExp), enum
//   - Number: min, max, integer
//   - Array: minItems, maxItems, items schema (per-element validation)
//   - Object: properties (nested schema validation)
//   - Custom validator function
//
//  Dua mode:
//   - Soft: validate() → { ok, errors } — tidak throw
//   - Hard: validateOrThrow() → throw ValidationError jika gagal
//
//  Helper `field()` untuk TypeScript-idiomatic schema definition:
//
//    const schema = new SchemaValidator({
//      name:   field('string').required().maxLength(100).build(),
//      phone:  field('string').required().pattern(/^628\d{8,12}$/).build(),
//      points: field('number').min(0).default(0).build(),
//      tags:   field('array').maxItems(20).items(field('string').build()).build(),
//    });
// ============================================================
import { makeLogger } from '../utils/logger.js';
import { validateRegex } from '../utils/security.js';
const log = makeLogger('schema');
// ── SchemaValidator ───────────────────────────────────────────
export class SchemaValidator {
    schema;
    constructor(schema) {
        this.schema = schema;
        log.debug(`Schema dibuat — ${Object.keys(schema).length} field`);
    }
    /**
     * Validasi dokumen. Tidak melempar exception.
     * @returns { ok, errors } — ok = false jika ada error
     */
    validate(doc) {
        const errors = [];
        for (const [field, rule] of Object.entries(this.schema)) {
            errors.push(...this._validateField(field, doc[field], rule));
        }
        return { ok: errors.length === 0, errors };
    }
    /**
     * Validasi dan throw ValidationError jika gagal.
     * Gunakan ini di insertOne/updateOne untuk strict validation.
     */
    validateOrThrow(doc, context = 'document') {
        const { ok, errors } = this.validate(doc);
        if (!ok)
            throw new ValidationError(`[OvnDB] Validasi ${context} gagal:\n` + errors.map(e => `  • ${e}`).join('\n'), errors);
    }
    /**
     * Terapkan default values ke dokumen (in-place).
     * Panggil ini sebelum validate() agar field dengan default tidak dianggap missing.
     */
    applyDefaults(doc) {
        for (const [field, rule] of Object.entries(this.schema)) {
            if (doc[field] === undefined && rule.default !== undefined) {
                doc[field] = typeof rule.default === 'function'
                    ? rule.default()
                    : rule.default;
            }
        }
    }
    // ── Privates ──────────────────────────────────────────────
    _validateField(fieldPath, val, rule) {
        const errors = [];
        // Null/undefined handling
        if (val === null) {
            if (!rule.nullable)
                errors.push(`"${fieldPath}" tidak boleh null`);
            return errors;
        }
        if (val === undefined) {
            if (rule.required)
                errors.push(`"${fieldPath}" wajib diisi`);
            return errors; // tidak validasi lebih lanjut jika kosong
        }
        if (rule.type === 'any')
            return errors; // skip validasi untuk 'any'
        // Type checking
        const actualType = Array.isArray(val) ? 'array' : typeof val;
        if (actualType !== rule.type) {
            errors.push(`"${fieldPath}" harus bertipe ${rule.type}, got ${actualType}`);
            return errors; // type salah → tidak perlu validasi lebih lanjut
        }
        // String validations
        if (rule.type === 'string' && typeof val === 'string') {
            if (rule.minLength !== undefined && val.length < rule.minLength)
                errors.push(`"${fieldPath}" minimal ${rule.minLength} karakter (got ${val.length})`);
            if (rule.maxLength !== undefined && val.length > rule.maxLength)
                errors.push(`"${fieldPath}" maksimal ${rule.maxLength} karakter (got ${val.length})`);
            if (rule.pattern) {
                // SECURITY: validasi pattern sebelum dikompilasi — cegah ReDoS dari schema definition
                // Schema biasanya dikonfigurasi developer, bukan user, tapi defence-in-depth.
                if (typeof rule.pattern === 'string')
                    validateRegex(rule.pattern);
                const re = rule.pattern instanceof RegExp ? rule.pattern : new RegExp(rule.pattern);
                if (!re.test(val))
                    errors.push(`"${fieldPath}" tidak cocok dengan pattern ${re}`);
            }
            if (rule.enum && !rule.enum.includes(val))
                errors.push(`"${fieldPath}" harus salah satu dari: ${rule.enum.join(', ')}`);
        }
        // Number validations
        if (rule.type === 'number' && typeof val === 'number') {
            if (rule.integer && !Number.isInteger(val))
                errors.push(`"${fieldPath}" harus bilangan bulat`);
            if (rule.min !== undefined && val < rule.min)
                errors.push(`"${fieldPath}" minimal ${rule.min} (got ${val})`);
            if (rule.max !== undefined && val > rule.max)
                errors.push(`"${fieldPath}" maksimal ${rule.max} (got ${val})`);
        }
        // Array validations
        if (rule.type === 'array' && Array.isArray(val)) {
            if (rule.minItems !== undefined && val.length < rule.minItems)
                errors.push(`"${fieldPath}" minimal ${rule.minItems} item (got ${val.length})`);
            if (rule.maxItems !== undefined && val.length > rule.maxItems)
                errors.push(`"${fieldPath}" maksimal ${rule.maxItems} item (got ${val.length})`);
            if (rule.items) {
                val.forEach((elem, i) => {
                    errors.push(...this._validateField(`${fieldPath}[${i}]`, elem, rule.items));
                });
            }
        }
        // Object validations (nested schema)
        if (rule.type === 'object' && typeof val === 'object' && !Array.isArray(val)) {
            if (rule.properties) {
                const nested = new SchemaValidator(rule.properties);
                const result = nested.validate(val);
                errors.push(...result.errors.map(e => `${fieldPath}.${e}`));
            }
        }
        // Custom validator
        if (rule.custom) {
            const msg = rule.custom(val);
            if (msg)
                errors.push(`"${fieldPath}": ${msg}`);
        }
        return errors;
    }
}
export class ValidationError extends Error {
    errors;
    constructor(message, errors) {
        super(message);
        this.name = 'ValidationError';
        this.errors = errors;
    }
}
// ── Fluent Builder helper ─────────────────────────────────────
/**
 * Fluent builder untuk FieldSchema — TypeScript-idiomatic API.
 *
 * @example
 *   field('string').required().maxLength(100).build()
 *   field('number').min(0).max(100).build()
 *   field('array').items(field('string').build()).maxItems(10).build()
 */
export function field(type) {
    return new FieldBuilder(type);
}
class FieldBuilder {
    _schema;
    constructor(type) {
        this._schema = { type };
    }
    required() { this._schema.required = true; return this; }
    nullable() { this._schema.nullable = true; return this; }
    default(val) { this._schema.default = val; return this; }
    minLength(n) { this._schema.minLength = n; return this; }
    maxLength(n) { this._schema.maxLength = n; return this; }
    pattern(re) { this._schema.pattern = re; return this; }
    enum(...vals) { this._schema.enum = vals; return this; }
    min(n) { this._schema.min = n; return this; }
    max(n) { this._schema.max = n; return this; }
    integer() { this._schema.integer = true; return this; }
    minItems(n) { this._schema.minItems = n; return this; }
    maxItems(n) { this._schema.maxItems = n; return this; }
    items(schema) { this._schema.items = schema; return this; }
    properties(def) { this._schema.properties = def; return this; }
    custom(fn) { this._schema.custom = fn; return this; }
    build() { return { ...this._schema }; }
}
//# sourceMappingURL=schema-validator.js.map