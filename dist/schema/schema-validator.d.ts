export type FieldType = 'string' | 'number' | 'boolean' | 'array' | 'object' | 'any';
export interface FieldSchema {
    type: FieldType;
    required?: boolean;
    nullable?: boolean;
    default?: unknown;
    minLength?: number;
    maxLength?: number;
    pattern?: RegExp | string;
    enum?: string[];
    min?: number;
    max?: number;
    integer?: boolean;
    minItems?: number;
    maxItems?: number;
    items?: FieldSchema;
    properties?: SchemaDefinition;
    custom?: (val: unknown) => string | null;
}
export type SchemaDefinition = Record<string, FieldSchema>;
export interface ValidationResult {
    ok: boolean;
    errors: string[];
}
export declare class SchemaValidator {
    private readonly schema;
    constructor(schema: SchemaDefinition);
    /**
     * Validasi dokumen. Tidak melempar exception.
     * @returns { ok, errors } — ok = false jika ada error
     */
    validate(doc: Record<string, unknown>): ValidationResult;
    /**
     * Validasi dan throw ValidationError jika gagal.
     * Gunakan ini di insertOne/updateOne untuk strict validation.
     */
    validateOrThrow(doc: Record<string, unknown>, context?: string): void;
    /**
     * Terapkan default values ke dokumen (in-place).
     * Panggil ini sebelum validate() agar field dengan default tidak dianggap missing.
     */
    applyDefaults(doc: Record<string, unknown>): void;
    private _validateField;
}
export declare class ValidationError extends Error {
    readonly errors: string[];
    constructor(message: string, errors: string[]);
}
/**
 * Fluent builder untuk FieldSchema — TypeScript-idiomatic API.
 *
 * @example
 *   field('string').required().maxLength(100).build()
 *   field('number').min(0).max(100).build()
 *   field('array').items(field('string').build()).maxItems(10).build()
 */
export declare function field(type: FieldType): FieldBuilder;
declare class FieldBuilder {
    private _schema;
    constructor(type: FieldType);
    required(): this;
    nullable(): this;
    default(val: unknown): this;
    minLength(n: number): this;
    maxLength(n: number): this;
    pattern(re: RegExp | string): this;
    enum(...vals: string[]): this;
    min(n: number): this;
    max(n: number): this;
    integer(): this;
    minItems(n: number): this;
    maxItems(n: number): this;
    items(schema: FieldSchema): this;
    properties(def: SchemaDefinition): this;
    custom(fn: (v: unknown) => string | null): this;
    build(): FieldSchema;
}
export {};
//# sourceMappingURL=schema-validator.d.ts.map