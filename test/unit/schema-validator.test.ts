import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SchemaValidator, ValidationError, field } from '../../src/schema/schema-validator.js';

describe('SchemaValidator — type checking', () => {
  const schema = new SchemaValidator({
    name:   { type: 'string' },
    age:    { type: 'number' },
    active: { type: 'boolean' },
    tags:   { type: 'array' },
    meta:   { type: 'object' },
    any:    { type: 'any' },
  });

  it('accepts correct types', () => {
    const { ok } = schema.validate({ name: 'Alice', age: 30, active: true, tags: [], meta: {}, any: 42 });
    assert.equal(ok, true);
  });

  it('rejects wrong type', () => {
    const { ok, errors } = schema.validate({ name: 123 });
    assert.equal(ok, false);
    assert.ok(errors.some(e => e.includes('string')));
  });

  it('allows any type for "any" field', () => {
    const { ok } = schema.validate({ any: { nested: true } });
    assert.equal(ok, true);
  });
});

describe('SchemaValidator — required / nullable', () => {
  const schema = new SchemaValidator({
    name: { type: 'string', required: true },
    bio:  { type: 'string', nullable: true },
  });

  it('rejects missing required field', () => {
    const { ok, errors } = schema.validate({});
    assert.equal(ok, false);
    assert.ok(errors.some(e => e.includes('wajib')));
  });

  it('accepts null for nullable field', () => {
    const { ok } = schema.validate({ name: 'Alice', bio: null });
    assert.equal(ok, true);
  });

  it('rejects null for non-nullable field', () => {
    const { ok, errors } = schema.validate({ name: null });
    assert.equal(ok, false);
    assert.ok(errors.some(e => e.includes('null')));
  });
});

describe('SchemaValidator — string constraints', () => {
  const schema = new SchemaValidator({
    name:  { type: 'string', minLength: 2, maxLength: 50 },
    role:  { type: 'string', enum: ['admin', 'user'] },
    phone: { type: 'string', pattern: /^628\d+$/ },
  });

  it('rejects too short string', () => {
    const { ok } = schema.validate({ name: 'A' });
    assert.equal(ok, false);
  });

  it('rejects too long string', () => {
    const { ok } = schema.validate({ name: 'x'.repeat(51) });
    assert.equal(ok, false);
  });

  it('rejects value not in enum', () => {
    const { ok } = schema.validate({ role: 'superadmin' });
    assert.equal(ok, false);
  });

  it('rejects pattern mismatch', () => {
    const { ok } = schema.validate({ phone: '081234' });
    assert.equal(ok, false);
  });

  it('accepts valid phone', () => {
    const { ok } = schema.validate({ phone: '628123456789' });
    assert.equal(ok, true);
  });
});

describe('SchemaValidator — number constraints', () => {
  const schema = new SchemaValidator({
    age:   { type: 'number', min: 0, max: 150 },
    level: { type: 'number', integer: true },
  });

  it('rejects below min', () => {
    const { ok } = schema.validate({ age: -1 });
    assert.equal(ok, false);
  });

  it('rejects above max', () => {
    const { ok } = schema.validate({ age: 200 });
    assert.equal(ok, false);
  });

  it('rejects non-integer', () => {
    const { ok } = schema.validate({ level: 1.5 });
    assert.equal(ok, false);
  });

  it('accepts valid integer', () => {
    const { ok } = schema.validate({ level: 5 });
    assert.equal(ok, true);
  });
});

describe('SchemaValidator — array constraints', () => {
  const schema = new SchemaValidator({
    tags: { type: 'array', minItems: 1, maxItems: 5, items: { type: 'string' } },
  });

  it('rejects empty array when minItems = 1', () => {
    const { ok } = schema.validate({ tags: [] });
    assert.equal(ok, false);
  });

  it('rejects array exceeding maxItems', () => {
    const { ok } = schema.validate({ tags: ['a', 'b', 'c', 'd', 'e', 'f'] });
    assert.equal(ok, false);
  });

  it('rejects item of wrong type', () => {
    const { ok } = schema.validate({ tags: ['a', 42] });
    assert.equal(ok, false);
  });

  it('accepts valid array', () => {
    const { ok } = schema.validate({ tags: ['a', 'b'] });
    assert.equal(ok, true);
  });
});

describe('SchemaValidator — nested object', () => {
  const schema = new SchemaValidator({
    profile: {
      type: 'object',
      properties: {
        level: { type: 'number', min: 1, max: 10 },
      },
    },
  });

  it('validates nested properties', () => {
    const { ok } = schema.validate({ profile: { level: 0 } });
    assert.equal(ok, false);
  });

  it('accepts valid nested object', () => {
    const { ok } = schema.validate({ profile: { level: 5 } });
    assert.equal(ok, true);
  });
});

describe('SchemaValidator — defaults', () => {
  const schema = new SchemaValidator({
    points: { type: 'number', default: 0 },
    role:   { type: 'string', default: 'user' },
  });

  it('applies defaults for missing fields', () => {
    const doc: Record<string, unknown> = {};
    schema.applyDefaults(doc);
    assert.equal(doc['points'], 0);
    assert.equal(doc['role'], 'user');
  });

  it('does not overwrite existing values', () => {
    const doc: Record<string, unknown> = { points: 100 };
    schema.applyDefaults(doc);
    assert.equal(doc['points'], 100);
  });
});

describe('SchemaValidator — custom validator', () => {
  const schema = new SchemaValidator({
    even: { type: 'number', custom: (v) => (v as number) % 2 === 0 ? null : 'harus genap' },
  });

  it('rejects with custom error', () => {
    const { ok, errors } = schema.validate({ even: 3 });
    assert.equal(ok, false);
    assert.ok(errors.some(e => e.includes('genap')));
  });

  it('accepts valid value', () => {
    const { ok } = schema.validate({ even: 4 });
    assert.equal(ok, true);
  });
});

describe('SchemaValidator — validateOrThrow', () => {
  const schema = new SchemaValidator({
    name: { type: 'string', required: true },
  });

  it('throws ValidationError on invalid doc', () => {
    assert.throws(
      () => schema.validateOrThrow({}),
      (err) => err instanceof ValidationError && err.errors.length > 0,
    );
  });

  it('does not throw on valid doc', () => {
    assert.doesNotThrow(() => schema.validateOrThrow({ name: 'Alice' }));
  });
});

describe('field() fluent builder', () => {
  it('builds a complete FieldSchema', () => {
    const fs = field('string').required().minLength(2).maxLength(100).build();
    assert.equal(fs.type, 'string');
    assert.equal(fs.required, true);
    assert.equal(fs.minLength, 2);
    assert.equal(fs.maxLength, 100);
  });

  it('builds number schema', () => {
    const fs = field('number').min(0).max(100).integer().default(0).build();
    assert.equal(fs.type, 'number');
    assert.equal(fs.min, 0);
    assert.equal(fs.max, 100);
    assert.equal(fs.integer, true);
    assert.equal(fs.default, 0);
  });

  it('builds array schema with items', () => {
    const fs = field('array').maxItems(10).items(field('string').build()).build();
    assert.equal(fs.type, 'array');
    assert.equal(fs.maxItems, 10);
    assert.equal(fs.items?.type, 'string');
  });
});
