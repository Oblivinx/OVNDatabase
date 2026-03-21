// ============================================================
//  Unit Test — SchemaValidator
//  Test: type checking, required, nullable, string/number/array/object,
//        custom validator, applyDefaults, validateOrThrow
// ============================================================

import assert from 'node:assert/strict';
import { test, describe } from 'node:test';
import { SchemaValidator, ValidationError, field } from '../../src/schema/schema-validator.js';

describe('SchemaValidator — dasar', () => {

  const schema = new SchemaValidator({
    name:   { type: 'string',  required: true },
    age:    { type: 'number',  required: true },
    active: { type: 'boolean', required: false },
  });

  test('dokumen valid → ok = true', () => {
    const r = schema.validate({ name: 'Budi', age: 25, active: true });
    assert.ok(r.ok);
    assert.equal(r.errors.length, 0);
  });

  test('field required tidak ada → error', () => {
    const r = schema.validate({ age: 25 });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('"name"') && e.includes('wajib')));
  });

  test('tipe salah → error', () => {
    const r = schema.validate({ name: 123 as unknown as string, age: 25 });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('"name"') && e.includes('string')));
  });

  test('field optional boleh tidak ada', () => {
    const r = schema.validate({ name: 'Budi', age: 25 }); // active tidak ada
    assert.ok(r.ok);
  });
});

describe('SchemaValidator — string constraints', () => {

  const schema = new SchemaValidator({
    username: field('string').required().minLength(3).maxLength(20).pattern(/^[a-z]+$/).build(),
    role:     field('string').enum('admin', 'user', 'guest').build(),
  });

  test('string valid', () => {
    const r = schema.validate({ username: 'budi', role: 'admin' });
    assert.ok(r.ok, r.errors.join(', '));
  });

  test('terlalu pendek', () => {
    const r = schema.validate({ username: 'ab' });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('minimal')));
  });

  test('terlalu panjang', () => {
    const r = schema.validate({ username: 'a'.repeat(21) });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('maksimal')));
  });

  test('pattern tidak cocok', () => {
    const r = schema.validate({ username: 'Budi123' });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('pattern')));
  });

  test('enum tidak valid', () => {
    const r = schema.validate({ username: 'budi', role: 'superadmin' });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('role')));
  });
});

describe('SchemaValidator — number constraints', () => {

  const schema = new SchemaValidator({
    score:  field('number').required().min(0).max(100).build(),
    level:  field('number').integer().build(),
  });

  test('number valid', () => {
    const r = schema.validate({ score: 75, level: 3 });
    assert.ok(r.ok, r.errors.join(', '));
  });

  test('di bawah min', () => {
    const r = schema.validate({ score: -1 });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('minimal')));
  });

  test('di atas max', () => {
    const r = schema.validate({ score: 101 });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('maksimal')));
  });

  test('bukan integer', () => {
    const r = schema.validate({ score: 50, level: 3.5 });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('bilangan bulat')));
  });
});

describe('SchemaValidator — array constraints', () => {

  const schema = new SchemaValidator({
    tags: field('array').minItems(1).maxItems(5)
      .items(field('string').maxLength(30).build())
      .build(),
  });

  test('array valid', () => {
    const r = schema.validate({ tags: ['nodejs', 'ts'] });
    assert.ok(r.ok, r.errors.join(', '));
  });

  test('array kosong melanggar minItems', () => {
    const r = schema.validate({ tags: [] });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('minimal')));
  });

  test('terlalu banyak item', () => {
    const r = schema.validate({ tags: ['a','b','c','d','e','f'] });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('maksimal')));
  });

  test('item tidak valid (schema items)', () => {
    const r = schema.validate({ tags: ['ok', 'x'.repeat(31)] });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('[1]'))); // index elemen bermasalah
  });
});

describe('SchemaValidator — nullable & default', () => {

  const schema = new SchemaValidator({
    note:  field('string').nullable().build(),
    score: field('number').default(0).build(),
  });

  test('null diperbolehkan jika nullable', () => {
    const r = schema.validate({ note: null });
    assert.ok(r.ok, r.errors.join(', '));
  });

  test('null tidak diperbolehkan tanpa nullable', () => {
    const s = new SchemaValidator({ name: field('string').required().build() });
    const r = s.validate({ name: null as unknown as string });
    assert.ok(!r.ok);
  });

  test('applyDefaults mengisi field dengan default value', () => {
    const doc: Record<string, unknown> = {};
    schema.applyDefaults(doc);
    assert.equal(doc['score'], 0);
  });

  test('applyDefaults tidak menimpa nilai yang sudah ada', () => {
    const doc: Record<string, unknown> = { score: 99 };
    schema.applyDefaults(doc);
    assert.equal(doc['score'], 99);
  });
});

describe('SchemaValidator — custom validator', () => {

  const schema = new SchemaValidator({
    phone: field('string').required()
      .custom(v => /^628\d{8,12}$/.test(v as string) ? null : 'Format nomor HP tidak valid')
      .build(),
  });

  test('custom validator lolos', () => {
    const r = schema.validate({ phone: '6281234567890' });
    assert.ok(r.ok, r.errors.join(', '));
  });

  test('custom validator gagal', () => {
    const r = schema.validate({ phone: '08123456789' });
    assert.ok(!r.ok);
    assert.ok(r.errors.some(e => e.includes('Format nomor HP')));
  });
});

describe('SchemaValidator.validateOrThrow', () => {

  const schema = new SchemaValidator({
    name: field('string').required().build(),
  });

  test('throw ValidationError jika gagal', () => {
    assert.throws(() => schema.validateOrThrow({}), (err) => {
      assert.ok(err instanceof ValidationError);
      assert.ok(err.errors.length > 0);
      return true;
    });
  });

  test('tidak throw jika valid', () => {
    assert.doesNotThrow(() => schema.validateOrThrow({ name: 'Budi' }));
  });
});

describe('field() builder', () => {

  test('builder membuat FieldSchema yang benar', () => {
    const s = field('string').required().minLength(5).maxLength(50).pattern(/^[a-z]+$/).build();
    assert.equal(s.type,      'string');
    assert.equal(s.required,  true);
    assert.equal(s.minLength, 5);
    assert.equal(s.maxLength, 50);
    assert.ok(s.pattern);
  });

  test('field number dengan semua constraints', () => {
    const s = field('number').min(0).max(100).integer().build();
    assert.equal(s.min, 0);
    assert.equal(s.max, 100);
    assert.equal(s.integer, true);
  });
});
