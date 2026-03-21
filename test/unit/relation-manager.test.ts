// ============================================================
//  Unit Test — RelationManager
//  Test: register, populate (single), populateMany (batch),
//        array foreign keys, fallback ke ID jika tidak ditemukan
// ============================================================

import assert      from 'node:assert/strict';
import { test, describe, before, after } from 'node:test';
import os          from 'node:os';
import path        from 'node:path';
import fsp         from 'node:fs/promises';
import { OvnDB }   from '../../src/index.js';
import { RelationManager } from '../../src/collection/relation-manager.js';

interface User { _id: string; name: string; city: string }
interface Group { _id: string; title: string }
interface Message { _id: string; text: string; userId: string; groupId: string }

let db:    OvnDB;
let dir:   string;
let users: Awaited<ReturnType<typeof db.collection<User>>>;
let groups:Awaited<ReturnType<typeof db.collection<Group>>>;
let msgs:  Awaited<ReturnType<typeof db.collection<Message>>>;
let rel:   RelationManager;

before(async () => {
  dir    = await fsp.mkdtemp(path.join(os.tmpdir(), 'ovndb-rel-'));
  db     = await OvnDB.open(dir, { fileLock: false });
  users  = await db.collection<User>('users');
  groups = await db.collection<Group>('groups');
  msgs   = await db.collection<Message>('messages');

  // Seed data
  await users.insertMany([
    { _id: 'u1', name: 'Budi',  city: 'Jakarta' },
    { _id: 'u2', name: 'Siti',  city: 'Bandung' },
    { _id: 'u3', name: 'Ahmad', city: 'Surabaya' },
  ]);
  await groups.insertMany([
    { _id: 'g1', title: 'Backend Devs' },
    { _id: 'g2', title: 'Frontend Devs' },
  ]);
  await msgs.insertMany([
    { _id: 'm1', text: 'Hello!',   userId: 'u1', groupId: 'g1' },
    { _id: 'm2', text: 'Hi there', userId: 'u2', groupId: 'g1' },
    { _id: 'm3', text: 'Yo!',      userId: 'u1', groupId: 'g2' },
    { _id: 'm4', text: 'Hey',      userId: 'u3', groupId: 'g2' },
  ]);

  rel = new RelationManager();
  rel.register('users',  users);
  rel.register('groups', groups);
});

after(async () => {
  await db.close();
  await fsp.rm(dir, { recursive: true, force: true });
});

describe('RelationManager.populate (single)', () => {

  test('resolve satu foreign key', async () => {
    const msg  = await msgs.findOne({ _id: 'm1' });
    const full = await rel.populate(msg!, { userId: 'users' });
    assert.equal(typeof full.userId, 'object', 'userId harus jadi objek User');
    assert.equal((full.userId as User).name, 'Budi');
  });

  test('resolve banyak foreign key sekaligus', async () => {
    const msg  = await msgs.findOne({ _id: 'm2' });
    const full = await rel.populate(msg!, { userId: 'users', groupId: 'groups' });
    assert.equal((full.userId as User).name, 'Siti');
    assert.equal((full.groupId as Group).title, 'Backend Devs');
  });

  test('fallback ke string ID jika dokumen tidak ditemukan', async () => {
    const fakeMsg = { _id: 'fake', text: 'x', userId: 'u-nonexistent', groupId: 'g1' };
    const full    = await rel.populate(fakeMsg, { userId: 'users' });
    assert.equal(full.userId, 'u-nonexistent', 'Harus fallback ke ID string');
  });

  test('field bukan string dilewati', async () => {
    const fakeMsg = { _id: 'f2', text: 'x', userId: 123 as unknown as string, groupId: 'g1' };
    const full    = await rel.populate(fakeMsg, { userId: 'users' });
    assert.equal(full.userId, 123, 'Field non-string tidak dimodifikasi');
  });
});

describe('RelationManager.populateMany (batch)', () => {

  test('resolve semua foreign key dengan deduplication', async () => {
    const messages = await msgs.find({});
    const pop      = await rel.populateMany(messages, { userId: 'users', groupId: 'groups' });

    assert.equal(pop.length, 4);
    for (const m of pop) {
      assert.equal(typeof m.userId,  'object', `${m._id}: userId harus objek`);
      assert.equal(typeof m.groupId, 'object', `${m._id}: groupId harus objek`);
    }
  });

  test('u1 muncul di dua pesan tapi hanya di-fetch sekali (deduplication)', async () => {
    // u1 ada di m1 dan m3 — pastikan keduanya resolved dengan benar
    const messages = await msgs.find({ userId: 'u1' });
    const pop      = await rel.populateMany(messages, { userId: 'users' });
    assert.equal(pop.length, 2);
    assert.ok(pop.every(m => (m.userId as User).name === 'Budi'));
  });

  test('array kosong → hasil kosong', async () => {
    const pop = await rel.populateMany([], { userId: 'users' });
    assert.equal(pop.length, 0);
  });

  test('dokumen yang tidak punya foreign key → tidak error', async () => {
    const docs = [{ _id: 'x1', text: 'hello' } as unknown as Message];
    const pop  = await rel.populateMany(docs, { userId: 'users' });
    assert.equal(pop.length, 1);
    // userId tidak ada di docs → field tidak ada di hasil
    assert.equal((pop[0]! as Record<string,unknown>)['userId'], undefined);
  });
});

describe('RelationManager utility', () => {

  test('registeredCollections mengembalikan nama yang di-register', () => {
    const names = rel.registeredCollections.sort();
    assert.deepEqual(names, ['groups', 'users']);
  });

  test('collection yang tidak di-register → throw', async () => {
    await assert.rejects(
      () => rel.populate({ _id: 'x', text: 't', userId: 'u1', groupId: 'g1' }, { userId: 'nonexistent' }),
      /belum di-register/,
    );
  });

  test('clear() menghapus semua registrasi', () => {
    const tempRel = new RelationManager();
    tempRel.register('users', users);
    assert.equal(tempRel.registeredCollections.length, 1);
    tempRel.clear();
    assert.equal(tempRel.registeredCollections.length, 0);
  });
});
