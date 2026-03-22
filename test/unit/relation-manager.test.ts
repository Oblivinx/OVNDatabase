import { describe, it, before, after } from 'node:test';
import assert from 'node:assert/strict';
import fsp from 'fs/promises';
import path from 'path';
import os from 'os';
import { OvnDB } from '../../src/index.js';
import { RelationManager } from '../../src/collection/relation-manager.js';

function tmpDir(): string {
  return path.join(os.tmpdir(), 'ovndb-rel-test-' + Date.now() + '-' + Math.random().toString(36).slice(2, 8));
}
async function rmDir(dir: string) {
  await fsp.rm(dir, { recursive: true, force: true }).catch(() => {});
}

describe('RelationManager', () => {
  let dir: string;
  let db: Awaited<ReturnType<typeof OvnDB.open>>;
  let rel: RelationManager;

  before(async () => {
    dir = tmpDir();
    db = await OvnDB.open(dir, { fileLock: false });
    rel = new RelationManager();
  });

  after(async () => {
    await db.close();
    await rmDir(dir);
  });

  it('registers collections', async () => {
    const users = await db.collection('users');
    rel.register('users', users);
    assert.ok(rel.registeredCollections.includes('users'));
  });

  it('populate resolves single foreign key', async () => {
    const users = await db.collection('users');
    const messages = await db.collection('messages');

    await users.insertOne({ _id: 'u1', name: 'Alice' });
    await messages.insertOne({ _id: 'm1', text: 'hello', userId: 'u1' });

    rel.register('users', users);

    const msg = await messages.findOne({ _id: 'm1' });
    const populated = await rel.populate(msg!, { userId: 'users' });
    assert.equal((populated['userId'] as any)['name'], 'Alice');
  });

  it('populate falls back to ID when not found', async () => {
    const messages = await db.collection('messages2');
    await messages.insertOne({ _id: 'm2', text: 'hi', userId: 'nonexistent' });
    
    const msg = await messages.findOne({ _id: 'm2' });
    const populated = await rel.populate(msg!, { userId: 'users' });
    assert.equal(populated['userId'], 'nonexistent');
  });

  it('populateMany deduplicates', async () => {
    const users = await db.collection('users3');
    const posts = await db.collection('posts3');

    await users.insertOne({ _id: 'u1', name: 'Alice' });
    await posts.insertOne({ _id: 'p1', authorId: 'u1' });
    await posts.insertOne({ _id: 'p2', authorId: 'u1' });

    const relMgr = new RelationManager();
    relMgr.register('users3', users);

    const allPosts = await posts.find({});
    const populated = await relMgr.populateMany(allPosts, { authorId: 'users3' });
    assert.equal(populated.length, 2);
    assert.equal((populated[0]!['authorId'] as any)['name'], 'Alice');
    assert.equal((populated[1]!['authorId'] as any)['name'], 'Alice');
  });

  it('throws on unregistered collection', async () => {
    const doc = { _id: '1', refId: 'x' };
    const relMgr = new RelationManager();
    await assert.rejects(
      relMgr.populate(doc, { refId: 'unregistered' }),
      /belum di-register/,
    );
  });

  it('clear removes all registrations', () => {
    const relMgr = new RelationManager();
    relMgr.register('test', {} as any);
    relMgr.clear();
    assert.equal(relMgr.registeredCollections.length, 0);
  });
});
