// ============================================================
//  Unit Test — MVCCManager
//  Test: begin snapshot, commit, abort, visibility check,
//        write conflict detection, GC
// ============================================================

import assert from 'node:assert/strict';
import { test, describe, beforeEach } from 'node:test';
import { MVCCManager, WriteConflictError } from '../../src/core/transaction/mvcc.js';

describe('MVCCManager — begin & snapshot', () => {

  let mvcc: MVCCManager;
  beforeEach(() => { mvcc = new MVCCManager(); });

  test('begin() menghasilkan snapshot dengan txId unik', () => {
    const s1 = mvcc.begin();
    const s2 = mvcc.begin();
    assert.ok(s1.txId > 0n);
    assert.ok(s2.txId > s1.txId);
  });

  test('snapshot visibleTxIds berisi txId yang sudah committed sebelum begin', () => {
    // Commit txId 1 dulu
    const snap1 = mvcc.begin();
    mvcc.commit(snap1.txId);

    // Snapshot baru harus bisa lihat txId snap1
    const snap2 = mvcc.begin();
    assert.ok(snap2.visibleTxIds.has(snap1.txId),
      'snap2 harus bisa melihat snap1 yang sudah committed');
  });

  test('snapshot tidak bisa lihat txId yang commit setelah begin', () => {
    const snap1 = mvcc.begin();  // ambil snapshot
    const snap2 = mvcc.begin();  // tx lain

    // snap2 commit SETELAH snap1 begin
    mvcc.commit(snap2.txId);

    // snap1 tidak boleh melihat snap2
    assert.ok(!snap1.visibleTxIds.has(snap2.txId),
      'snap1 tidak boleh melihat txId yang commit setelah snap1 begin');
  });
});

describe('MVCCManager — isVisible', () => {

  let mvcc: MVCCManager;
  beforeEach(() => { mvcc = new MVCCManager(); });

  test('record ditulis oleh tx sendiri → visible', () => {
    const snap = mvcc.begin();
    // Record dengan txId sama dengan snapshot
    assert.ok(mvcc.isVisible(snap.txId, snap));
  });

  test('record dari committed tx sebelum snapshot → visible', () => {
    const old  = mvcc.begin();
    mvcc.commit(old.txId);
    const snap = mvcc.begin(); // begin setelah old commit
    assert.ok(mvcc.isVisible(old.txId, snap));
  });

  test('record dari uncommitted tx → tidak visible', () => {
    const other = mvcc.begin();
    const snap  = mvcc.begin(); // begin setelah other (tapi other belum commit)
    // snap.visibleTxIds tidak berisi other.txId karena belum commit saat snap dibuat
    assert.ok(!mvcc.isVisible(other.txId, snap));
  });

  test('isCommitted: benar untuk committed txId', () => {
    const snap = mvcc.begin();
    assert.ok(!mvcc.isCommitted(snap.txId)); // belum commit
    mvcc.commit(snap.txId);
    assert.ok(mvcc.isCommitted(snap.txId));  // sudah commit
  });
});

describe('MVCCManager — commit & abort', () => {

  let mvcc: MVCCManager;
  beforeEach(() => { mvcc = new MVCCManager(); });

  test('commit: txId masuk ke committed set', () => {
    const snap = mvcc.begin();
    mvcc.commit(snap.txId);
    assert.ok(mvcc.isCommitted(snap.txId));
  });

  test('abort: txId tidak masuk ke committed set', () => {
    const snap = mvcc.begin();
    mvcc.abort(snap.txId);
    assert.ok(!mvcc.isCommitted(snap.txId));
  });

  test('commit txId yang tidak dikenal → throw', () => {
    assert.throws(() => mvcc.commit(999n), /Unknown transaction/);
  });
});

describe('MVCCManager — write conflict detection', () => {

  let mvcc: MVCCManager;
  beforeEach(() => { mvcc = new MVCCManager(); });

  test('dua tx menulis key yang berbeda → tidak conflict', () => {
    const s1 = mvcc.begin();
    const s2 = mvcc.begin();
    mvcc.recordWrite(s1.txId, 'key-A');
    mvcc.recordWrite(s2.txId, 'key-B');
    mvcc.commit(s1.txId); // commit s1 dulu
    assert.doesNotThrow(() => mvcc.commit(s2.txId)); // s2 tidak conflict
  });

  test('dua tx menulis key yang sama → WriteConflictError saat commit kedua', () => {
    const s1 = mvcc.begin();
    const s2 = mvcc.begin();
    mvcc.recordWrite(s1.txId, 'shared-key');
    mvcc.recordWrite(s2.txId, 'shared-key');
    mvcc.commit(s1.txId); // s1 commit dulu

    assert.throws(() => mvcc.commit(s2.txId), (err) => {
      assert.ok(err instanceof WriteConflictError);
      assert.ok(err.message.includes('shared-key'));
      return true;
    });
  });
});

describe('MVCCManager — autoCommitTxId', () => {

  let mvcc: MVCCManager;
  beforeEach(() => { mvcc = new MVCCManager(); });

  test('autoCommitTxId menghasilkan txId yang langsung committed', () => {
    const txId = mvcc.autoCommitTxId();
    assert.ok(txId > 0n);
    assert.ok(mvcc.isCommitted(txId));
  });

  test('autoCommitTxId unik setiap call', () => {
    const a = mvcc.autoCommitTxId();
    const b = mvcc.autoCommitTxId();
    assert.notEqual(a, b);
  });
});
