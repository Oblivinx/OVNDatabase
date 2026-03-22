import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { MVCCManager, WriteConflictError } from '../../src/core/transaction/mvcc.js';

describe('MVCCManager', () => {

  // ── Basic lifecycle ───────────────────────────────────────

  it('begin returns unique txIds', () => {
    const mvcc = new MVCCManager();
    const s1 = mvcc.begin();
    const s2 = mvcc.begin();
    assert.notEqual(s1.txId, s2.txId);
    assert.ok(s2.txId > s1.txId);
  });

  it('commit marks tx as committed', () => {
    const mvcc = new MVCCManager();
    const snap = mvcc.begin();
    mvcc.commit(snap.txId);
    assert.equal(mvcc.isCommitted(snap.txId), true);
  });

  it('abort removes tx from active', () => {
    const mvcc = new MVCCManager();
    const snap = mvcc.begin();
    mvcc.abort(snap.txId);
    assert.equal(mvcc.isCommitted(snap.txId), false);
  });

  it('commit unknown txId throws', () => {
    const mvcc = new MVCCManager();
    assert.throws(() => mvcc.commit(999n), /Unknown transaction/);
  });

  // ── Snapshot isolation ────────────────────────────────────

  it('snapshot only sees pre-existing committed txIds', () => {
    const mvcc = new MVCCManager();
    // Auto-commit some writes
    const tx1 = mvcc.autoCommitTxId();
    const snap = mvcc.begin();
    const tx2 = mvcc.autoCommitTxId();

    // snap was taken after tx1 but before tx2
    assert.equal(mvcc.isVisible(tx1, snap), true);
    assert.equal(mvcc.isVisible(tx2, snap), false);
  });

  it('tx can see its own writes', () => {
    const mvcc = new MVCCManager();
    const snap = mvcc.begin();
    assert.equal(mvcc.isVisible(snap.txId, snap), true);
  });

  it('auto-commit txId is immediately committed', () => {
    const mvcc = new MVCCManager();
    const txId = mvcc.autoCommitTxId();
    assert.equal(mvcc.isCommitted(txId), true);
  });

  // ── Write conflict detection ──────────────────────────────

  it('detects write conflict on same key', () => {
    const mvcc = new MVCCManager();
    const snap1 = mvcc.begin();
    const snap2 = mvcc.begin();

    mvcc.recordWrite(snap1.txId, 'user:1');
    mvcc.recordWrite(snap2.txId, 'user:1');

    mvcc.commit(snap1.txId); // snap1 commits first

    // snap2 should fail — snap1 committed after snap2's snapshot & wrote same key
    assert.throws(
      () => mvcc.commit(snap2.txId),
      (err) => err instanceof WriteConflictError,
    );
  });

  it('no conflict when writing different keys', () => {
    const mvcc = new MVCCManager();
    const snap1 = mvcc.begin();
    const snap2 = mvcc.begin();

    mvcc.recordWrite(snap1.txId, 'user:1');
    mvcc.recordWrite(snap2.txId, 'user:2');

    mvcc.commit(snap1.txId);
    assert.doesNotThrow(() => mvcc.commit(snap2.txId));
  });

  it('no conflict when other tx was visible at snapshot time', () => {
    const mvcc = new MVCCManager();
    const snap1 = mvcc.begin();
    mvcc.recordWrite(snap1.txId, 'key');
    mvcc.commit(snap1.txId);

    // snap2 starts AFTER snap1 committed — snap1 is in visibleTxIds
    const snap2 = mvcc.begin();
    mvcc.recordWrite(snap2.txId, 'key');
    assert.doesNotThrow(() => mvcc.commit(snap2.txId));
  });

  // ── isCommitted ───────────────────────────────────────────

  it('txId 0n (system) is always committed', () => {
    const mvcc = new MVCCManager();
    assert.equal(mvcc.isCommitted(0n), true);
  });

  it('non-existent txId is not committed', () => {
    const mvcc = new MVCCManager();
    assert.equal(mvcc.isCommitted(999n), false);
  });

  // ── nextTxId ──────────────────────────────────────────────

  it('nextTxId increments after begin and autoCommit', () => {
    const mvcc = new MVCCManager();
    const before = mvcc.nextTxId;
    mvcc.begin();
    assert.ok(mvcc.nextTxId > before);
    mvcc.autoCommitTxId();
    assert.ok(mvcc.nextTxId > before + 1n);
  });
});
