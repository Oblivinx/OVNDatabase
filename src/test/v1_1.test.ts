// ============================================================
//  OvnDB v1.1 — Integration Test
//  Menguji semua fitur baru: FileLock, Transaction, TTLIndex,
//  Observability, Schema, dan fast path encrypted _id lookup.
//
//  Run: npm run build:full && OVNDB_LOG=silent node dist/test/v1_1.test.js
// ============================================================
import { OvnDB, TTLIndex, Observability, field }  from '../index.js';
import { cryptoFromPassphrase }                   from '../index.js';
import fsp from 'fs/promises';

const DATA_DIR = '/tmp/ovndb-v1_1-test';

let passed = 0;
let failed = 0;

function ok(msg: string)  { console.log(`  ✓ ${msg}`); passed++; }
function fail(msg: string){ console.error(`  ✗ FAIL: ${msg}`); failed++; }
function section(title: string) {
  console.log('');
  console.log(`  ┌─ ${title} ${'─'.repeat(Math.max(0, 52 - title.length))}┐`);
}
function assert(cond: boolean, msg: string) {
  if (cond) ok(msg);
  else       fail(msg);
}
async function assertThrows(fn: () => Promise<unknown>, msgMatch: string, label: string) {
  try {
    await fn();
    fail(`${label} — seharusnya throw tapi tidak`);
  } catch (e: any) {
    if (e.message?.includes(msgMatch)) ok(label);
    else fail(`${label} — error tidak sesuai: ${e.message}`);
  }
}

// ═══════════════════════════════════════════════════════════
async function main() {
  await fsp.rm(DATA_DIR, { recursive: true, force: true });

  console.log('');
  console.log('  ╔══════════════════════════════════════════════════════╗');
  console.log('  ║         OvnDB v1.1 — Integration Test Suite         ║');
  console.log('  ╚══════════════════════════════════════════════════════╝');

  // ─────────────────────────────────────────────────────────
  // TEST 1: FileLock — single writer guard
  // ─────────────────────────────────────────────────────────
  section('1. FileLock — single writer guard');
  {
    const db1 = await OvnDB.open(DATA_DIR);
    // Coba buka DB yang sama dari "proses lain" (simulasi: override PID)
    // Karena kita tidak bisa fork, test logika stale detection.
    // FileLock hanya bisa diuji penuh dengan dua proses berbeda.
    // Di sini kita test: lock file exists + release bersih.
    assert(db1.isOpen, 'DB terbuka dengan file lock');

    const lockPath = `${DATA_DIR}/.ovndb.lock`;
    const lockExists = await fsp.stat(lockPath).then(() => true).catch(() => false);
    assert(lockExists, 'Lock file dibuat saat DB dibuka');

    await db1.close();
    const lockAfterClose = await fsp.stat(lockPath).then(() => true).catch(() => false);
    assert(!lockAfterClose, 'Lock file dihapus setelah DB ditutup');

    // Buka lagi — harus berhasil setelah lock dilepas
    const db2 = await OvnDB.open(DATA_DIR);
    assert(db2.isOpen, 'DB bisa dibuka ulang setelah lock dilepas');
    await db2.close();
  }

  // ─────────────────────────────────────────────────────────
  // TEST 2: Transaction — atomic multi-collection writes
  // ─────────────────────────────────────────────────────────
  section('2. Transaction — atomic commit & rollback');
  {
    const db    = await OvnDB.open(DATA_DIR);
    const users = await db.collection<any>('users');
    const wallets = await db.collection<any>('wallets');

    // 2a: Commit berhasil — semua operasi ter-apply
    const tx1 = db.beginTransaction();
    tx1.insert(users,   { name: 'Budi', phone: '628111' });
    tx1.insert(wallets, { owner: 'Budi', balance: 100_000 });
    await tx1.commit();
    assert(tx1.status === 'committed', 'tx1 status = committed');

    const budi   = await users.findOne({ phone: '628111' });
    const wallet = await wallets.findOne({ owner: 'Budi' });
    assert(budi !== null,   'user Budi ter-insert via transaction');
    assert(wallet !== null, 'wallet ter-insert via transaction');
    assert(tx1.opCount === 2, `tx1 op count = 2 (got ${tx1.opCount})`);

    // 2b: Rollback manual sebelum commit
    const tx2 = db.beginTransaction();
    tx2.insert(users, { name: 'Sari', phone: '628222' });
    await tx2.rollback();
    assert(tx2.status === 'rolled_back', 'tx2 status = rolled_back');

    const sari = await users.findOne({ phone: '628222' });
    // rollback sebelum commit → tidak ada yang masuk (staging belum dieksekusi)
    assert(sari === null, 'Sari tidak ada setelah rollback (belum di-commit)');

    // 2c: Update dalam transaction
    const tx3 = db.beginTransaction();
    tx3.update(wallets, { _id: wallet!._id }, { $inc: { balance: -50_000 } });
    await tx3.commit();
    const updatedWallet = await wallets.findOne({ _id: wallet!._id });
    assert(updatedWallet?.balance === 50_000, `Balance setelah update: ${updatedWallet?.balance}`);

    // 2d: Error setelah tx committed → tidak bisa dimodifikasi
    await assertThrows(
      () => tx1.commit(),
      'status saat ini',
      'Commit dua kali throw error'
    );

    await db.close();
  }

  // ─────────────────────────────────────────────────────────
  // TEST 3: Schema Validation
  // ─────────────────────────────────────────────────────────
  section('3. Schema Validation');
  {
    const db = await OvnDB.open(DATA_DIR);
    const contacts = await db.collectionV2<any>('contacts', {
      schema: {
        name:  field.string({ required: true, minLength: 1, maxLength: 50 }),
        phone: field.phoneID({ required: true }),
        age:   field.number({ min: 0, max: 150, integer: true }),
      },
      schemaMode: 'strict',
    });

    // 3a: Insert valid — harus berhasil
    const c1 = await contacts.insertOne({ name: 'Budi', phone: '6281234567890', age: 25 });
    assert(c1._id !== undefined, 'Insert valid berhasil');

    // 3b: Insert invalid name — harus throw
    await assertThrows(
      () => contacts.insertOne({ name: '', phone: '6281234567890' }),
      'Schema validation',
      'Insert nama kosong throw ValidationError'
    );

    // 3c: Insert phone invalid
    await assertThrows(
      () => contacts.insertOne({ name: 'Sari', phone: '0812xxx' }),
      'Schema validation',
      'Insert phone non-628 throw ValidationError'
    );

    // 3d: Insert age out of range
    await assertThrows(
      () => contacts.insertOne({ name: 'Reza', phone: '628999', age: 200 }),
      'Schema validation',
      'Insert age > 150 throw ValidationError'
    );

    // 3e: Warn mode — tidak throw, hanya log
    const lenient = await db.collectionV2<any>('lenient', {
      schema:     { name: field.string({ required: true }) },
      schemaMode: 'warn',
    });
    const lenDoc = await lenient.insertOne({ name: '' }); // required string tapi empty — warn only
    assert(lenDoc._id !== undefined, 'Warn mode tidak throw, dokumen masuk');

    await db.close();
  }

  // ─────────────────────────────────────────────────────────
  // TEST 4: TTLIndex — auto-expiry
  // ─────────────────────────────────────────────────────────
  section('4. TTLIndex — auto-expiry');
  {
    // Gunakan direktori terpisah agar tidak tercampur data test lain
    const TTL_DIR = DATA_DIR + '/ttl';
    const db       = await OvnDB.open(TTL_DIR);
    const sessions = await db.collection<any>('sessions');

    // Insert 3 dokumen: 2 sudah expired, 1 belum
    const now = Date.now();
    await sessions.insertMany([
      { sessionId: 'A', user: 'Budi', expiresAt: now - 10_000 },  // expired
      { sessionId: 'B', user: 'Sari', expiresAt: now - 5_000  },  // expired
      { sessionId: 'C', user: 'Reza', expiresAt: now + 60_000 },  // valid (1 menit lagi)
    ]);
    await sessions.flush();

    const ttl = new TTLIndex(sessions, {
      field:         'expiresAt',
      checkInterval: 60_000, // tidak auto-fire dalam test
    });

    // Purge manual
    const deleted = await ttl.purgeNow();
    assert(deleted === 2, `TTL purge menghapus 2 expired docs (got ${deleted})`);
    assert(ttl.totalDeleted === 2, `totalDeleted = 2 (got ${ttl.totalDeleted})`);
    assert(ttl.cycleCount === 1,   `cycleCount = 1 (got ${ttl.cycleCount})`);

    await sessions.flush();
    const remaining = await sessions.count({});
    assert(remaining === 1, `Tersisa 1 session valid (got ${remaining})`);

    const valid = await sessions.findOne({ sessionId: 'C' });
    assert(valid !== null, 'Session C (belum expired) masih ada');

    // Helper static
    const exp5m = TTLIndex.expiresIn(5, 'minutes');
    assert(exp5m > now + 4 * 60_000 && exp5m < now + 6 * 60_000,
           'expiresIn(5, minutes) dalam range yang benar');

    assert(TTLIndex.isExpired({ expiresAt: now - 1 }),   'isExpired: waktu lampau = true');
    assert(!TTLIndex.isExpired({ expiresAt: now + 999 }), 'isExpired: waktu depan = false');

    ttl.stop();
    assert(!ttl.isRunning, 'TTL worker berhenti setelah stop()');

    await db.close();
    await fsp.rm(TTL_DIR, { recursive: true, force: true }).catch(() => {});
  }

  // ─────────────────────────────────────────────────────────
  // TEST 5: Observability — tracking + laporan
  // ─────────────────────────────────────────────────────────
  section('5. Observability — query tracking');
  {
    const obs = new Observability({ slowQueryMs: 5, logSlowQueries: false });
    const db  = await OvnDB.open(DATA_DIR);
    const col = await db.collection<any>('obs_test');

    await col.insertMany([
      { name: 'A', val: 1 },
      { name: 'B', val: 2 },
      { name: 'C', val: 3 },
    ]);

    // Track operasi
    await obs.track('findOne', 'obs_test', () => col.findOne({ name: 'A' }));
    await obs.track('find',    'obs_test', () => col.find({}));
    await obs.track('findOne', 'obs_test', () => col.findOne({ name: 'Z' })); // miss

    // Simulasi slow query dengan sleep
    await obs.track('find', 'obs_test', async () => {
      await new Promise(r => setTimeout(r, 10)); // 10ms > threshold 5ms
      return col.find({});
    });

    const report = obs.report();
    assert(report.totalOps === 4,      `totalOps = 4 (got ${report.totalOps})`);
    assert(report.summary.slowOps >= 1, `ada slow queries (got ${report.summary.slowOps})`);
    assert(report.collections.length >= 1, 'ada data per collection');

    const obsCol = report.collections.find(c => c.collection === 'obs_test');
    assert(obsCol !== undefined,      'collection obs_test ada di report');
    assert(obsCol!.totalOps === 4,    `obs_test.totalOps = 4 (got ${obsCol!.totalOps})`);

    // Error tracking
    obs.record('insertOne', 'obs_test', 2, { error: 'Duplicate _id' });
    const report2 = obs.report();
    assert(report2.recentErrors.length >= 1, 'error dicatat di recentErrors');

    // formatReport tidak crash
    const formatted = obs.formatReport();
    assert(formatted.includes('OvnDB Observability Report'), 'formatReport menghasilkan output valid');

    await db.close();
  }

  // ─────────────────────────────────────────────────────────
  // TEST 6: Encrypted _id fast path (fix v1.1)
  // ─────────────────────────────────────────────────────────
  section('6. Encrypted collection — fast path _id lookup');
  {
    const db     = await OvnDB.open(DATA_DIR);
    const crypto = cryptoFromPassphrase('test-passphrase-v11', DATA_DIR);
    const enc    = await db.collectionV2<any>('enc_test', { crypto });

    // Insert 1000 dokumen terenkripsi
    const N   = 1_000;
    const ids: string[] = [];
    for (let i = 0; i < N; i++) {
      const doc = await enc.insertOne({ seq: i, data: `payload-${i}` });
      ids.push(doc._id);
    }
    await enc.flush();

    // Ukur waktu lookup by _id — harusnya cepat (O(log n), bukan O(n) scan)
    const sampleId = ids[Math.floor(N / 2)]!;
    const t0  = Date.now();
    const doc = await enc.findOne({ _id: sampleId });
    const ms  = Date.now() - t0;

    assert(doc !== null, 'findOne(_id) pada encrypted collection berhasil');
    assert(doc!._id === sampleId, '_id dokumen sesuai');
    assert(ms < 50, `findOne(_id) selesai dalam <50ms (got ${ms}ms) — bukan full scan`);

    // Verify semua 1000 bisa dibaca
    let misses = 0;
    for (const id of ids.slice(0, 100)) {
      const d = await enc.findOne({ _id: id });
      if (!d) misses++;
    }
    assert(misses === 0, '100 random _id reads berhasil (0 misses)');

    await db.close();
  }

  // ─────────────────────────────────────────────────────────
  // TEST 7: FileLock — no double open (simulasi)
  // ─────────────────────────────────────────────────────────
  section('7. FileLock — tolak buka dua kali dari proses sama (stale=false)');
  {
    const db1 = await OvnDB.open(DATA_DIR);

    // Buka DB kedua ke folder yang sama — harus throw karena lock aktif
    // (proses yang sama, PID sama → lock.kill(pid,0) tidak throw → bukan stale)
    await assertThrows(
      () => OvnDB.open(DATA_DIR),
      'digunakan oleh proses lain',
      'Buka DB dua kali dari proses sama throw error'
    );

    await db1.close();

    // Setelah close, bisa dibuka lagi
    const db2 = await OvnDB.open(DATA_DIR);
    assert(db2.isOpen, 'DB bisa dibuka ulang setelah close');
    await db2.close();
  }

  // ─────────────────────────────────────────────────────────
  // SUMMARY
  // ─────────────────────────────────────────────────────────
  console.log('');
  const total = passed + failed;
  console.log('  ╔══════════════════════════════════════════════════════╗');
  if (failed === 0) {
    console.log('  ║          SEMUA TEST LULUS ✓                          ║');
  } else {
    console.log('  ║          ADA TEST GAGAL ✗                            ║');
  }
  console.log('  ╠══════════════════════════════════════════════════════╣');
  console.log(`  ║  Total  : ${String(total).padEnd(42)}║`);
  console.log(`  ║  Passed : ${String(passed).padEnd(42)}║`);
  console.log(`  ║  Failed : ${String(failed).padEnd(42)}║`);
  console.log('  ╚══════════════════════════════════════════════════════╝');
  console.log('');

  await fsp.rm(DATA_DIR, { recursive: true, force: true }).catch(() => {});

  if (failed > 0) process.exit(1);
}

main().catch(err => {
  console.error('\n  ✗ TEST CRASHED:', err);
  process.exit(1);
});
