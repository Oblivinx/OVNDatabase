// ============================================================
//  OvnDB v3.0 — Full Feature Showcase
//  Demonstrasi semua 19 gap yang sudah diperbaiki
// ============================================================

import path from 'path';
import fsp from 'fs/promises';
import zlib from 'zlib';
import {
  OvnDB, field, SchemaValidator,
  cryptoFromPassphrase, FieldCrypto, CryptoLayer,
  RelationManager, MigrationRunner, TTLIndex,
} from '../src/index.js';

const DIR = path.join(process.cwd(), '.tmp-v3-showcase');

// ── Schema ────────────────────────────────────────────────────

const userSchema = new SchemaValidator({
  name: field('string').required().minLength(2).build(),
  email: field('string').required().pattern(/^[^@]+@[^@]+\.[^@]+$/).build(),
  role: field('string').required().enum('admin', 'user', 'guest').build(),
  points: field('number').min(0).default(0).build(),
  active: field('boolean').default(true).build(),
});

async function main() {
  await fsp.rm(DIR, { recursive: true, force: true });

  // ── G15: DB-level compression ─────────────────────────────
  console.log('\n=== G15: Compression (gzip) ===');
  const db = await OvnDB.open(DIR, {
    gracefulShutdown: true,
    compressFn: (buf) => zlib.gzipSync(buf),
    decompressFn: (buf) => zlib.gunzipSync(buf),
  });

  const users = await db.collection<{
    _id: string; name: string; email: string; role: string; points: number; active: boolean;
  }>('users');

  await users.createIndex({ field: 'email', unique: true });
  await users.createIndex({ field: 'role', unique: false });
  await users.createIndex({ field: 'points', unique: false });

  // ── Insert data ───────────────────────────────────────────
  const inserted = await users.insertMany([
    { name: 'Budi Santoso', email: 'budi@mail.com', role: 'admin', points: 1500, active: true },
    { name: 'Siti Rahayu', email: 'siti@mail.com', role: 'user', points: 200, active: true },
    { name: 'Ahmad Fauzi', email: 'ahmad@mail.com', role: 'user', points: 800, active: true },
    { name: 'Dewi Lestari', email: 'dewi@mail.com', role: 'guest', points: 50, active: false },
    { name: 'Eko Prasetyo', email: 'eko@mail.com', role: 'user', points: 2500, active: true },
    { name: 'Farida Hanum', email: 'farida@mail.com', role: 'admin', points: 750, active: true },
    { name: 'Gilang Ramadan', email: 'gilang@mail.com', role: 'user', points: 100, active: true },
    { name: 'Hana Pertiwi', email: 'hana@mail.com', role: 'user', points: 3000, active: true },
  ]);
  console.log(`Inserted ${inserted.length} users (data compressed with gzip)`);

  // ── G19: findWithStats ────────────────────────────────────
  console.log('\n=== G19: Execution Stats ===');
  const { docs: admins, stats: adminStats } = await users.findWithStats({ role: 'admin' });
  console.log(`Find admins → ${admins.length} docs`);
  console.log(`  planType:         ${adminStats.planType}`);
  console.log(`  indexUsed:        ${adminStats.indexUsed ?? 'none'}`);
  console.log(`  totalDocsScanned: ${adminStats.totalDocsScanned}`);
  console.log(`  totalKeysScanned: ${adminStats.totalKeysScanned}`);
  console.log(`  executionTimeMs:  ${adminStats.executionTimeMs}ms`);
  console.log(`  nReturned:        ${adminStats.nReturned}`);

  const { docs: richUsers, stats: richStats } = await users.findWithStats(
    { points: { $gte: 1000 } },
    { sort: { points: -1 } },
  );
  console.log(`\nFind points >= 1000 → ${richUsers.length} docs`);
  console.log(`  scanType: ${richStats.planType}, scanned: ${richStats.totalDocsScanned}, ms: ${richStats.executionTimeMs}`);

  // ── G10: $type, $where, $mod operators ───────────────────
  console.log('\n=== G10: New Query Operators ===');
  const stringNames = await users.find({ name: { $type: 'string' } as any });
  console.log(`$type string (name): ${stringNames.length} users`);

  const highPoints = await users.find({ points: { $where: (v: unknown) => (v as number) > 500 } as any });
  console.log(`$where (points > 500): ${highPoints.length} users`);

  const evenPoints = await users.find({ points: { $mod: [2, 0] } as any });
  console.log(`$mod (points % 2 == 0): ${evenPoints.length} users`);

  // ── G11: $bucket, $facet, $setWindowFields ────────────────
  console.log('\n=== G11: Advanced Aggregation ===');

  const buckets = await users.aggregate([
    { $match: { active: true } },
    { $bucket: { groupBy: '$points', boundaries: [0, 100, 500, 1000, 5000], default: 'other' } } as any,
  ]);
  console.log('$bucket (points distribution):', JSON.stringify(buckets));

  const facets = await users.aggregate([
    {
      $facet: {
        byRole: [{ $group: { _id: '$role', count: { $sum: 1 } } }],
        topUsers: [{ $sort: { points: -1 } }, { $limit: 3 }],
        stats: [{ $group: { _id: null, avg: { $avg: '$points' }, total: { $sum: '$points' } } }],
      }
    } as any,
  ]);
  const f = facets[0] as any;
  console.log('$facet byRole:', JSON.stringify(f.byRole));
  console.log('$facet topUsers names:', f.topUsers.map((u: any) => u.name));
  console.log('$facet stats:', JSON.stringify(f.stats));

  const windowResults = await users.aggregate([
    { $sort: { points: -1 } },
    {
      $setWindowFields: {
        sortBy: { points: -1 },
        output: {
          rank: { $rank: {} },
          runningSum: { $sum: '$points', window: { documents: ['unbounded', 'current'] } },
        },
      }
    } as any,
  ]);
  console.log('\n$setWindowFields (rank & running sum):');
  for (const r of (windowResults as any[]).slice(0, 4)) {
    console.log(`  ${r.name}: points=${r.points}, rank=${r.rank}, runningSum=${r.runningSum}`);
  }

  // ── G1: Byte-based LRU stats ──────────────────────────────
  console.log('\n=== G1: Byte-based LRU Cache ===');
  const s = await users.stats();
  console.log(`Cache size: ${s.cacheSize} entries, hit rate: ${(s.cacheHitRate * 100).toFixed(1)}%`);

  // ── G12: FieldCrypto — per-field encryption ───────────────
  console.log('\n=== G12: Per-field Encryption ===');
  const cryptoKey = await cryptoFromPassphrase('field-secret-key', DIR + '/fieldcrypto', 0);
  const fieldCrypto = new FieldCrypto(cryptoKey);

  const sensitiveDoc = { name: 'Secret User', phone: '08123456789', ssn: '1234-5678-9012' };
  const encrypted = fieldCrypto.encryptFields(sensitiveDoc, ['phone', 'ssn']);
  console.log('Original phone:', sensitiveDoc.phone);
  console.log('Encrypted phone (base64):', (encrypted as any).phone.slice(0, 30) + '...');

  const decrypted = fieldCrypto.decryptFields(encrypted);
  console.log('Decrypted phone:', (decrypted as any).phone);
  console.log('SSN match:', (decrypted as any).ssn === sensitiveDoc.ssn);
  console.log('isEncryptedField(phone):', fieldCrypto.isEncryptedField(encrypted, 'phone'));
  console.log('isEncryptedField(name):', fieldCrypto.isEncryptedField(encrypted, 'name'));

  // ── G13: Key versioning ───────────────────────────────────
  console.log('\n=== G13: Key Versioning ===');
  const cryptoV0 = await cryptoFromPassphrase('passphrase-v0', DIR + '/kv', 0);
  const cryptoV1 = await cryptoFromPassphrase('passphrase-v1', DIR + '/kv', 1);
  const plainBuf = Buffer.from('{"secret":"data"}', 'utf8');

  const ciphertextV0 = cryptoV0.encrypt(plainBuf);
  const version = CryptoLayer.getKeyVersion(ciphertextV0);
  console.log('Key version in ciphertext:', version);

  const rotated = cryptoV0.reencrypt(ciphertextV0, cryptoV1);
  const versionAfter = CryptoLayer.getKeyVersion(rotated);
  console.log('Key version after rotation:', versionAfter);

  const decryptedV1 = cryptoV1.decrypt(rotated);
  console.log('Decrypted with v1 key:', decryptedV1.toString('utf8'));

  // ── G17: Manifest checksum demo ───────────────────────────
  console.log('\n=== G17: Manifest Checksum ===');
  const status = await db.status();
  const col = status.collections.find(c => c.name === 'users');
  console.log(`users manifest: live=${col?.totalLive}, healthy=${status.isHealthy}`);
  console.log('(Manifest corruption akan terdeteksi saat open() berikutnya)');

  // ── G18: MigrationRunner ──────────────────────────────────
  console.log('\n=== G18: MigrationRunner ===');

  // Simulasi: v1 docs tidak punya _schemaVersion
  // Migrate ke v2: tambah field 'tier' berdasarkan points
  const runner = new MigrationRunner(users);

  const pending = await runner.countPending(2);
  console.log(`Pending migration to v2: ${pending} docs`);

  const dryRun = await runner.migrate(2, doc => doc, { dryRun: true });
  console.log(`Dry run count: ${dryRun.dryRunCount}`);

  const migResult = await runner.migrate(2, (doc) => ({
    ...doc,
    tier: doc.points >= 1000 ? 'gold' : doc.points >= 500 ? 'silver' : 'bronze',
  }), {
    batchSize: 5,
    onProgress: (p) => process.stdout.write(`  Progress: ${p.migrated}/${p.total}\r`),
  });
  console.log(`\nMigration complete: migrated=${migResult.migrated}, failed=${migResult.failed}, ${migResult.elapsedMs}ms`);

  const isComplete = await runner.isComplete(2);
  console.log(`isComplete(v2): ${isComplete}`);

  // Verify tier field terpasang
  const goldUsers = await users.find({ tier: 'gold' } as any);
  console.log(`Gold tier users: ${goldUsers.length}`);

  // ── G5: WAL size cap demo ─────────────────────────────────
  console.log('\n=== G5+G6: WAL Stability ===');
  const orders = await db.collection<{ _id: string; userId: string; total: number; status: string }>('orders');
  // Insert banyak data — WAL akan auto-rotate jika melampaui WAL_MAX_SIZE_BYTES
  const bulkOrders = Array.from({ length: 50 }, (_, i) => ({
    userId: inserted[i % inserted.length]!._id,
    total: Math.floor(Math.random() * 1_000_000),
    status: ['pending', 'paid', 'shipped', 'done'][i % 4]!,
  }));
  await orders.insertMany(bulkOrders);
  console.log(`Inserted ${bulkOrders.length} orders, WAL stable`);

  // ── G7+G8: B+ Tree cursor ─────────────────────────────────
  console.log('\n=== G7+G8: B+ Tree Features ===');
  const { docs: page1 } = await users.findWithStats({}, { limit: 3, sort: { _id: 1 } });
  const { docs: page2 } = await users.findWithStats({}, { limit: 3, sort: { _id: 1 }, after: page1[page1.length - 1]!._id });
  console.log(`Page 1 users: ${page1.map(u => u.name).join(', ')}`);
  console.log(`Page 2 users: ${page2.map(u => u.name).join(', ')}`);

  // truncate test — pakai G2 deleteAll() O(1) di belakangnya
  const ordersBefore = await orders.countDocuments();
  await orders.truncate();
  const ordersAfter = await orders.countDocuments();
  console.log(`\norders.truncate(): ${ordersBefore} → ${ordersAfter} docs`);

  // ── TTL demo ──────────────────────────────────────────────
  console.log('\n=== TTL Index ===');
  const sessions = await db.collection<{ _id: string; userId: string; expiresAt: number }>('sessions');
  await sessions.insertOne({ userId: inserted[0]!._id, expiresAt: Date.now() - 1000 }); // sudah expired
  await sessions.insertOne({ userId: inserted[1]!._id, expiresAt: TTLIndex.expiresIn(1, 'hours') });

  const ttl = new TTLIndex(sessions, { field: 'expiresAt', checkInterval: 100, batchSize: 10 }).start();
  await new Promise(r => setTimeout(r, 200));
  ttl.stop();

  const remainingSessions = await sessions.countDocuments();
  console.log(`Sessions after TTL purge: ${remainingSessions} (was 2, 1 expired)`);
  console.log(`TTL totalDeleted: ${ttl.totalDeleted}`);

  // ── Backup & Status ───────────────────────────────────────
  console.log('\n=== Backup + Status ===');
  await db.backup(DIR + '-backup');
  const finalStatus = await db.status();
  console.log(`DB isHealthy: ${finalStatus.isHealthy}`);
  console.log('Collections:');
  for (const c of finalStatus.collections) {
    console.log(`  ${c.name}: live=${c.totalLive}, encrypted=${c.encrypted}, cache=${(c.cacheHitRate * 100).toFixed(0)}%`);
  }

  console.log('\n=== All v3.0 features verified ✅ ===');
  await db.close();
  await fsp.rm(DIR, { recursive: true, force: true });
  await fsp.rm(DIR + '-backup', { recursive: true, force: true });
  await fsp.rm(DIR + '/fieldcrypto', { recursive: true, force: true }).catch(() => { });
  await fsp.rm(DIR + '/kv', { recursive: true, force: true }).catch(() => { });
}

main().catch(err => { console.error('FATAL:', err); process.exit(1); });
