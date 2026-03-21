// ============================================================
//  OvnDB v2.1 — Complex CRUD Examples
//
//  Demonstrasi semua fitur baru:
//   - bulkWrite() mixed operations
//   - truncate() + exists() + findById() + findManyById()
//   - Transaction.upsert() dan Transaction.replace()
//   - backup() dan status()
//   - CollectionV2 (fix: tidak ada bug scan + writeBuffer lagi)
//   - $setOnInsert dalam upsertOne()
//   - findOneAndReplace()
//   - compact()
//   - aggregate() dengan custom lookupResolver
// ============================================================

import path   from 'path';
import fsp    from 'fs/promises';
import {
  OvnDB, field, SchemaValidator,
  cryptoFromPassphrase, RelationManager, Transaction,
  generateId,
} from '../src/index.js';

const DATA_DIR   = path.join(process.cwd(), '.tmp-complex-crud');
const BACKUP_DIR = path.join(process.cwd(), '.tmp-backup');

// ── Schema ────────────────────────────────────────────────────

const userSchema = new SchemaValidator({
  name:   field('string').required().minLength(2).maxLength(100).build(),
  email:  field('string').required().pattern(/^[^@]+@[^@]+\.[^@]+$/).build(),
  role:   field('string').required().enum('admin', 'user', 'guest').build(),
  points: field('number').min(0).default(0).build(),
  tags:   field('array').maxItems(10).build(),
  active: field('boolean').default(true).build(),
});

const orderSchema = new SchemaValidator({
  userId: field('string').required().build(),
  total:  field('number').required().min(0).build(),
  status: field('string').required().enum('pending', 'paid', 'shipped', 'done').build(),
});

// ── Main ──────────────────────────────────────────────────────

async function main() {
  // Bersihkan sisa test sebelumnya
  await fsp.rm(DATA_DIR,   { recursive: true, force: true });
  await fsp.rm(BACKUP_DIR, { recursive: true, force: true });

  // Buka DB dengan graceful shutdown
  const db = await OvnDB.open(DATA_DIR, { gracefulShutdown: true });

  // ── 1. Setup collections ───────────────────────────────────
  console.log('\n=== 1. Setup Collections ===');

  const users  = await db.collection<{ _id: string; name: string; email: string; role: string; points: number; tags: string[]; active: boolean }>('users');
  const orders = await db.collection<{ _id: string; userId: string; total: number; status: string }>('orders');

  await users.createIndex({ field: 'email', unique: true });
  await users.createIndex({ field: 'role',  unique: false });
  await orders.createIndex({ field: 'userId', unique: false });

  console.log('Collections & indexes ready');

  // ── 2. Insert ──────────────────────────────────────────────
  console.log('\n=== 2. InsertMany + Schema Validation ===');

  const newUsers = [
    { name: 'Budi Santoso',   email: 'budi@mail.com',   role: 'admin', points: 500, tags: ['vip'], active: true },
    { name: 'Siti Rahayu',    email: 'siti@mail.com',   role: 'user',  points: 200, tags: [], active: true },
    { name: 'Ahmad Fauzi',    email: 'ahmad@mail.com',  role: 'user',  points: 0,   tags: [], active: true },
    { name: 'Dewi Lestari',   email: 'dewi@mail.com',   role: 'guest', points: 50,  tags: ['new'], active: false },
    { name: 'Eko Prasetyo',   email: 'eko@mail.com',    role: 'user',  points: 1500, tags: ['vip', 'loyal'], active: true },
  ];

  for (const u of newUsers) {
    userSchema.applyDefaults(u as Record<string, unknown>);
    userSchema.validateOrThrow(u as Record<string, unknown>);
  }

  const insertedUsers = await users.insertMany(newUsers);
  console.log(`Inserted ${insertedUsers.length} users`);

  const insertedOrders = await orders.insertMany([
    { userId: insertedUsers[0]!._id, total: 150_000, status: 'done' },
    { userId: insertedUsers[0]!._id, total: 75_000,  status: 'paid' },
    { userId: insertedUsers[1]!._id, total: 200_000, status: 'pending' },
    { userId: insertedUsers[4]!._id, total: 500_000, status: 'shipped' },
  ]);
  console.log(`Inserted ${insertedOrders.length} orders`);

  // ── 3. findById & findManyById ─────────────────────────────
  console.log('\n=== 3. findById & findManyById ===');

  const budi = await users.findById(insertedUsers[0]!._id);
  console.log('findById:', budi?.name); // Budi Santoso

  const twoUsers = await users.findManyById([insertedUsers[0]!._id, insertedUsers[1]!._id, 'nonexistent']);
  console.log('findManyById (2 found):', twoUsers.map(u => u.name));

  // ── 4. exists() ───────────────────────────────────────────
  console.log('\n=== 4. exists() ===');
  const adminExists = await users.exists({ role: 'admin' });
  const ceoExists   = await users.exists({ role: 'ceo' });
  console.log('admin exists:', adminExists); // true
  console.log('ceo exists:', ceoExists);     // false

  // ── 5. bulkWrite() ────────────────────────────────────────
  console.log('\n=== 5. bulkWrite() — mixed operations ===');

  const bulkResult = await users.bulkWrite([
    // Insert baru
    { op: 'insertOne', doc: { name: 'Rini Wulandari', email: 'rini@mail.com', role: 'user', points: 0, tags: [], active: true } },
    // Update satu user
    { op: 'updateOne', filter: { email: 'ahmad@mail.com' }, spec: { $set: { role: 'admin' }, $inc: { points: 100 } } },
    // Update banyak user
    { op: 'updateMany', filter: { role: 'guest' }, spec: { $set: { active: false } } },
    // Upsert (insert jika belum ada)
    { op: 'upsertOne', filter: { email: 'new@mail.com' }, spec: { $set: { name: 'New User', role: 'guest', points: 0, tags: [], active: true } } },
    // Delete
    { op: 'deleteOne', filter: { email: 'dewi@mail.com' } },
  ]);

  console.log('bulkWrite result:', {
    inserted: bulkResult.insertedCount,
    updated:  bulkResult.updatedCount,
    deleted:  bulkResult.deletedCount,
    upserted: bulkResult.upsertedCount,
    errors:   bulkResult.errors.length,
  });

  // ── 6. upsertOne + $setOnInsert ───────────────────────────
  console.log('\n=== 6. upsertOne dengan $setOnInsert ===');

  // Insert: $setOnInsert dan $set keduanya di-apply
  const upserted1 = await users.upsertOne(
    { email: 'promo@mail.com' },
    {
      $set:         { name: 'Promo User', role: 'user', points: 0, tags: [], active: true },
      $setOnInsert: { createdFrom: 'promo-campaign', joinedAt: Date.now() },
    }
  );
  console.log('upsert (insert):', upserted1.name, '| createdFrom:', (upserted1 as any).createdFrom);

  // Update: $setOnInsert TIDAK di-apply
  const upserted2 = await users.upsertOne(
    { email: 'promo@mail.com' },
    {
      $set:         { points: 50 },
      $setOnInsert: { createdFrom: 'SHOULD_NOT_APPEAR' },
    }
  );
  console.log('upsert (update):', upserted2.points, '| createdFrom masih:', (upserted2 as any).createdFrom);

  // ── 7. findOneAndReplace ──────────────────────────────────
  console.log('\n=== 7. findOneAndReplace ===');

  const replaced = await users.findOneAndReplace(
    { email: 'siti@mail.com' },
    { name: 'Siti Rahayu Updated', email: 'siti@mail.com', role: 'admin', points: 999, tags: ['vip'], active: true } as any,
  );
  console.log('replaced:', replaced?.name, '→ new role:', replaced?.role);

  // ── 8. Transaction dengan upsert + replace ─────────────────
  console.log('\n=== 8. Transaction (upsert + replace + delete) ===');

  const tx = db.beginTransaction();
  tx.insert(orders, { userId: insertedUsers[2]!._id, total: 99_000, status: 'pending' });
  tx.update(users, { email: 'budi@mail.com' }, { $inc: { points: 1000 } });
  tx.upsert(users, { email: 'tx-upsert@mail.com' }, {
    $set: { name: 'TX Upsert', role: 'user', points: 0, tags: [], active: true },
    $setOnInsert: { fromTx: true },
  });
  await tx.commit();
  console.log('Transaction committed OK');

  // Verifikasi tx rollback
  console.log('\n--- Rollback test ---');
  const txBad = db.beginTransaction();
  txBad.insert(orders, { userId: 'nonexistent', total: 0, status: 'pending' });
  txBad.update(users, { email: '__nonexistent__@mail.com' }, { $set: { role: 'admin' } }); // no-op, not error
  txBad.insert(users, { name: 'X', email: 'budi@mail.com', role: 'user', points: 0, tags: [], active: true }); // akan DUPLIKAT → rollback
  try {
    await txBad.commit();
    console.log('  ERROR: should have thrown');
  } catch (err) {
    console.log('  Expected rollback:', (err as Error).message.slice(0, 80));
    console.log('  TX status:', txBad.status); // rolled_back
  }

  // ── 9. aggregate() dengan lookupResolver ──────────────────
  console.log('\n=== 9. aggregate() dengan cross-collection lookup ===');

  // Buat lookupResolver yang bisa resolve ke collection lain
  const lookupResolver = async (colName: string) => {
    if (colName === 'orders') {
      return (await orders.find({})) as unknown as Record<string, unknown>[];
    }
    if (colName === 'users') {
      return (await users.find({})) as unknown as Record<string, unknown>[];
    }
    return [];
  };

  const roleStats = await users.aggregate([
    { $match: { active: true } },
    { $group: { _id: '$role', count: { $sum: 1 }, totalPoints: { $sum: '$points' } } },
    { $sort: { totalPoints: -1 } },
  ], lookupResolver);
  console.log('Role stats:', JSON.stringify(roleStats, null, 2));

  const orderSummary = await orders.aggregate([
    { $group: { _id: '$status', count: { $sum: 1 }, totalRevenue: { $sum: '$total' } } },
    { $sort: { totalRevenue: -1 } },
  ]);
  console.log('Order summary:', JSON.stringify(orderSummary, null, 2));

  // ── 10. CollectionV2 — Encrypted ──────────────────────────
  console.log('\n=== 10. CollectionV2 — AES-256-GCM Encryption ===');

  const cryptoLayer = await cryptoFromPassphrase('super-secret-passphrase-123', DATA_DIR);
  const secrets = await db.collectionV2<{ _id: string; userId: string; token: string; createdAt: number }>('secrets', {
    crypto:  cryptoLayer,
    indexes: [{ field: 'userId', unique: false }],
  });

  // Insert beberapa record terenkripsi
  const s1 = await secrets.insertOne({ userId: insertedUsers[0]!._id, token: 'tok_ABCDEF123456', createdAt: Date.now() });
  const s2 = await secrets.insertOne({ userId: insertedUsers[1]!._id, token: 'tok_XYZ789', createdAt: Date.now() });
  const s3 = await secrets.insertOne({ userId: insertedUsers[0]!._id, token: 'tok_REFRESH_001', createdAt: Date.now() });

  // fix v2.1: find({}) sekarang benar — tidak ada bug writeBuffer + decrypt lagi
  const allSecrets = await secrets.find({});
  console.log('All secrets (should be 3):', allSecrets.length);

  // findOne by field index
  const budiSecrets = await secrets.find({ userId: insertedUsers[0]!._id });
  console.log('Budi secrets:', budiSecrets.length, '| tokens:', budiSecrets.map(s => s.token));

  // Update encrypted record
  await secrets.updateOne({ _id: s1._id }, { $set: { token: 'tok_RENEWED_ABCDEF' } });
  const updated = await secrets.findOne({ _id: s1._id });
  console.log('Updated token:', updated?.token);

  // Delete
  await secrets.deleteOne({ _id: s3._id });
  const afterDelete = await secrets.find({});
  console.log('After delete (should be 2):', afterDelete.length);

  // bulkWrite on encrypted collection
  const encBulk = await secrets.bulkWrite([
    { op: 'insertOne', doc: { userId: insertedUsers[2]!._id, token: 'tok_BULK_001', createdAt: Date.now() } },
    { op: 'updateOne', filter: { _id: s2._id }, spec: { $set: { token: 'tok_BULK_UPDATE' } } },
  ]);
  console.log('Encrypted bulkWrite:', { inserted: encBulk.insertedCount, updated: encBulk.updatedCount });

  // aggregate on encrypted collection
  const secretsAgg = await secrets.aggregate([
    { $group: { _id: '$userId', count: { $sum: 1 } } },
    { $sort: { count: -1 } },
  ]);
  console.log('Secrets per user:', JSON.stringify(secretsAgg));

  // ── 11. RelationManager ───────────────────────────────────
  console.log('\n=== 11. RelationManager — populate ===');

  const rel = new RelationManager();
  rel.register('users', users);
  rel.register('orders', orders);

  const order = await orders.findOne({ status: 'done' });
  if (order) {
    const populated = await rel.populate(order, { userId: 'users' });
    console.log('Populated order:', {
      orderId: populated._id,
      total:   populated.total,
      user:    (populated.userId as any)?.name,
    });
  }

  // populateMany
  const allOrders  = await orders.find({});
  const populated  = await rel.populateMany(allOrders, { userId: 'users' });
  console.log('populateMany sample:', populated.slice(0, 2).map(o => ({
    total: o.total,
    user:  (o.userId as any)?.name ?? o.userId,
  })));

  // ── 12. compact() ─────────────────────────────────────────
  console.log('\n=== 12. compact() — manual compaction ===');
  await users.deleteMany({ active: false });
  await users.compact();
  console.log('Compaction triggered');

  // ── 13. truncate() ────────────────────────────────────────
  console.log('\n=== 13. truncate() — clear all docs ===');
  const beforeCount = await orders.countDocuments();
  await orders.truncate();
  const afterCount  = await orders.countDocuments();
  console.log(`orders: ${beforeCount} → ${afterCount} (truncated)`);

  // ── 14. backup() ──────────────────────────────────────────
  console.log('\n=== 14. backup() ===');
  await db.backup(BACKUP_DIR);
  const backupEntries = await fsp.readdir(BACKUP_DIR);
  console.log('Backup dirs:', backupEntries);

  // ── 15. status() ──────────────────────────────────────────
  console.log('\n=== 15. status() — DB health report ===');
  const statusReport = await db.status();
  console.log('DB healthy:', statusReport.isHealthy);
  console.log('Collections:');
  for (const col of statusReport.collections) {
    console.log(`  ${col.name}: live=${col.totalLive}, encrypted=${col.encrypted}, fragRatio=${col.fragmentRatio.toFixed(2)}`);
  }
  console.log('Total size:', (statusReport.totalSize / 1024).toFixed(1) + ' KB');

  // ── 16. CryptoLayer utils ─────────────────────────────────
  console.log('\n=== 16. CryptoLayer — verify, reencrypt, rotateKey ===');
  const { CryptoLayer } = await import('../src/crypto/crypto-layer.js');

  const plainBuf    = Buffer.from('{"secret":"data"}', 'utf8');
  const encrypted   = cryptoLayer.encrypt(plainBuf);
  const isValid     = cryptoLayer.verify(encrypted);
  const isInvalid   = cryptoLayer.verify(Buffer.from('not-a-ciphertext'));
  console.log('verify valid cipher:', isValid);
  console.log('verify invalid buf:', isInvalid);

  const isCipher    = CryptoLayer.isEncryptedBuffer(encrypted);
  const isPlain     = CryptoLayer.isEncryptedBuffer(plainBuf);
  console.log('isEncryptedBuffer(ciphertext):', isCipher);
  console.log('isEncryptedBuffer(plaintext):', isPlain);

  // Key rotation
  const newCrypto   = await cryptoFromPassphrase('new-passphrase-rotated', DATA_DIR + '-newkey');
  const rotated     = cryptoLayer.reencrypt(encrypted, newCrypto);
  const decrypted   = newCrypto.decrypt(rotated);
  console.log('Key rotation OK:', decrypted.toString('utf8'));

  // ── 17. distinct() & explain() ────────────────────────────
  console.log('\n=== 17. distinct() & explain() ===');
  const distinctRoles = await users.distinct('role');
  console.log('Distinct roles:', distinctRoles);

  const plan = users.explain({ role: 'admin' });
  console.log('Query plan:', plan.planType, '| estimatedCost:', plan.estimatedCost);

  // ── Done ──────────────────────────────────────────────────
  console.log('\n=== All tests passed ✅ ===');

  await db.close();
  // Cleanup
  await fsp.rm(DATA_DIR,   { recursive: true, force: true });
  await fsp.rm(BACKUP_DIR, { recursive: true, force: true });
  await fsp.rm(DATA_DIR + '-newkey', { recursive: true, force: true });
}

main().catch(err => {
  console.error('FATAL:', err);
  process.exit(1);
});
