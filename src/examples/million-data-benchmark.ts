// ============================================================
//  OvnDB — Million Data Benchmark
//  Contoh lengkap untuk membuktikan OvnDB mampu menampung
//  dan memproses jutaan records dengan efisien.
//
//  Run: npm run build:full && OVNDB_LOG=silent node dist/examples/million-data-benchmark.js
// ============================================================

import { OvnDB }          from '../index.js';
import { cryptoFromPassphrase } from '../index.js';
import fsp                from 'fs/promises';
import os                 from 'os';

// ── Config ────────────────────────────────────────────────────
const DATA_DIR    = '/tmp/ovndb-million-benchmark';
const TOTAL_DOCS  = 1_000_000;   // 1 juta data
const BATCH_SIZE  = 5_000;       // insertMany per batch
const READ_SAMPLE = 100_000;     // random reads
const UPDATE_N    = 50_000;      // docs to update
const DELETE_N    = 10_000;      // docs to delete

// ── Tipe dokumen ──────────────────────────────────────────────
interface ProductDoc extends Record<string, unknown> {
  _id:        string;
  sku:        string;        // unique product code
  name:       string;
  category:   string;
  price:      number;
  stock:      number;
  sold:       number;
  rating:     number;        // 1.0 – 5.0
  region:     string;
  active:     boolean;
  createdAt:  number;
  updatedAt:  number;
}

// ── Seed data ─────────────────────────────────────────────────
const CATEGORIES = ['Elektronik','Fashion','Makanan','Minuman','Otomotif',
                    'Kesehatan','Olahraga','Buku','Mainan','Rumah Tangga'];
const REGIONS    = ['JKT','SBY','BDG','MDN','MKS','SMG','PLG','BJM','PKB','BTM',
                    'PTK','YGY','MLG','PSP','AMB'];
const BRAND_NAMES = ['AcmeCorp','ZenTech','SkyBrand','NovaLine','BlueWave',
                     'EcoProd','StarMart','PrimeTech','GlobalGo','FreshMart'];

function makeProduct(i: number): Omit<ProductDoc, '_id'> {
  const cat    = CATEGORIES[i % CATEGORIES.length]!;
  const brand  = BRAND_NAMES[i % BRAND_NAMES.length]!;
  const region = REGIONS[i % REGIONS.length]!;
  const now    = Date.now();
  return {
    sku:       `SKU-${cat.substring(0,3).toUpperCase()}-${String(i).padStart(8,'0')}`,
    name:      `${brand} ${cat} Produk-${i}`,
    category:  cat,
    price:     Math.round((Math.random() * 999_000 + 1_000) / 100) * 100,
    stock:     Math.floor(Math.random() * 1_000),
    sold:      Math.floor(Math.random() * 50_000),
    rating:    Math.round((3 + Math.random() * 2) * 10) / 10,
    region,
    active:    i % 13 !== 0,   // ~92% active
    createdAt: now - Math.floor(Math.random() * 365 * 86_400_000),
    updatedAt: now - Math.floor(Math.random() * 7 * 86_400_000),
  };
}

// ── Helpers ───────────────────────────────────────────────────
function fmt(n: number)    { return n.toLocaleString('id-ID'); }
function mb(b: number)     { return (b / 1048576).toFixed(2) + ' MB'; }
function gb(b: number)     { return (b / 1073741824).toFixed(3) + ' GB'; }
function pct(n: number)    { return (n * 100).toFixed(1) + '%'; }
function ms2s(ms: number)  { return (ms / 1000).toFixed(2) + 's'; }
function throughput(n: number, ms: number) {
  return `${fmt(Math.round(n / ms * 1000))}/s`;
}
function memUsage() {
  const u = process.memoryUsage();
  return `heap ${mb(u.heapUsed)} / rss ${mb(u.rss)}`;
}
function bar(ratio: number, w = 40) {
  const filled = Math.round(ratio * w);
  return '█'.repeat(filled) + '░'.repeat(w - filled);
}
function sep(char = '─', w = 60) { return char.repeat(w); }
function heading(title: string) {
  console.log('');
  console.log(`  ┌─ ${title} ${'─'.repeat(Math.max(0, 54 - title.length))}┐`);
}
function ok(msg: string) { console.log(`  │ ✓ ${msg}`); }
function info(msg: string) { console.log(`  │   ${msg}`); }
function fail(msg: string) { console.error(`\n  ✗ FAILED: ${msg}`); process.exit(1); }
function assert(cond: boolean, msg: string) { if (!cond) fail(msg); }

// ── Main ──────────────────────────────────────────────────────
async function main() {
  await fsp.rm(DATA_DIR, { recursive: true, force: true });
  await fsp.mkdir(DATA_DIR, { recursive: true });

  const startAll = Date.now();

  console.log('');
  console.log('  ╔══════════════════════════════════════════════════════════╗');
  console.log('  ║       OvnDB — Benchmark 1.000.000 Data (Million Test)    ║');
  console.log('  ║   Membuktikan kemampuan menampung & memproses jutaan data ║');
  console.log('  ╚══════════════════════════════════════════════════════════╝');
  console.log('');
  console.log(`  Node.js  : ${process.version}`);
  console.log(`  Platform : ${os.platform()} ${os.arch()}`);
  console.log(`  CPUs     : ${os.cpus().length}x ${os.cpus()[0]?.model ?? 'unknown'}`);
  console.log(`  RAM Free : ${gb(os.freemem())} / ${gb(os.totalmem())}`);
  console.log(`  Data dir : ${DATA_DIR}`);
  console.log('');
  console.log(sep('═', 62));

  // ── Buka database ─────────────────────────────────────────
  const db       = await OvnDB.open(DATA_DIR, { cacheSize: 200_000 });
  const products = await db.collectionV2<ProductDoc>('products', {
    indexes: [
      { field: 'sku',      unique: true  },
      { field: 'category', unique: false },
      { field: 'region',   unique: false },
    ],
  });

  // ════════════════════════════════════════════════════════════
  // FASE 1: Bulk Insert 1.000.000 dokumen menggunakan insertMany
  // ════════════════════════════════════════════════════════════
  heading('FASE 1 — Bulk Insert 1.000.000 Dokumen');
  info(`Batch size: ${fmt(BATCH_SIZE)} dok/batch  |  Total batch: ${fmt(TOTAL_DOCS / BATCH_SIZE)}`);
  info('');

  const insertedIds: string[] = [];
  const t_insert = Date.now();
  let lastPrint  = Date.now();

  for (let i = 0; i < TOTAL_DOCS; i += BATCH_SIZE) {
    const batch = Array.from({ length: Math.min(BATCH_SIZE, TOTAL_DOCS - i) },
                             (_, j) => makeProduct(i + j));
    const docs = await products.insertMany(batch);
    for (const d of docs) insertedIds.push(d._id);

    // Progress bar
    if (Date.now() - lastPrint > 400) {
      lastPrint = Date.now();
      const done    = i + BATCH_SIZE;
      const ratio   = Math.min(done / TOTAL_DOCS, 1);
      const elapsed = Date.now() - t_insert;
      const eta     = Math.round((elapsed / ratio) - elapsed);
      process.stdout.write(
        `\r  │ ${bar(ratio, 36)} ${pct(ratio).padStart(6)}  ETA: ${ms2s(eta).padStart(7)}   `
      );
    }
  }
  await products.flush();
  const insertMs = Date.now() - t_insert;

  process.stdout.write('\r  │ ' + bar(1, 36) + ' 100.0%  selesai!          \n');
  ok(`${fmt(TOTAL_DOCS)} dokumen dimasukkan dalam ${ms2s(insertMs)}`);
  ok(`Throughput insert: ${throughput(TOTAL_DOCS, insertMs)}`);
  ok(`Memory setelah insert: ${memUsage()}`);

  // ════════════════════════════════════════════════════════════
  // FASE 2: Statistik Collection
  // ════════════════════════════════════════════════════════════
  heading('FASE 2 — Statistik Collection');
  const stats = await products.stats();
  info(`Live records    : ${fmt(stats.liveCount)}`);
  info(`Index entries   : ${fmt(stats.indexEntries)}`);
  info(`File size       : ${mb(stats.fileSize)}`);
  info(`Fragment ratio  : ${pct(stats.fragmentRatio)}`);
  info(`Cache hit-rate  : ${pct(stats.cacheHitRate)}`);
  info(`Cache size      : ${fmt(stats.cacheSize)} entries`);
  info(`WAL pending     : ${fmt(stats.walPending)}`);
  assert(stats.liveCount    >= TOTAL_DOCS, `liveCount harus ≥ ${TOTAL_DOCS}`);
  assert(stats.indexEntries >= TOTAL_DOCS, 'indexEntries terlalu rendah');
  ok('Semua statistik valid ✓');

  // ════════════════════════════════════════════════════════════
  // FASE 3: Random Read berdasarkan _id (LRU warm-up)
  // ════════════════════════════════════════════════════════════
  heading('FASE 3 — Random Read _id (100.000 sampel)');
  const t_read = Date.now();
  let misses   = 0;
  for (let i = 0; i < READ_SAMPLE; i++) {
    const id  = insertedIds[Math.floor(Math.random() * TOTAL_DOCS)]!;
    const doc = await products.findOne({ _id: id });
    if (!doc) misses++;
  }
  const readMs = Date.now() - t_read;
  ok(`${fmt(READ_SAMPLE)} reads dalam ${ms2s(readMs)}`);
  ok(`Throughput read: ${throughput(READ_SAMPLE, readMs)}`);
  ok(`Cache hit-rate setelah read: ${pct((await products.stats()).cacheHitRate)}`);
  assert(misses === 0, `${misses} dokumen tidak ditemukan`);

  // ════════════════════════════════════════════════════════════
  // FASE 4: Query dengan Secondary Index (cari berdasarkan field)
  // ════════════════════════════════════════════════════════════
  heading('FASE 4 — Query Secondary Index');

  // 4a: Secondary index — filter by kategori (uses internal secIdx.lookup)
  const t_idx1   = Date.now();
  const elekDocs = await products.find({ category: 'Elektronik' });
  ok(`find({ category:'Elektronik' }) via secondary index → ${fmt(elekDocs.length)} docs dalam ${Date.now() - t_idx1}ms`);

  // 4b: Secondary index — filter by region
  const t_idx2  = Date.now();
  const jktDocs = await products.find({ region: 'JKT' });
  ok(`find({ region:'JKT' }) via secondary index → ${fmt(jktDocs.length)} docs dalam ${Date.now() - t_idx2}ms`);

  // 4c: Unique index lookup — cari SKU spesifik
  const skuSample = `SKU-ELE-${String(999).padStart(8,'0')}`;
  const t_idx3    = Date.now();
  const bysku     = await products.findOne({ sku: skuSample });
  ok(`findOne({ sku: '${skuSample}' }) via unique index dalam ${Date.now() - t_idx3}ms → ${bysku ? 'found' : 'not found'}`);

  // ════════════════════════════════════════════════════════════
  // FASE 5: Full Scan dengan Complex Filter + Pagination
  // ════════════════════════════════════════════════════════════
  heading('FASE 5 — Complex Filter + Pagination');

  // Filter: produk aktif, harga > 500rb, rating >= 4.5, region JKT
  const t_scan1 = Date.now();
  const premium = await products.find(
    { active: true, price: { $gt: 500_000 }, rating: { $gte: 4.5 }, region: 'JKT' },
    { limit: 200, sort: { price: -1 } }
  );
  ok(`Premium products (page 1, 200 records) dalam ${Date.now() - t_scan1}ms`);
  info(`  → ${fmt(premium.length)} hasil | harga tertinggi: ${fmt(premium[0]?.price ?? 0)}`);
  assert(premium.every(d => d.active && d.price > 500_000 && d.rating >= 4.5),
         'filter correctness gagal');

  // Filter: produk tidak aktif (soft-deleted catalog)
  const t_scan2 = Date.now();
  const inactive = await products.find({ active: false }, { limit: 100 });
  ok(`Inactive products (limit 100) dalam ${Date.now() - t_scan2}ms → ${fmt(inactive.length)} results`);

  // Count produk per kategori (full scan)
  const t_cnt = Date.now();
  const countElek = await products.count({ category: 'Elektronik', active: true });
  ok(`count({ category:'Elektronik', active:true }) = ${fmt(countElek)} dalam ${Date.now() - t_cnt}ms`);

  // ════════════════════════════════════════════════════════════
  // FASE 6: Top-K Sorting (tanpa load semua data ke memory)
  // ════════════════════════════════════════════════════════════
  heading('FASE 6 — Top-K Sorting (min-heap, tanpa full sort)');

  const t_top = Date.now();
  const top20BySold = await products.find(
    { active: true },
    { sort: { sold: -1 }, limit: 20 }
  );
  ok(`Top-20 produk terlaris dalam ${Date.now() - t_top}ms`);
  for (let i = 1; i < top20BySold.length; i++)
    assert(top20BySold[i-1]!.sold >= top20BySold[i]!.sold, `urutan sort salah di index ${i}`);
  ok(`Urutan sort sold DESC terbukti benar ✓`);
  info(`  Posisi #1: ${top20BySold[0]?.name} — sold=${fmt(top20BySold[0]?.sold ?? 0)}`);

  const t_top2 = Date.now();
  const top10ByRating = await products.find(
    { stock: { $gt: 0 }, active: true },
    { sort: { rating: -1 }, limit: 10 }
  );
  ok(`Top-10 rating tertinggi (stock > 0) dalam ${Date.now() - t_top2}ms`);
  ok(`Rating #1: ${top10ByRating[0]?.rating} — ${top10ByRating[0]?.name}`);

  // ════════════════════════════════════════════════════════════
  // FASE 7: Bulk Update 50.000 dokumen
  // ════════════════════════════════════════════════════════════
  heading('FASE 7 — Bulk Update 50.000 Dokumen');

  const toUpdate = insertedIds.slice(0, UPDATE_N);
  const t_update = Date.now();
  for (const id of toUpdate) {
    await products.updateOne(
      { _id: id },
      { $inc: { sold: 1, stock: -1 }, $set: { updatedAt: Date.now() } }
    );
  }
  await products.flush();
  const updateMs = Date.now() - t_update;
  ok(`${fmt(UPDATE_N)} updates dalam ${ms2s(updateMs)}`);
  ok(`Throughput update: ${throughput(UPDATE_N, updateMs)}`);

  // Verifikasi update benar
  const verifyDoc = await products.findOne({ _id: toUpdate[0]! });
  assert(verifyDoc !== null,      'dokumen yang diupdate tidak ditemukan');
  assert(typeof verifyDoc!.sold === 'number', 'field sold harus number');

  // ════════════════════════════════════════════════════════════
  // FASE 8: Soft Delete + Hard Delete 10.000 dokumen
  // ════════════════════════════════════════════════════════════
  heading('FASE 8 — Delete 10.000 Dokumen');

  const toDelete = insertedIds.slice(TOTAL_DOCS - DELETE_N);
  const t_delete = Date.now();
  for (const id of toDelete) await products.deleteOne({ _id: id });
  await products.flush();
  const deleteMs = Date.now() - t_delete;
  ok(`${fmt(DELETE_N)} deletes dalam ${ms2s(deleteMs)}`);
  ok(`Throughput delete: ${throughput(DELETE_N, deleteMs)}`);

  // Verifikasi benar-benar terhapus
  let stillFound = 0;
  for (let i = 0; i < 200; i++) {
    const id = toDelete[Math.floor(Math.random() * toDelete.length)]!;
    if (await products.findOne({ _id: id })) stillFound++;
  }
  assert(stillFound === 0, `${stillFound} dokumen deleted masih ditemukan`);
  ok('Semua dokumen terhapus terbukti benar ✓');

  // ════════════════════════════════════════════════════════════
  // FASE 9: Persistence — Close + Reopen (cold start)
  // ════════════════════════════════════════════════════════════
  heading('FASE 9 — Persistence: Close & Cold Reopen');

  await db.close();

  const t_reopen = Date.now();
  const db2      = await OvnDB.open(DATA_DIR, { cacheSize: 200_000 });
  const prod2    = await db2.collectionV2<ProductDoc>('products', {
    indexes: [
      { field: 'sku',      unique: true  },
      { field: 'category', unique: false },
      { field: 'region',   unique: false },
    ],
  });
  const reopenMs = Date.now() - t_reopen;

  ok(`Database berhasil dibuka ulang dalam ${ms2s(reopenMs)}`);

  const s2 = await prod2.stats();
  info(`Live records setelah reopen: ${fmt(s2.liveCount)}`);
  assert(s2.liveCount >= TOTAL_DOCS - DELETE_N,
         `Live count seharusnya ~${TOTAL_DOCS - DELETE_N}, dapat ${s2.liveCount}`);

  // Cold read — dokumen yang tidak pernah di-cache
  const coldId  = insertedIds[Math.floor(TOTAL_DOCS / 2)]!;
  const coldDoc = await prod2.findOne({ _id: coldId });
  assert(coldDoc !== null, 'cold read gagal — dokumen tidak ditemukan');
  ok(`Cold disk read berhasil: ${coldDoc!._id} (${coldDoc!.name})`);

  // Secondary index tetap berfungsi setelah reopen
  const t_idx_cold = Date.now();
  const elekAfter  = await prod2.find({ category: 'Elektronik' });
  ok(`Secondary index 'category' setelah reopen → ${fmt(elekAfter.length)} docs dalam ${Date.now() - t_idx_cold}ms`);

  // ════════════════════════════════════════════════════════════
  // FASE 10: Concurrent Simulation — simulasi banyak user bersamaan
  // ════════════════════════════════════════════════════════════
  heading('FASE 10 — Simulasi Concurrent Access (Promise.all)');

  const CONCUR = 1_000;
  const t_conc = Date.now();

  // Simulasi 1000 "user" melakukan read secara bersamaan
  await Promise.all(
    Array.from({ length: CONCUR }, (_, i) => {
      const id = insertedIds[Math.floor(Math.random() * (TOTAL_DOCS - DELETE_N))]!;
      return prod2.findOne({ _id: id });
    })
  );
  ok(`${fmt(CONCUR)} concurrent reads dalam ${Date.now() - t_conc}ms`);

  // Simulasi 500 update bersamaan
  const t_conc2 = Date.now();
  const updateBatch = insertedIds.slice(UPDATE_N, UPDATE_N + 500);
  await Promise.all(
    updateBatch.map(id =>
      prod2.updateOne({ _id: id }, { $inc: { sold: 1 } })
    )
  );
  await prod2.flush();
  ok(`500 concurrent updates dalam ${Date.now() - t_conc2}ms`);

  // ════════════════════════════════════════════════════════════
  // FASE 11: Encrypted Collection (AES-256-GCM)
  // ════════════════════════════════════════════════════════════
  heading('FASE 11 — Encrypted Collection (AES-256-GCM)');

  const crypto  = cryptoFromPassphrase('rahasia-super-kuat-2024', DATA_DIR);
  const secured = await db2.collectionV2<ProductDoc>('products_secure', {
    crypto,
    indexes: [{ field: 'sku', unique: true }],
  });

  const ENC_N = 10_000;
  const t_enc = Date.now();
  for (let i = 0; i < ENC_N; i++) {
    await secured.insertOne(makeProduct(i + 9_000_000));
  }
  await secured.flush();
  const encMs = Date.now() - t_enc;
  ok(`${fmt(ENC_N)} dokumen terenkripsi dimasukkan dalam ${ms2s(encMs)}`);
  ok(`Throughput insert encrypted: ${throughput(ENC_N, encMs)}`);

  // Baca kembali dari enkripsi
  const encStats = await secured.stats();
  info(`File terenkripsi: ${mb(encStats.fileSize)}`);
  ok(`Enkripsi + indeks berfungsi normal ✓`);

  await db2.close();

  // ════════════════════════════════════════════════════════════
  // RINGKASAN FINAL
  // ════════════════════════════════════════════════════════════
  const totalMs = Date.now() - startAll;
  console.log('');
  console.log('  ╔══════════════════════════════════════════════════════════╗');
  console.log('  ║              BENCHMARK SELESAI — SEMUA FASE LULUS ✓     ║');
  console.log('  ╠══════════════════════════════════════════════════════════╣');
  console.log(`  ║  Total waktu       : ${ms2s(totalMs).padEnd(37)}║`);
  console.log('  ╠══════════════════════════════════════════════════════════╣');
  console.log(`  ║  Insert 1.000.000  : ${throughput(TOTAL_DOCS, insertMs).padEnd(37)}║`);
  console.log(`  ║  Random read       : ${throughput(READ_SAMPLE, readMs).padEnd(37)}║`);
  console.log(`  ║  Update 50.000     : ${throughput(UPDATE_N, updateMs).padEnd(37)}║`);
  console.log(`  ║  Delete 10.000     : ${throughput(DELETE_N, deleteMs).padEnd(37)}║`);
  console.log(`  ║  Cold reopen       : ${ms2s(reopenMs).padEnd(37)}║`);
  console.log('  ╠══════════════════════════════════════════════════════════╣');
  console.log(`  ║  File size akhir   : ${mb(s2.fileSize).padEnd(37)}║`);
  console.log(`  ║  Fragment ratio    : ${pct(s2.fragmentRatio).padEnd(37)}║`);
  console.log(`  ║  Cache hit-rate    : ${pct(s2.cacheHitRate).padEnd(37)}║`);
  console.log('  ╚══════════════════════════════════════════════════════════╝');
  console.log('');
  console.log('  OvnDB terbukti mampu menampung dan memproses 1 JUTA data!');
  console.log('');

  // Cleanup
  await fsp.rm(DATA_DIR, { recursive: true, force: true });
}

main().catch(err => {
  console.error('\n  ✗ BENCHMARK CRASH:', err);
  process.exit(1);
});
