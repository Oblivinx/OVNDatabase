import { OvnDB } from '../src/index.js';
import fsp from 'fs/promises';
import os from 'os';

// ── Config ────────────────────────────────────────────────────
const DATA_DIR = '/tmp/ovndb-stress-test';
const TOTAL_DOCS = parseInt(process.env.STRESS_N ?? '10000000', 10); // Default 10 Juta
const BATCH_SIZE = 10_000;
const READ_SAMPLE = 50_000;
const UPDATE_N = 50_000;
const DELETE_N = 10_000;

interface LogDoc extends Record<string, unknown> {
  _id: string;
  level: string;
  service: string;
  message: string;
  timestamp: number;
}

const LEVELS = ['INFO', 'WARN', 'ERROR', 'DEBUG'];
const SERVICES = ['api-gateway', 'auth-service', 'payment-service', 'user-service'];

function makeLog(i: number): Omit<LogDoc, '_id'> {
  return {
    level: LEVELS[i % LEVELS.length]!,
    service: SERVICES[i % SERVICES.length]!,
    message: `Log message number ${i} from stress test generation.`,
    timestamp: Date.now() - (i * 100),
  };
}

// ── Helpers ───────────────────────────────────────────────────
const fmt = (n: number) => n.toLocaleString('id-ID');
const mb = (b: number) => (b / 1048576).toFixed(2) + ' MB';
const pct = (n: number) => (n * 100).toFixed(1) + '%';
const ms2s = (ms: number) => (ms / 1000).toFixed(2) + 's';
const tps = (n: number, ms: number) => `${fmt(Math.round((n / ms) * 1000))}/s`;

function memUsage() {
  const u = process.memoryUsage();
  return `heap: ${mb(u.heapUsed)} / rss: ${mb(u.rss)}`;
}

async function main() {
  await fsp.rm(DATA_DIR, { recursive: true, force: true });
  await fsp.mkdir(DATA_DIR, { recursive: true });

  console.log(`\n======================================================`);
  console.log(` OvnDB — EXTREME STRESS TEST (${fmt(TOTAL_DOCS)} Dokumen)`);
  console.log(`======================================================`);
  console.log(`Node.js  : ${process.version}`);
  console.log(`RAM Free : ${mb(os.freemem())}`);
  console.log(`Data Dir : ${DATA_DIR}`);
  console.log(`======================================================\n`);

  const startAll = Date.now();

  // Buka OvnDB dengan Cache yang wajar untuk puluhan juta data
  const db = await OvnDB.open(DATA_DIR, { cacheSize: 500_000 });
  const logs = await db.collection<LogDoc>('logs');

  // FASE 1: BULK INSERT
  console.log(`[FASE 1] Menulis ${fmt(TOTAL_DOCS)} dokumen... (Batch=${fmt(BATCH_SIZE)})`);
  const t_insert = Date.now();
  
  let insertedIds: string[] = []; // Simpan sebagian ID untuk test baca acak
  let lastPrint = Date.now();

  for (let i = 0; i < TOTAL_DOCS; i += BATCH_SIZE) {
    const batchSize = Math.min(BATCH_SIZE, TOTAL_DOCS - i);
    const batch = Array.from({ length: batchSize }, (_, j) => makeLog(i + j));
    const docs = await logs.insertMany(batch);
    
    // Simpan 1 setiap 1000 insert ke memori untuk random sampling di fase read
    for (let k = 0; k < docs.length; k += 1000) {
      insertedIds.push(docs[k]!._id);
    }

    if (Date.now() - lastPrint > 1000) {
      lastPrint = Date.now();
      const p = ((i + batchSize) / TOTAL_DOCS) * 100;
      process.stdout.write(`\r  ➜ Progress: ${p.toFixed(1)}% | Memori: ${memUsage()}`);
    }
  }
  await logs.flush();
  const insertMs = Date.now() - t_insert;
  process.stdout.write(`\r  ➜ Selesai! Waktu: ${ms2s(insertMs)} | Throughput: ${tps(TOTAL_DOCS, insertMs)}\n`);

  // FASE 2: STATISTIK
  console.log(`\n[FASE 2] Melihat Statistik Database...`);
  const stats = await logs.stats();
  console.log(`  Live count : ${fmt(stats.liveCount)}`);
  console.log(`  File size  : ${mb(stats.fileSize)}`);
  console.log(`  Cache hit  : ${pct(stats.cacheHitRate)}`);

  // FASE 3: RANDOM READ
  console.log(`\n[FASE 3] Membaca acak ${fmt(READ_SAMPLE)} dokumen (Disk / LRU)...`);
  const t_read = Date.now();
  let misses = 0;
  for (let i = 0; i < READ_SAMPLE; i++) {
    const id = insertedIds[Math.floor(Math.random() * insertedIds.length)]!;
    const doc = await logs.findOne({ _id: id });
    if (!doc) misses++;
  }
  const readMs = Date.now() - t_read;
  console.log(`  ➜ Selesai! Waktu: ${ms2s(readMs)} | Throughput: ${tps(READ_SAMPLE, readMs)} | Miss: ${misses}`);

  // FASE 4: UPDATE BESAR
  console.log(`\n[FASE 4] Meng-update ${fmt(UPDATE_N)} dokumen secara konkuren...`);
  const t_update = Date.now();
  const updatePromises = [];
  for (let i = 0; i < UPDATE_N; i++) {
    const id = insertedIds[i % insertedIds.length]!;
    updatePromises.push(logs.updateOne({ _id: id }, { $set: { message: "UPDATED!" } }));
  }
  await Promise.all(updatePromises);
  await logs.flush();
  const updateMs = Date.now() - t_update;
  console.log(`  ➜ Selesai! Waktu: ${ms2s(updateMs)} | Throughput: ${tps(UPDATE_N, updateMs)}`);

  // FASE 5: DELETE BESAR
  console.log(`\n[FASE 5] Menghapus ${fmt(DELETE_N)} dokumen secara berurutan...`);
  const t_delete = Date.now();
  for (let i = 0; i < DELETE_N; i++) {
    const id = insertedIds[insertedIds.length - 1 - i]!;
    await logs.deleteOne({ _id: id });
  }
  await logs.flush();
  const deleteMs = Date.now() - t_delete;
  console.log(`  ➜ Selesai! Waktu: ${ms2s(deleteMs)} | Throughput: ${tps(DELETE_N, deleteMs)}`);

  console.log(`\n======================================================`);
  const totalMs = Date.now() - startAll;
  console.log(` 🎉 TOTAL WAKTU STRESS TEST: ${ms2s(totalMs)}`);
  console.log(`======================================================\n`);

  await db.close();
}

main().catch(console.error);
