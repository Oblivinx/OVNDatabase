// ============================================================
//  Benchmark — OvnDB v2.0 Throughput & Latency
//
//  Jalankan dengan:
//    npx tsx test/benchmark/throughput.benchmark.ts
//
//  Mengukur:
//   - Insert throughput (ops/s)
//   - Point read throughput (cache hit vs cold read)
//   - Update throughput
//   - Delete throughput
//   - Full scan latency
//   - find() dengan filter latency
//   - Aggregation latency
//
//  Output berupa tabel yang mudah dibaca.
// ============================================================

import os   from 'node:os';
import path from 'node:path';
import fsp  from 'node:fs/promises';
import { OvnDB } from '../../src/index.js';

interface BenchDoc {
  _id:    string;
  name:   string;
  city:   string;
  points: number;
  active: boolean;
  ts:     number;
}

const CITIES   = ['Jakarta', 'Bandung', 'Surabaya', 'Medan', 'Makassar'];
const NAMES    = ['Budi', 'Siti', 'Ahmad', 'Dewi', 'Rudi', 'Rina', 'Andi', 'Yeni'];
const N        = parseInt(process.env['BENCH_N'] ?? '100000', 10);
const BATCH    = 500; // flush setiap N insert

// ── Helper ────────────────────────────────────────────────────

function randomDoc(i: number): Omit<BenchDoc, '_id'> & { _id: string } {
  return {
    _id:    `doc-${String(i).padStart(9, '0')}`,
    name:   NAMES[i % NAMES.length]!,
    city:   CITIES[i % CITIES.length]!,
    points: Math.floor(Math.random() * 10000),
    active: i % 3 !== 0,
    ts:     Date.now(),
  };
}

async function measure<T>(label: string, fn: () => Promise<T>): Promise<{ result: T; ms: number }> {
  const start  = performance.now();
  const result = await fn();
  const ms     = performance.now() - start;
  return { result, ms };
}

function printResult(label: string, ms: number, count?: number): void {
  const ops = count !== undefined ? Math.round(count / (ms / 1000)) : null;
  const opsStr = ops !== null ? `${ops.toLocaleString()} ops/s` : '';
  console.log(
    `  ${label.padEnd(35)} ${String(Math.round(ms)).padStart(7)} ms   ${opsStr}`
  );
}

// ── Main Benchmark ────────────────────────────────────────────

async function run(): Promise<void> {
  const tmpDir = await fsp.mkdtemp(path.join(os.tmpdir(), 'ovndb-bench-'));

  try {
    console.log('\n╔══════════════════════════════════════════════════════╗');
    console.log(`║  OvnDB v2.0 Benchmark — N = ${N.toLocaleString().padEnd(23)}║`);
    console.log(`║  ${os.cpus()[0]?.model?.slice(0, 50).padEnd(50) ?? 'CPU Info'} ║`);
    console.log('╚══════════════════════════════════════════════════════╝\n');

    const db  = await OvnDB.open(tmpDir, { fileLock: false });
    const col = await db.collection<BenchDoc>('bench');

    console.log('── Write ─────────────────────────────────────────────');

    // 1. Insert — bulk path: tulis langsung ke write buffer tanpa await WAL per record
    {
      const { ms } = await measure('Insert', async () => {
        for (let i = 0; i < N; i++) {
          const doc = randomDoc(i);
          const buf = Buffer.from(JSON.stringify(doc));
          await col.engine.insertBulk(doc._id, buf);
        }
        await col.flush();
      });
      printResult('insertBulk (no WAL per-op)', ms, N);
    }

    // 1b. Insert dengan WAL (durable) — test dengan subset kecil
    {
      const SAMPLE = Math.min(N, 1000);
      // Bersihkan dulu: hapus key yang akan di-test lagi tidak perlu, pakai collection baru
      const col2 = await db.collection<BenchDoc>('bench-wal');
      const { ms } = await measure('Insert WAL', async () => {
        for (let i = 0; i < SAMPLE; i++) {
          await col2.insertOne(randomDoc(i + N));
        }
        await col2.flush();
      });
      printResult(`insertOne (WAL, ${SAMPLE} docs)`, ms, SAMPLE);
    }

    // 2. Update
    {
      const ids    = Array.from({ length: 1000 }, (_, i) => `doc-${String(i).padStart(9,'0')}`);
      const { ms } = await measure('Update', async () => {
        for (const id of ids) {
          await col.updateOne({ _id: id }, { $inc: { points: 1 } });
        }
        await col.flush();
      });
      printResult('updateOne (1k)', ms, 1000);
    }

    // 3. Delete
    {
      const ids    = Array.from({ length: 500 }, (_, i) => `doc-${String(N - i - 1).padStart(9,'0')}`);
      const { ms } = await measure('Delete', async () => {
        for (const id of ids) {
          await col.deleteOne({ _id: id });
        }
        await col.flush();
      });
      printResult('deleteOne (500)', ms, 500);
    }

    console.log('\n── Read (warm cache) ────────────────────────────────');

    // 4. Point read (warm - baru saja di-insert)
    {
      const ids    = Array.from({ length: 5000 }, (_, i) => `doc-${String(i * 10).padStart(9,'0')}`);
      const { ms } = await measure('Read warm', async () => {
        for (const id of ids) await col.findOne({ _id: id });
      });
      printResult('findOne by _id (cache)', ms, 5000);
    }

    console.log('\n── Read (cold / scan) ───────────────────────────────');

    // 5. Full scan
    {
      const { ms } = await measure('Full scan', async () => {
        let count = 0;
        for await (const _ of col['engine'].scan()) count++;
        return count;
      });
      printResult(`Full scan (${N.toLocaleString()} docs)`, ms);
    }

    // 6. find() dengan filter
    {
      const { result: docs, ms } = await measure('Find filter', async () =>
        col.find({ city: 'Jakarta' }, { limit: 100 })
      );
      printResult(`find({city:'Jakarta'}, limit:100)`, ms);
      console.log(`    → returned ${docs.length} docs`);
    }

    // 7. countDocuments
    {
      const { result, ms } = await measure('Count', async () =>
        col.countDocuments({ active: true })
      );
      printResult('countDocuments({active:true})', ms);
      console.log(`    → count: ${result.toLocaleString()}`);
    }

    console.log('\n── Aggregation ──────────────────────────────────────');

    // 8. Aggregation pipeline
    {
      const { ms } = await measure('Aggregate', async () =>
        col.aggregate([
          { $match: { active: true } },
          { $group: { _id: '$city', total: { $sum: '$points' }, count: { $sum: 1 } } },
          { $sort: { total: -1 } },
        ])
      );
      printResult('$match + $group + $sort', ms);
    }

    // 9. Stats
    const stats = await col.stats();
    console.log('\n── Collection Stats ──────────────────────────────────');
    console.log(`  Segment count:    ${stats.segmentCount}`);
    console.log(`  Total live:       ${stats.totalLive.toLocaleString()}`);
    console.log(`  Total dead:       ${stats.totalDead.toLocaleString()}`);
    console.log(`  Fragment ratio:   ${(stats.fragmentRatio * 100).toFixed(1)}%`);
    console.log(`  Cache hit rate:   ${(stats.cacheHitRate * 100).toFixed(1)}%`);
    console.log(`  Buffer pool used: ${stats.bufferPoolUsed} pages`);
    console.log(`  File size:        ${(stats.totalFileSize / 1024 / 1024).toFixed(2)} MB`);

    await db.close();
    console.log('\n✓ Benchmark selesai\n');

  } finally {
    await fsp.rm(tmpDir, { recursive: true, force: true });
  }
}

run().catch(console.error);
