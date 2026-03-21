import { OvnDB } from '../src/index.js';
import fsp from 'fs/promises';

async function main() {
  const dir = './data-demo';

  // 1. Open Database
  console.log('🔄 Membuka database...');
  const db = await OvnDB.open(dir);

  // 2. Akses Collection
  const users = await db.collection('users');

  // 3. Insert Data
  console.log('📝 Menambahkan data user...');
  await users.insertOne({ _id: 'u1', name: 'Alice', points: 150 });
  await users.insertOne({ _id: 'u2', name: 'Bob', points: 300 });
  await users.insertOne({ _id: 'u3', name: 'Charlie', points: 50 });

  // 4. Query Data
  console.log('🔍 Mencari user dengan poin >= 100...');
  const topUsers = await users.find(
    { points: { $gte: 100 } },
    { sort: { points: -1 }, limit: 10 }
  );

  console.log('🏆 Hasil pencarian:', topUsers);

  // 5. Update Data
  console.log('✏️ Mengupdate poin Bob...');
  await users.updateOne({ _id: 'u2' }, { $inc: { points: 50 } });

  // 6. Tutup Database
  console.log('🛑 Menutup database...');
  await db.close();

  // Bersihkan data (hanya untuk demo)
  await fsp.rm(dir, { recursive: true, force: true });
}

main().catch(console.error);
