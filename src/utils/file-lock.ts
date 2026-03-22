// ============================================================
//  FileLock — single-writer process guard
//  Mencegah dua proses membuka database yang sama secara bersamaan.
//  Menggunakan .lock file dengan PID content.
//
//  v3.1 FIX: acquire() pakai O_CREAT|O_EXCL|O_RDWR — atomic OS-level lock.
//  Flag lama ('wx') rentan race condition saat dua proses start bersamaan.
// ============================================================

import fs   from 'fs';
import fsp  from 'fs/promises';
import path from 'path';
import { makeLogger } from './logger.js';

const log = makeLogger('file-lock');

export class FileLock {
  private readonly lockPath: string;
  private _held = false;

  constructor(dirPath: string) {
    this.lockPath = path.join(dirPath, '.ovndb.lock');
  }

  async acquire(): Promise<void> {
    // FIX: gunakan O_CREAT | O_EXCL untuk atomic OS-level lock creation.
    // Dua proses yang race condition tidak bisa keduanya sukses — OS garantikan
    // hanya satu yang berhasil membuat file dengan flag O_EXCL.
    // Flag 'wx' sebelumnya berpotensi race: dua proses bisa keduanya baca
    // lockfile yang tidak ada sebelum salah satu berhasil write.
    const O_CREAT = fs.constants.O_CREAT;
    const O_EXCL  = fs.constants.O_EXCL;
    const O_RDWR  = fs.constants.O_RDWR;

    try {
      // Atomic create — OS garantikan hanya satu proses yang berhasil
      const fd = fs.openSync(this.lockPath, O_CREAT | O_EXCL | O_RDWR, 0o644);
      fs.writeSync(fd, String(process.pid));
      fs.fdatasyncSync(fd);
      fs.closeSync(fd);
      this._held = true;
      log.debug('Lock acquired', { pid: process.pid, path: this.lockPath });
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code !== 'EEXIST') throw err;

      // Lock file sudah ada — cek apakah pemiliknya masih hidup
      try {
        const raw = await fsp.readFile(this.lockPath, 'utf8');
        const pid = parseInt(raw.trim(), 10);
        // Coba kirim signal 0 — tidak ada efek tapi error jika PID tidak ada
        try { process.kill(pid, 0); } catch {
          // Proses sudah mati — hapus lock stale dan coba lagi (sekali)
          log.warn(`Removing stale lock from PID ${pid}`, { path: this.lockPath });
          await fsp.unlink(this.lockPath);
          return this.acquire();
        }
        throw new Error(
          `[OvnDB] Database is already open by PID ${pid}. ` +
          `Use OvnDBOptions.fileLock = false to disable this check.`
        );
      } catch (e) {
        if ((e as NodeJS.ErrnoException).code === 'ENOENT') return this.acquire();
        throw e;
      }
    }
  }

  async release(): Promise<void> {
    if (!this._held) return;
    try {
      await fsp.unlink(this.lockPath);
      this._held = false;
      log.debug('Lock released', { path: this.lockPath });
    } catch { /* abaikan jika file sudah tidak ada */ }
  }

  get isHeld(): boolean { return this._held; }
}
