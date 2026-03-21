// ============================================================
//  FileLock — single-writer process guard
//  Mencegah dua proses membuka database yang sama secara bersamaan.
//  Menggunakan .lock file dengan PID content.
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
    try {
      // Coba buat file .lock — gagal jika sudah ada (O_EXCL)
      const fd = fs.openSync(this.lockPath, 'wx');
      fs.writeSync(fd, String(process.pid));
      fs.closeSync(fd);
      this._held = true;
      log.debug('Lock acquired', { pid: process.pid, path: this.lockPath });
    } catch {
      // Cek apakah proses yang memegang lock masih hidup
      try {
        const content = await fsp.readFile(this.lockPath, 'utf8');
        const pid     = parseInt(content.trim(), 10);
        // Coba kirim signal 0 — tidak ada efek tapi akan error jika PID tidak ada
        try { process.kill(pid, 0); } catch {
          // Proses sudah mati — hapus lock stale dan coba lagi
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
