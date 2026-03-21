// ============================================================
//  FileLock — Single-writer / single-process guard
//
//  Mencegah dua proses membuka database yang sama secara
//  bersamaan. Menggunakan .lock file dengan PID + timestamp
//  dan periodic heartbeat untuk mendeteksi stale lock.
//
//  Strategy:
//    1. Cek apakah .lock file ada
//    2. Jika ada, baca PID — cek apakah proses itu masih hidup
//    3. Jika proses mati (stale lock), ambil alih
//    4. Jika proses masih hidup → throw error
//    5. Write lock file dengan PID kita + timestamp
//    6. Refresh heartbeat tiap HEARTBEAT_INTERVAL ms
//    7. Release → hapus lock file
// ============================================================
import fs   from 'fs';
import fsp  from 'fs/promises';
import path from 'path';
import { makeLogger } from '../utils/logger.js';

const log = makeLogger('file-lock');

const HEARTBEAT_INTERVAL = 5_000;  // ms — refresh lock setiap 5 detik
const STALE_THRESHOLD    = 15_000; // ms — lock dianggap stale jika > 15 detik tidak diperbarui

interface LockData {
  pid:       number;
  hostname:  string;
  timestamp: number;  // ms sejak epoch
  dbPath:    string;
}

export class FileLock {
  private readonly lockPath: string;
  private heartbeat: ReturnType<typeof setInterval> | null = null;
  private _held = false;

  constructor(dirPath: string) {
    this.lockPath = path.join(dirPath, '.ovndb.lock');
  }

  /**
   * Acquire the lock. Throws jika database sedang dibuka
   * oleh proses lain yang masih aktif.
   */
  async acquire(): Promise<void> {
    const existing = await this._readLock();

    if (existing) {
      const isStale = await this._isStale(existing);
      if (!isStale) {
        throw new Error(
          `[OvnDB] Database sedang digunakan oleh proses lain:\n` +
          `  PID: ${existing.pid}  Host: ${existing.hostname}\n` +
          `  Path: ${existing.dbPath}\n` +
          `  Sejak: ${new Date(existing.timestamp).toISOString()}\n` +
          `Pastikan hanya satu proses yang membuka database ini sekaligus.`
        );
      }
      // Stale lock — ambil alih
      log.warn(`Stale lock ditemukan dari PID ${existing.pid}, mengambil alih...`);
      await this._writeLock();
    } else {
      await this._writeLock();
    }

    this._held = true;

    // Heartbeat — perbarui timestamp secara berkala agar lock tidak dianggap stale
    this.heartbeat = setInterval(async () => {
      if (this._held) {
        await this._writeLock().catch(err =>
          log.error('Gagal memperbarui heartbeat lock', { err: String(err) })
        );
      }
    }, HEARTBEAT_INTERVAL);
    (this.heartbeat as unknown as { unref?: () => void }).unref?.();

    log.debug(`Lock acquired`, { pid: process.pid, path: this.lockPath });
  }

  /**
   * Release the lock. Aman dipanggil berkali-kali.
   */
  async release(): Promise<void> {
    if (!this._held) return;
    this._held = false;

    if (this.heartbeat) {
      clearInterval(this.heartbeat);
      this.heartbeat = null;
    }

    try {
      // Pastikan lock masih milik kita sebelum menghapus
      const current = await this._readLock();
      if (current?.pid === process.pid) {
        await fsp.unlink(this.lockPath);
        log.debug(`Lock released`, { pid: process.pid });
      }
    } catch {
      // Tidak apa-apa jika file sudah tidak ada
    }
  }

  get isHeld(): boolean { return this._held; }

  // ── Privates ──────────────────────────────────────────────

  private async _writeLock(): Promise<void> {
    const data: LockData = {
      pid:       process.pid,
      hostname:  process.env['HOSTNAME'] ?? 'unknown',
      timestamp: Date.now(),
      dbPath:    path.dirname(this.lockPath),
    };
    // Atomic write via temp file + rename untuk mencegah partial-write
    const tmp = this.lockPath + '.tmp';
    await fsp.writeFile(tmp, JSON.stringify(data, null, 2), 'utf8');
    await fsp.rename(tmp, this.lockPath);
  }

  private async _readLock(): Promise<LockData | null> {
    try {
      const content = await fsp.readFile(this.lockPath, 'utf8');
      return JSON.parse(content) as LockData;
    } catch {
      return null;
    }
  }

  private async _isStale(lock: LockData): Promise<boolean> {
    // 1. Cek timestamp — jika terlalu lama tidak diperbarui, anggap stale
    const age = Date.now() - lock.timestamp;
    if (age > STALE_THRESHOLD) return true;

    // 2. Cek apakah proses masih berjalan (hanya bisa di localhost)
    if (lock.pid === process.pid) return false; // diri sendiri
    try {
      // process.kill(pid, 0) — tidak benar-benar mengirim signal,
      // hanya mengecek apakah proses ada. Throw jika tidak ada.
      process.kill(lock.pid, 0);
      return false; // proses masih hidup
    } catch {
      return true; // proses tidak ada → stale
    }
  }
}
