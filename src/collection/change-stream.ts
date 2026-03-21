// ============================================================
//  OvnDB v2.0 — Change Stream (Change Data Capture)
//
//  Memungkinkan aplikasi subscribe ke perubahan collection secara
//  real-time — berguna untuk:
//   - WebSocket push notification ke client
//   - Cache invalidation yang tepat sasaran
//   - Event sourcing / audit log
//   - Sync antara service
//
//  API:
//    const stream = collection.watch();
//    stream.on('change', event => console.log(event));
//    stream.on('insert', doc => io.emit('newDoc', doc));
//    stream.on('update', ({ before, after }) => cache.invalidate(after._id));
//    stream.on('delete', ({ _id }) => cache.delete(_id));
//    stream.close();
//
//  Filter:
//    collection.watch({ filter: { status: 'active' } })
//    — hanya emit event untuk dokumen yang cocok dengan filter
// ============================================================

import { EventEmitter } from 'events';
import type { OvnDocument, ChangeEvent, QueryFilter } from '../types/index.js';
import { matchFilter } from '../core/query/filter.js';

export interface WatchOptions {
  /** Filter dokumen — hanya emit event untuk dokumen yang cocok. */
  filter?: QueryFilter;
  /** Jika true, emit full document bahkan untuk update (default: true). */
  fullDocument?: boolean;
}

export class ChangeStream<T extends OvnDocument = OvnDocument> extends EventEmitter {
  private readonly filter?:  QueryFilter;
  private readonly fullDoc:  boolean;
  private _closed = false;

  constructor(opts: WatchOptions = {}) {
    super();
    this.filter  = opts.filter;
    this.fullDoc = opts.fullDocument ?? true;
    this.setMaxListeners(100); // izinkan banyak listener tanpa warning
  }

  /**
   * Internal: dipanggil oleh Collection setiap ada perubahan.
   * Tidak untuk dipanggil langsung oleh user.
   */
  _emit(event: ChangeEvent<T>): void {
    if (this._closed) return;

    // Terapkan filter jika ada
    if (this.filter && event.fullDocument) {
      if (!matchFilter(event.fullDocument as unknown as Record<string, unknown>, this.filter)) return;
    }

    // Emit berdasarkan tipe operasi
    this.emit('change', event);
    this.emit(event.operationType, event);
  }

  /** Tutup stream — tidak akan emit event lagi setelah ini. */
  close(): void {
    this._closed = true;
    this.emit('close');
    this.removeAllListeners();
  }

  get isClosed(): boolean { return this._closed; }

  // Override EventEmitter.on dengan type yang lebih spesifik
  override on(event: 'change',  listener: (e: ChangeEvent<T>) => void): this;
  override on(event: 'insert',  listener: (e: ChangeEvent<T>) => void): this;
  override on(event: 'update',  listener: (e: ChangeEvent<T>) => void): this;
  override on(event: 'delete',  listener: (e: ChangeEvent<T>) => void): this;
  override on(event: 'close',   listener: () => void): this;
  override on(event: string | symbol,    listener: (...args: any[]) => void): this {
    return super.on(event, listener);
  }
}

/** Registry semua ChangeStream yang aktif untuk satu collection. */
export class ChangeStreamRegistry<T extends OvnDocument = OvnDocument> {
  private readonly streams: Set<ChangeStream<T>> = new Set();

  /** Buat stream baru dan daftarkan ke registry. */
  create(opts?: WatchOptions): ChangeStream<T> {
    const stream = new ChangeStream<T>(opts);
    this.streams.add(stream);
    // Hapus dari registry saat stream ditutup
    stream.on('close', () => this.streams.delete(stream));
    return stream;
  }

  /** Emit event ke semua stream yang aktif. */
  emit(event: ChangeEvent<T>): void {
    for (const stream of this.streams) {
      if (!stream.isClosed) stream._emit(event);
    }
  }

  /** Tutup semua stream (saat collection di-drop atau DB ditutup). */
  closeAll(): void {
    for (const stream of this.streams) stream.close();
    this.streams.clear();
  }

  get count(): number { return this.streams.size; }
}
