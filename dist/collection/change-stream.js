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
import { matchFilter } from '../core/query/filter.js';
export class ChangeStream extends EventEmitter {
    filter;
    fullDoc;
    _closed = false;
    constructor(opts = {}) {
        super();
        this.filter = opts.filter;
        this.fullDoc = opts.fullDocument ?? true;
        this.setMaxListeners(100); // izinkan banyak listener tanpa warning
    }
    /**
     * Internal: dipanggil oleh Collection setiap ada perubahan.
     * Tidak untuk dipanggil langsung oleh user.
     */
    _emit(event) {
        if (this._closed)
            return;
        // Terapkan filter jika ada
        if (this.filter && event.fullDocument) {
            if (!matchFilter(event.fullDocument, this.filter))
                return;
        }
        // Emit berdasarkan tipe operasi
        this.emit('change', event);
        this.emit(event.operationType, event);
    }
    /** Tutup stream — tidak akan emit event lagi setelah ini. */
    close() {
        this._closed = true;
        this.emit('close');
        this.removeAllListeners();
    }
    get isClosed() { return this._closed; }
    on(event, listener) {
        return super.on(event, listener);
    }
}
/** Registry semua ChangeStream yang aktif untuk satu collection. */
export class ChangeStreamRegistry {
    streams = new Set();
    /** Buat stream baru dan daftarkan ke registry. */
    create(opts) {
        const stream = new ChangeStream(opts);
        this.streams.add(stream);
        // Hapus dari registry saat stream ditutup
        stream.on('close', () => this.streams.delete(stream));
        return stream;
    }
    /** Emit event ke semua stream yang aktif. */
    emit(event) {
        for (const stream of this.streams) {
            if (!stream.isClosed)
                stream._emit(event);
        }
    }
    /** Tutup semua stream (saat collection di-drop atau DB ditutup). */
    closeAll() {
        for (const stream of this.streams)
            stream.close();
        this.streams.clear();
    }
    get count() { return this.streams.size; }
}
//# sourceMappingURL=change-stream.js.map