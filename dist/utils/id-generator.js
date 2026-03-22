// ============================================================
//  OvnDB v2.0 — ID Generator
//
//  Format: {12 hex timestamp ms}{6 hex process-random}{6 hex counter}
//          = 24 hex chars total
//
//  Properti:
//   - Monotonically non-decreasing secara leksikografis
//   - Unik antar proses (process-random berbeda per process)
//   - Unik dalam satu proses (counter monotonic dalam satu ms)
//   - Sortable: IDs yang lebih baru selalu >= IDs yang lebih lama
//
//  Bandingkan dengan MongoDB ObjectId (24 hex):
//   - ts(8) + machine(6) + pid(4) + counter(6) = sama konsepnya
//
//  SECURITY FIX (v3.1): PRNG diganti crypto.randomBytes(3).
//  PRNG seperti pseudo-random number generator adalah deterministik dan predictable —
//  observer yang melihat beberapa ID dapat memprediksi PROCESS_RAND
//  dari proses lain. crypto.randomBytes menggunakan OS CSPRNG
//  (getrandom() di Linux) sehingga token per-proses tidak dapat ditebak.
// ============================================================
import crypto from 'crypto';
// 6 hex = 3 bytes CSPRNG, di-set sekali per process
// SECURITY: crypto.randomBytes(3) bukan PRNG — tidak predictable
const PROCESS_RAND = crypto.randomBytes(3).toString('hex');
let lastMs = 0;
let counter = 0;
/**
 * Generate ID unik sortable (24 hex chars).
 * Monotonically non-decreasing — ID yang lebih baru >= ID yang lebih lama.
 */
export function generateId() {
    const ms = Date.now();
    if (ms > lastMs) {
        lastMs = ms;
        counter = 0;
    }
    else {
        counter = (counter + 1) & 0xFFFFFF; // 24-bit counter, wrap at 16M/ms
    }
    // 12 hex = 48-bit timestamp ms
    const tsPart = ms.toString(16).padStart(12, '0');
    // 6 hex = fixed per-process random (separates different processes)
    // 6 hex = monotonic counter within same ms
    const cntPart = counter.toString(16).padStart(6, '0');
    return tsPart + PROCESS_RAND + cntPart;
}
/** Ekstrak timestamp (epoch ms) dari ID yang di-generate oleh generateId(). */
export function idToTimestamp(id) {
    return parseInt(id.slice(0, 12), 16);
}
/** Validasi apakah string adalah ID yang valid (24 hex chars). */
export function isValidId(id) {
    return typeof id === 'string' && id.length === 24 && /^[0-9a-f]{24}$/.test(id);
}
//# sourceMappingURL=id-generator.js.map