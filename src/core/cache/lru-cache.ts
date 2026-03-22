// ============================================================
//  OvnDB v3.0 — LRU Cache (Byte-aware)
//
//  G1 FIX: Kapasitas sekarang berbasis BYTES, bukan jumlah entry.
//  Ini mencegah OOM ketika dokumen memiliki ukuran yang bervariasi
//  dari 1KB hingga 10MB+.
//
//  Cara kerja:
//   - Constructor menerima maxBytes (default 256 MB)
//   - Setiap set() menghitung ukuran value menggunakan sizeof()
//   - Evict LRU entries hingga _bytes <= maxBytes
//   - get size() tetap ada (jumlah entry) untuk backward compat
//   - get bytes() untuk monitoring penggunaan memory aktual
// ============================================================

interface Node<K, V> {
  key:   K;
  val:   V;
  bytes: number;   // G1: ukuran value dalam bytes
  prev:  Node<K, V> | null;
  next:  Node<K, V> | null;
}

/** Hitung ukuran Buffer atau object dalam bytes (perkiraan). */
function defaultSizeof(val: unknown): number {
  if (Buffer.isBuffer(val)) return val.byteLength;
  if (typeof val === 'object' && val !== null && 'data' in val) {
    // Page object dari PageManager: header + data buffer
    const p = val as { data: Buffer };
    return Buffer.isBuffer(p.data) ? p.data.byteLength + 64 : 256;
  }
  // Fallback: estimasi JSON size
  try { return Buffer.byteLength(JSON.stringify(val)); } catch { return 256; }
}

export class LRUCache<K, V> {
  // G1: kapasitas dalam bytes
  private readonly _maxBytes:  number;
  private readonly _sizeof:    (v: V) => number;
  private readonly map:        Map<K, Node<K, V>>;
  private readonly head:       Node<K, V>;
  private readonly tail:       Node<K, V>;

  // G1: track bytes aktual
  private _bytes  = 0;
  // Hit-rate tracking
  private _hits   = 0;
  private _misses = 0;
  private _ops    = 0;
  private readonly WINDOW = 10_000;

  /**
   * @param maxBytes Batas memori dalam bytes (default 256 MB).
   *                 Jika diisi angka kecil (<= 10000), dianggap jumlah entry
   *                 dan dikonversi ke estimasi bytes (backward compat).
   * @param sizeof   Fungsi untuk menghitung ukuran satu value.
   */
  constructor(maxBytes: number, sizeof?: (v: V) => number) {
    // G1: backward compat — angka kecil dianggap jumlah entry lama
    this._maxBytes = maxBytes <= 10_000 ? maxBytes * 4096 : maxBytes;
    this._sizeof   = (sizeof ?? defaultSizeof) as (v: V) => number;
    this.map  = new Map();
    this.head = { key: null as unknown as K, val: null as unknown as V, bytes: 0, prev: null, next: null };
    this.tail = { key: null as unknown as K, val: null as unknown as V, bytes: 0, prev: null, next: null };
    this.head.next = this.tail;
    this.tail.prev = this.head;
  }

  get(key: K): V | undefined {
    this._ops++;
    const node = this.map.get(key);
    if (!node) {
      this._misses++;
      this._maybeResetWindow();
      return undefined;
    }
    this._hits++;
    this._maybeResetWindow();
    this._detach(node);
    this._insertAfterHead(node);
    return node.val;
  }

  set(key: K, val: V): void {
    const nodeSize = this._sizeof(val);

    const existing = this.map.get(key);
    if (existing) {
      // G1: hapus byte lama, tambah byte baru
      this._bytes -= existing.bytes;
      existing.val   = val;
      existing.bytes = nodeSize;
      this._bytes += nodeSize;
      this._detach(existing);
      this._insertAfterHead(existing);
      this._evictIfNeeded();
      return;
    }

    const node: Node<K, V> = { key, val, bytes: nodeSize, prev: null, next: null };
    this.map.set(key, node);
    this._bytes += nodeSize;
    this._insertAfterHead(node);
    this._evictIfNeeded();
  }

  delete(key: K): boolean {
    const node = this.map.get(key);
    if (!node) return false;
    this._bytes -= node.bytes;
    this._detach(node);
    this.map.delete(key);
    return true;
  }

  has(key: K): boolean { return this.map.has(key); }

  /** Hapus semua entry dan reset statistik. */
  clear(): void {
    this.map.clear();
    this.head.next = this.tail;
    this.tail.prev = this.head;
    this._bytes  = 0;
    this._hits   = 0;
    this._misses = 0;
    this._ops    = 0;
  }

  get size():      number { return this.map.size; }
  get bytes():     number { return this._bytes; }      // G1: bytes aktual
  get maxBytes():  number { return this._maxBytes; }   // G1: limit
  get capacity_(): number { return this._maxBytes; }   // backward compat alias

  get hitRate(): number {
    const total = this._hits + this._misses;
    return total === 0 ? 1 : this._hits / total;
  }

  *entries(): IterableIterator<[K, V]> {
    let node = this.head.next;
    while (node && node !== this.tail) {
      yield [node.key, node.val];
      node = node.next;
    }
  }

  // ── Privates ──────────────────────────────────────────────

  /** G1: evict LRU entries sampai penggunaan bytes di bawah limit. */
  private _evictIfNeeded(): void {
    while (this._bytes > this._maxBytes && this.map.size > 0) {
      // Evict dari tail (LRU)
      const lru = this.tail.prev;
      if (!lru || lru === this.head) break;
      this._bytes -= lru.bytes;
      this._detach(lru);
      this.map.delete(lru.key);
    }
  }

  private _detach(node: Node<K, V>): void {
    node.prev!.next = node.next;
    node.next!.prev = node.prev;
  }

  private _insertAfterHead(node: Node<K, V>): void {
    node.prev       = this.head;
    node.next       = this.head.next;
    this.head.next!.prev = node;
    this.head.next       = node;
  }

  private _maybeResetWindow(): void {
    if (this._ops >= this.WINDOW) {
      this._hits   = Math.round(this._hits / 2);
      this._misses = Math.round(this._misses / 2);
      this._ops    = this._hits + this._misses;
    }
  }
}
