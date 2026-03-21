// ============================================================
//  OvnDB v2.0 — LRU Cache
//
//  O(1) get/set/delete menggunakan HashMap + doubly-linked list.
//  Tracking hit-rate dengan rolling window agar tidak akumulasi
//  angka lama yang menyesatkan.
//
//  Digunakan oleh:
//   - StorageEngine: cache dokumen yang sering diakses
//   - PageManager: buffer pool untuk B+ Tree pages
// ============================================================

interface Node<K, V> {
  key:  K;
  val:  V;
  prev: Node<K, V> | null;
  next: Node<K, V> | null;
}

export class LRUCache<K, V> {
  private readonly capacity: number;
  private readonly map:      Map<K, Node<K, V>>;
  // Sentinel nodes — tidak menyimpan data, hanya pembatas linked list
  private readonly head:     Node<K, V>;  // paling recently used
  private readonly tail:     Node<K, V>;  // paling least recently used

  // Hit-rate tracking dengan rolling window
  private _hits   = 0;
  private _misses = 0;
  private _ops    = 0;
  private readonly WINDOW = 10_000;

  constructor(capacity: number) {
    if (capacity < 1) throw new RangeError('LRUCache capacity must be ≥ 1');
    this.capacity = capacity;
    this.map      = new Map();
    this.head = { key: null as unknown as K, val: null as unknown as V, prev: null, next: null };
    this.tail = { key: null as unknown as K, val: null as unknown as V, prev: null, next: null };
    this.head.next = this.tail;
    this.tail.prev = this.head;
  }

  /**
   * Ambil nilai dari cache. Jika ada, node dipindahkan ke depan (MRU).
   * @returns nilai atau undefined jika tidak ada
   */
  get(key: K): V | undefined {
    this._ops++;
    const node = this.map.get(key);
    if (!node) {
      this._misses++;
      this._maybeResetWindow();
      return undefined;
    }
    // Pindahkan ke depan — ini yang bikin O(1): tidak perlu cari posisi
    this._detach(node);
    this._insertAfterHead(node);
    this._hits++;
    this._maybeResetWindow();
    return node.val;
  }

  /**
   * Set nilai. Jika key sudah ada, update nilai & pindahkan ke depan.
   * Jika cache penuh, hapus LRU (node paling belakang sebelum tail).
   */
  set(key: K, val: V): void {
    const existing = this.map.get(key);
    if (existing) {
      existing.val = val;
      this._detach(existing);
      this._insertAfterHead(existing);
      return;
    }
    const node: Node<K, V> = { key, val, prev: null, next: null };
    this.map.set(key, node);
    this._insertAfterHead(node);
    // Evict LRU jika sudah melebihi kapasitas
    if (this.map.size > this.capacity) {
      const lru = this.tail.prev!; // node sebelum tail = LRU
      this._detach(lru);
      this.map.delete(lru.key);
    }
  }

  /**
   * Hapus entry dari cache.
   * @returns true jika key ada dan berhasil dihapus
   */
  delete(key: K): boolean {
    const node = this.map.get(key);
    if (!node) return false;
    this._detach(node);
    this.map.delete(key);
    return true;
  }

  /** Hapus semua entry dan reset statistik. */
  clear(): void {
    this.map.clear();
    this.head.next = this.tail;
    this.tail.prev = this.head;
    this._hits = this._misses = this._ops = 0;
  }

  has(key: K):   boolean { return this.map.has(key); }
  get size():    number   { return this.map.size; }
  get capacity_(): number { return this.capacity; }

  /** Hit rate dalam rolling window terakhir (0.0–1.0). */
  get hitRate(): number {
    const total = this._hits + this._misses;
    return total === 0 ? 1 : this._hits / total;
  }

  /** Iterasi semua entry dari MRU ke LRU. */
  *entries(): IterableIterator<[K, V]> {
    let node = this.head.next;
    while (node && node !== this.tail) {
      yield [node.key, node.val];
      node = node.next;
    }
  }

  // ── Private helpers ──────────────────────────────────────

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

  /** Rolling window: setelah WINDOW operasi, bagi angka dengan 2.
   *  Ini membuat hit-rate mencerminkan pola akses recent, bukan all-time. */
  private _maybeResetWindow(): void {
    if (this._ops >= this.WINDOW) {
      this._hits   = Math.round(this._hits / 2);
      this._misses = Math.round(this._misses / 2);
      this._ops    = this._hits + this._misses;
    }
  }
}
