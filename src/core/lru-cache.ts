// O(1) LRU Cache — doubly-linked list + HashMap
interface Node<K, V> {
  key:  K; val: V;
  prev: Node<K, V> | null;
  next: Node<K, V> | null;
}

export class LRUCache<K, V> {
  private readonly capacity: number;
  private readonly map:      Map<K, Node<K, V>>;
  private readonly head:     Node<K, V>;
  private readonly tail:     Node<K, V>;
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

  get(key: K): V | undefined {
    this._ops++;
    const node = this.map.get(key);
    if (!node) {
      this._misses++;
      if (this._ops >= this.WINDOW) this._resetWindow();
      return undefined;
    }
    this._detach(node);
    this._insertAfterHead(node);
    this._hits++;
    if (this._ops >= this.WINDOW) this._resetWindow();
    return node.val;
  }

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
    if (this.map.size > this.capacity) {
      const lru = this.tail.prev!;
      this._detach(lru);
      this.map.delete(lru.key);
    }
  }

  delete(key: K): boolean {
    const node = this.map.get(key);
    if (!node) return false;
    this._detach(node);
    this.map.delete(key);
    return true;
  }

  has(key: K): boolean { return this.map.has(key); }

  clear(): void {
    this.map.clear();
    this.head.next = this.tail;
    this.tail.prev = this.head;
    this._hits = this._misses = this._ops = 0;
  }

  get size(): number    { return this.map.size; }
  get hitRate(): number {
    const total = this._hits + this._misses;
    return total === 0 ? 0 : this._hits / total;
  }

  private _detach(node: Node<K, V>): void {
    node.prev!.next = node.next;
    node.next!.prev = node.prev;
  }
  private _insertAfterHead(node: Node<K, V>): void {
    node.prev = this.head;
    node.next = this.head.next;
    this.head.next!.prev = node;
    this.head.next = node;
  }
  private _resetWindow(): void {
    this._hits   = Math.round(this._hits / 2);
    this._misses = Math.round(this._misses / 2);
    this._ops    = this._hits + this._misses;
  }
}
