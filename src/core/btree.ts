// B+ Tree — O(log n) insert/lookup/delete, O(k) range scan via linked leaves
// Min degree T=32 → height ≤ 3 for 1M records, ≤ 4 for 10M records
import type { RecordPointer } from '../types.js';

const T        = 32;
const MAX_KEYS = 2 * T - 1; // 63

interface LeafNode {
  leaf: true;
  keys: string[];
  vals: RecordPointer[];
  next: LeafNode | null;
  prev: LeafNode | null;
}
interface InternalNode {
  leaf:     false;
  keys:     string[];
  children: BTreeNode[];
}
type BTreeNode = LeafNode | InternalNode;

function makeLeaf():     LeafNode     { return { leaf: true,  keys: [], vals: [], next: null, prev: null }; }
function makeInternal(): InternalNode { return { leaf: false, keys: [], children: [] }; }

function bsearch(arr: string[], target: string): number {
  let lo = 0, hi = arr.length - 1;
  while (lo <= hi) {
    const mid = (lo + hi) >> 1;
    if      (arr[mid]! < target) lo = mid + 1;
    else if (arr[mid]! > target) hi = mid - 1;
    else return mid;
  }
  return lo;
}

export class BPlusTree {
  private root:       BTreeNode;
  private _size:      number    = 0;
  private _firstLeaf: LeafNode;
  private _lastLeaf:  LeafNode;

  constructor() {
    const leaf      = makeLeaf();
    this.root       = leaf;
    this._firstLeaf = leaf;
    this._lastLeaf  = leaf;
  }

  get size(): number { return this._size; }

  insert(key: string, val: RecordPointer): void {
    const root = this.root;
    if (root.keys.length === MAX_KEYS) {
      const newRoot = makeInternal();
      newRoot.children.push(root);
      this._splitChild(newRoot, 0);
      this.root = newRoot;
    }
    this._insertNonFull(this.root, key, val);
    this._size++;
  }

  get(key: string): RecordPointer | undefined {
    const leaf = this._findLeaf(key);
    const idx  = bsearch(leaf.keys, key);
    return idx >= 0 && leaf.keys[idx] === key ? leaf.vals[idx] : undefined;
  }

  has(key: string): boolean { return this.get(key) !== undefined; }

  set(key: string, val: RecordPointer): void {
    const leaf = this._findLeaf(key);
    const idx  = bsearch(leaf.keys, key);
    if (idx >= 0 && leaf.keys[idx] === key) leaf.vals[idx] = val;
    else this.insert(key, val);
  }

  delete(key: string): boolean {
    const found = this._delete(this.root, key, null, -1);
    if (found) this._size--;
    return found;
  }

  *range(gte?: string, lte?: string): IterableIterator<[string, RecordPointer]> {
    let leaf: LeafNode | null = gte ? this._findLeaf(gte) : this._firstLeaf;
    while (leaf) {
      for (let i = 0; i < leaf.keys.length; i++) {
        const k = leaf.keys[i]!;
        if (gte && k < gte) continue;
        if (lte && k > lte) return;
        yield [k, leaf.vals[i]!];
      }
      leaf = leaf.next;
    }
  }

  *entries(): IterableIterator<[string, RecordPointer]> { yield* this.range(); }

  toBuffer(): Buffer {
    const entries = [...this.entries()];
    let total = 4;
    for (const [k] of entries) total += 2 + Buffer.byteLength(k) + 16;
    const buf = Buffer.allocUnsafe(total);
    let pos = 0;
    buf.writeUInt32LE(entries.length, pos); pos += 4;
    for (const [k, p] of entries) {
      const keyBuf = Buffer.from(k);
      buf.writeUInt16LE(keyBuf.length, pos);    pos += 2;
      keyBuf.copy(buf, pos);                    pos += keyBuf.length;
      buf.writeDoubleBE(p.offset, pos);         pos += 8;
      buf.writeUInt32LE(p.totalSize, pos);      pos += 4;
      buf.writeUInt32LE(p.dataSize, pos);       pos += 4;
    }
    return buf;
  }

  fromBuffer(buf: Buffer): void {
    const leaf      = makeLeaf();
    this.root       = leaf;
    this._firstLeaf = leaf;
    this._lastLeaf  = leaf;
    this._size      = 0;
    let pos = 0;
    const count = buf.readUInt32LE(pos); pos += 4;
    for (let i = 0; i < count; i++) {
      const keyLen = buf.readUInt16LE(pos);                         pos += 2;
      const key    = buf.toString('utf8', pos, pos + keyLen);       pos += keyLen;
      const offset    = buf.readDoubleBE(pos);                      pos += 8;
      const totalSize = buf.readUInt32LE(pos);                      pos += 4;
      const dataSize  = buf.readUInt32LE(pos);                      pos += 4;
      this.insert(key, { offset, totalSize, dataSize });
    }
  }

  private _insertNonFull(node: BTreeNode, key: string, val: RecordPointer): void {
    if (node.leaf) {
      let lo = 0, hi = node.keys.length;
      while (lo < hi) {
        const mid = (lo + hi) >> 1;
        if (node.keys[mid]! < key) lo = mid + 1; else hi = mid;
      }
      node.keys.splice(lo, 0, key);
      node.vals.splice(lo, 0, val);
    } else {
      let i = node.keys.length - 1;
      while (i >= 0 && key < node.keys[i]!) i--;
      i++;
      if (node.children[i]!.keys.length === MAX_KEYS) {
        this._splitChild(node, i);
        if (key > node.keys[i]!) i++;
      }
      this._insertNonFull(node.children[i]!, key, val);
    }
  }

  private _splitChild(par: InternalNode, i: number): void {
    const child = par.children[i]!;
    if (child.leaf) {
      const right = makeLeaf();
      const mid   = T - 1;
      right.keys  = child.keys.splice(mid);
      right.vals  = child.vals.splice(mid);
      right.next  = child.next;
      right.prev  = child;
      if (child.next) child.next.prev = right;
      child.next  = right;
      if (this._lastLeaf === child) this._lastLeaf = right;
      par.keys    .splice(i, 0, right.keys[0]!);
      par.children.splice(i + 1, 0, right);
    } else {
      const right    = makeInternal();
      const mid      = T - 1;
      const median   = child.keys.splice(mid, 1)[0]!;
      right.keys     = child.keys.splice(mid);
      right.children = (child as InternalNode).children.splice(mid + 1);
      par.keys    .splice(i, 0, median);
      par.children.splice(i + 1, 0, right);
    }
  }

  private _findLeaf(key: string): LeafNode {
    let node = this.root;
    while (!node.leaf) {
      let i = node.keys.length - 1;
      while (i >= 0 && key < node.keys[i]!) i--;
      node = (node as InternalNode).children[i + 1]!;
    }
    return node as LeafNode;
  }

  private _delete(node: BTreeNode, key: string, par: InternalNode | null, idx: number): boolean {
    if (node.leaf) {
      const i = bsearch(node.keys, key);
      if (i < 0 || node.keys[i] !== key) return false;
      node.keys.splice(i, 1);
      node.vals.splice(i, 1);
      if (par && idx > 0 && node.keys.length > 0) par.keys[idx - 1] = node.keys[0]!;
      return true;
    } else {
      let i = node.keys.length - 1;
      while (i >= 0 && key < node.keys[i]!) i--;
      i++;
      const found = this._delete(node.children[i]!, key, node, i);
      if (node === this.root && node.keys.length === 0 && !node.leaf)
        this.root = node.children[0]!;
      return found;
    }
  }
}
