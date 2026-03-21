// ============================================================
//  CRC32 — checksum untuk integritas data
//  Implementasi Castagnoli polynomial (sama dengan storage v1)
// ============================================================

const TABLE = (() => {
  const t = new Uint32Array(256);
  for (let i = 0; i < 256; i++) {
    let c = i;
    for (let j = 0; j < 8; j++) c = c & 1 ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
    t[i] = c;
  }
  return t;
})();

export function crc32(buf: Buffer | Uint8Array): number {
  let crc = 0xffffffff;
  for (const byte of buf) crc = TABLE[(crc ^ byte) & 0xff]! ^ (crc >>> 8);
  return (crc ^ 0xffffffff) >>> 0;
}

export function writeCrc(buf: Buffer, offset: number, value: number): void {
  buf.writeUInt32LE(value, offset);
}

export function readCrc(buf: Buffer, offset: number): number {
  return buf.readUInt32LE(offset);
}
