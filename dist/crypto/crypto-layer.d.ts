export declare const IV_SIZE = 12;
export declare const TAG_SIZE = 16;
export declare const KEY_VERSION_SIZE = 1;
export declare const CRYPTO_OVERHEAD: number;
export declare const CRYPTO_OVERHEAD_V2: number;
export declare class CryptoLayer {
    private readonly key;
    private readonly keyVersion;
    private constructor();
    static fromKey(key: Buffer, keyVersion?: number): CryptoLayer;
    static fromPassphrase(passphrase: string, dataDir: string, keyVersion?: number): Promise<CryptoLayer>;
    /**
     * G13: Format baru: [1 keyVersion][12 IV][16 Tag][N CT]
     * Total overhead: 29 bytes (vs 28 di v2.x)
     */
    encrypt(plaintext: Buffer): Buffer;
    /**
     * G13: Detect format otomatis (v2 28 bytes overhead vs v3 29 bytes).
     *
     * SECURITY FIX: Heuristic lama `ciphertext[0]! <= MAX_KEY_VERSION` selalu
     * bernilai true karena MAX_KEY_VERSION = 255 dan semua byte adalah <= 255.
     * Akibatnya semua ciphertext v2 (overhead 28 byte) salah-diparse sebagai v3
     * (overhead 29 byte), menghasilkan IV/tag dari offset yang salah → decrypt
     * selalu gagal untuk data yang ditulis versi lama.
     *
     * Fix: format v3 hanya dipakai jika panjang ciphertext > CRYPTO_OVERHEAD_V2
     * (artinya ada setidaknya 1 byte ekstra di depan untuk version byte).
     * Ini adalah kondisi yang perlu DAN cukup untuk membedakan v2 dari v3,
     * karena ciphertext v2 dengan panjang tepat CRYPTO_OVERHEAD_V2 + N byte
     * plaintext tidak akan memiliki byte "ekstra" di posisi 0.
     *
     * Catatan: Untuk plaintext kosong (N=0), v2 = 28 bytes, v3 = 29 bytes.
     * Ambiguitas hanya terjadi pada plaintext 0 byte (sangat jarang untuk JSON doc).
     * Dalam kasus tersebut kita gunakan tryV3 → fallback tryV2.
     */
    decrypt(ciphertext: Buffer): Buffer;
    /** G13: ekstrak key version dari ciphertext tanpa dekripsi penuh */
    static getKeyVersion(ciphertext: Buffer): number | null;
    get version(): number;
    verify(ciphertext: Buffer): boolean;
    reencrypt(ciphertext: Buffer, newCrypto: CryptoLayer): Buffer;
    rotateKey(ciphertexts: Buffer[], newCrypto: CryptoLayer): Buffer[];
    static isEncryptedBuffer(buf: Buffer): boolean;
}
declare const ENCRYPTED_FIELDS_KEY = "_ef";
export declare class FieldCrypto {
    private readonly layer;
    constructor(layer: CryptoLayer);
    /**
     * G12: Enkripsi field tertentu dalam dokumen.
     * Field yang terenkripsi disimpan sebagai base64 string.
     * Daftar field yang terenkripsi disimpan di _ef (hidden field).
     */
    encryptFields<T extends Record<string, unknown>>(doc: T, fields: (keyof T)[]): T & {
        [ENCRYPTED_FIELDS_KEY]: string[];
    };
    /**
     * G12: Dekripsi field yang terenkripsi dalam dokumen.
     * Baca daftar field dari _ef lalu dekripsi satu per satu.
     */
    decryptFields<T extends Record<string, unknown>>(doc: T): T;
    /**
     * G12: Cek apakah field tertentu terenkripsi dalam dokumen.
     */
    isEncryptedField(doc: Record<string, unknown>, field: string): boolean;
}
export declare function cryptoFromPassphrase(passphrase: string, dataDir: string, keyVersion?: number): Promise<CryptoLayer>;
export {};
//# sourceMappingURL=crypto-layer.d.ts.map