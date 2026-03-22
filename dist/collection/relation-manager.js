// ============================================================
//  OvnDB v2.0 — RelationManager
//
//  Helper untuk relasi antar-collection (analog dengan JOIN di SQL).
//
//  Dua mode:
//   - populate()     : resolve satu foreign key dari dokumen tunggal
//   - populateMany() : resolve foreign key dari banyak dokumen,
//                      dengan deduplication ID (satu read per unik ID)
//
//  Contoh:
//    rel.register('users', usersCol);
//    rel.register('groups', groupsCol);
//
//    // Single document
//    const msg  = await messages.findOne({ _id: id });
//    const full = await rel.populate(msg, { userId: 'users' });
//    // full.userId sekarang adalah objek User, bukan string ID
//
//    // Batch — jauh lebih efisien dari loop populate()
//    const msgs = await messages.find({ groupId: 'xyz' });
//    const pop  = await rel.populateMany(msgs, { userId: 'users' });
//
//  RelationMap:
//    Key   = nama field foreign key di dokumen sumber
//    Value = nama collection yang di-register di RelationManager
// ============================================================
export class RelationManager {
    _cols = new Map();
    /**
     * Daftarkan collection agar bisa di-resolve lewat nama.
     */
    register(name, col) {
        this._cols.set(name, col);
        return this;
    }
    /**
     * Resolve foreign keys dari satu dokumen.
     *
     * @param doc        Dokumen sumber
     * @param relations  Map field → nama collection
     * @returns Dokumen baru dengan field foreign key diganti objek resolved
     *
     * @example
     *   const full = await rel.populate(msg, { userId: 'users', groupId: 'groups' });
     */
    async populate(doc, relations) {
        const result = { ...doc };
        for (const [field, colName] of Object.entries(relations)) {
            const id = result[field];
            if (typeof id !== 'string')
                continue;
            const col = this._col(colName);
            const ref = await col.findOne({ _id: id });
            result[field] = ref ?? id; // fallback ke ID jika tidak ditemukan
        }
        return result;
    }
    /**
     * Resolve foreign keys dari banyak dokumen secara efisien.
     * ID yang sama hanya di-fetch satu kali (deduplication).
     *
     * @param docs       Array dokumen sumber
     * @param relations  Map field → nama collection
     * @returns Array dokumen dengan field foreign key di-populate
     *
     * @example
     *   const populated = await rel.populateMany(messages, { userId: 'users' });
     */
    async populateMany(docs, relations) {
        // Kumpulkan semua unique ID per collection terlebih dahulu
        const idSets = {};
        for (const colName of Object.values(relations)) {
            idSets[colName] = new Set();
        }
        for (const doc of docs) {
            for (const [field, colName] of Object.entries(relations)) {
                const id = doc[field];
                if (typeof id === 'string')
                    idSets[colName].add(id);
                // Support array of IDs: { tags: ['id1', 'id2'] }
                if (Array.isArray(id)) {
                    for (const item of id) {
                        if (typeof item === 'string')
                            idSets[colName].add(item);
                    }
                }
            }
        }
        // Batch-fetch semua dokumen yang diperlukan
        const resolved = {};
        for (const [colName, ids] of Object.entries(idSets)) {
            const col = this._col(colName);
            const map = new Map();
            const idArr = [...ids];
            if (idArr.length > 0) {
                const fetched = await col.find({ _id: { $in: idArr } });
                for (const d of fetched)
                    map.set(d._id, d);
            }
            resolved[colName] = map;
        }
        // Populate setiap dokumen
        return docs.map(doc => {
            const result = { ...doc };
            for (const [field, colName] of Object.entries(relations)) {
                const id = result[field];
                const map = resolved[colName];
                if (typeof id === 'string') {
                    result[field] = map.get(id) ?? id;
                }
                else if (Array.isArray(id)) {
                    result[field] = id.map(item => typeof item === 'string' ? (map.get(item) ?? item) : item);
                }
            }
            return result;
        });
    }
    /** Hapus semua registrasi collection. */
    clear() {
        this._cols.clear();
    }
    get registeredCollections() {
        return [...this._cols.keys()];
    }
    _col(name) {
        const col = this._cols.get(name);
        if (!col)
            throw new Error(`[RelationManager] Collection "${name}" belum di-register. Panggil rel.register("${name}", col) terlebih dahulu.`);
        return col;
    }
}
//# sourceMappingURL=relation-manager.js.map