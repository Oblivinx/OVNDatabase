// ============================================================
//  ID Generator — timestamp-based sortable ID
//  Format: {48-bit timestamp ms}{80-bit random} = 16 hex chars
//  Sortable secara leksikografis → B+ Tree range scan efisien
// ============================================================
let lastMs = 0;
let counter = 0;
export function generateId() {
    const ms = Date.now();
    if (ms === lastMs) {
        counter++;
    }
    else {
        counter = 0;
        lastMs = ms;
    }
    const tsPart = ms.toString(16).padStart(12, '0');
    const randPart = Math.floor(Math.random() * 0xffffffff).toString(16).padStart(8, '0')
        + counter.toString(16).padStart(4, '0');
    return tsPart + randPart;
}
export function idToTimestamp(id) {
    return parseInt(id.slice(0, 12), 16);
}
export function isValidId(id) {
    return typeof id === 'string' && id.length === 24 && /^[0-9a-f]{24}$/.test(id);
}
const LEVELS = { debug: 0, info: 1, warn: 2, error: 3, silent: 99 };
function currentLevel() {
    return process.env['OVNDB_LOG'] ?? 'info';
}
export function makeLogger(module) {
    const isJson = process.env['NODE_ENV'] === 'production';
    function write(level, msg, meta) {
        if (LEVELS[level] < LEVELS[currentLevel()])
            return;
        const ts = new Date().toISOString();
        if (isJson) {
            process.stderr.write(JSON.stringify({ ts, level, module, msg, ...meta }) + '\n');
        }
        else {
            const metaStr = meta ? ' ' + JSON.stringify(meta) : '';
            const prefix = `[${ts}] [${level.toUpperCase().padEnd(5)}] [${module}]`;
            process.stderr.write(`${prefix} ${msg}${metaStr}\n`);
        }
    }
    return {
        debug: (msg, meta) => write('debug', msg, meta),
        info: (msg, meta) => write('info', msg, meta),
        warn: (msg, meta) => write('warn', msg, meta),
        error: (msg, meta) => write('error', msg, meta),
    };
}
//# sourceMappingURL=logger.js.map