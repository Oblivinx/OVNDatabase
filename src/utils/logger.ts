// ============================================================
//  ID Generator — timestamp-based sortable ID
//  Format: {48-bit timestamp ms}{80-bit random} = 16 hex chars
//  Sortable secara leksikografis → B+ Tree range scan efisien
// ============================================================

let lastMs  = 0;
let counter = 0;

export function generateId(): string {
  const ms = Date.now();
  if (ms === lastMs) { counter++; } else { counter = 0; lastMs = ms; }
  const tsPart  = ms.toString(16).padStart(12, '0');
  const randPart = Math.floor(Math.random() * 0xffffffff).toString(16).padStart(8, '0')
    + counter.toString(16).padStart(4, '0');
  return tsPart + randPart;
}

export function idToTimestamp(id: string): number {
  return parseInt(id.slice(0, 12), 16);
}

export function isValidId(id: string): boolean {
  return typeof id === 'string' && id.length === 24 && /^[0-9a-f]{24}$/.test(id);
}

// ============================================================
//  Logger — structured logging dengan level dan format
// ============================================================

type Level = 'debug' | 'info' | 'warn' | 'error' | 'silent';

const LEVELS: Record<Level, number> = { debug: 0, info: 1, warn: 2, error: 3, silent: 99 };

function currentLevel(): Level {
  return (process.env['OVNDB_LOG'] as Level | undefined) ?? 'info';
}

export function makeLogger(module: string) {
  const isJson = process.env['NODE_ENV'] === 'production';

  function write(level: Level, msg: string, meta?: Record<string, unknown>): void {
    if (LEVELS[level] < LEVELS[currentLevel()]) return;
    const ts = new Date().toISOString();
    if (isJson) {
      process.stderr.write(JSON.stringify({ ts, level, module, msg, ...meta }) + '\n');
    } else {
      const metaStr = meta ? ' ' + JSON.stringify(meta) : '';
      const prefix  = `[${ts}] [${level.toUpperCase().padEnd(5)}] [${module}]`;
      process.stderr.write(`${prefix} ${msg}${metaStr}\n`);
    }
  }

  return {
    debug: (msg: string, meta?: Record<string, unknown>) => write('debug', msg, meta),
    info:  (msg: string, meta?: Record<string, unknown>) => write('info',  msg, meta),
    warn:  (msg: string, meta?: Record<string, unknown>) => write('warn',  msg, meta),
    error: (msg: string, meta?: Record<string, unknown>) => write('error', msg, meta),
  };
}
