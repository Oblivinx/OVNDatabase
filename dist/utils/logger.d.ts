export declare function generateId(): string;
export declare function idToTimestamp(id: string): number;
export declare function isValidId(id: string): boolean;
export declare function makeLogger(module: string): {
    debug: (msg: string, meta?: Record<string, unknown>) => void;
    info: (msg: string, meta?: Record<string, unknown>) => void;
    warn: (msg: string, meta?: Record<string, unknown>) => void;
    error: (msg: string, meta?: Record<string, unknown>) => void;
};
//# sourceMappingURL=logger.d.ts.map