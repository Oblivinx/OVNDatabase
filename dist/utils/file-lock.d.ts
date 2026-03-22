export declare class FileLock {
    private readonly lockPath;
    private _held;
    constructor(dirPath: string);
    acquire(): Promise<void>;
    release(): Promise<void>;
    get isHeld(): boolean;
}
//# sourceMappingURL=file-lock.d.ts.map