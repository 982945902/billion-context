import { fork, type ChildProcess } from "node:child_process";
import { once } from "node:events";
import { fileURLToPath } from "node:url";
import type { CoreMessage, MessageContentStore } from "acp-kernel";
import type { ShadowStats, UnifiedShadow } from "./shadow.js";

export interface SessionIngest {
    record: unknown;
    rawTail?: { lastMessages: CoreMessage[]; lastMessagesFolded?: boolean };
    contentStore?: MessageContentStore;
}

export interface IngestReceipt {
    stats: Pick<ShadowStats, "ingests" | "deltaIngests" | "verifySamples" | "verifyOk" | "verifyDrift" | "lastError">;
    drifted: boolean;
    frozenSince?: number;
    committed: boolean;
}

interface IngestRequest extends SessionIngest {
    kind: "ingest";
    id: string;
    resetTrust: boolean;
}

type Request = IngestRequest | { kind: "setKv"; namespace: string; key: string; value: unknown };
type Reply = { kind: "ingest"; receipt: IngestReceipt } | { kind: "setKv" } | { kind: "error"; message: string };

export class SessionWriterProcess {
    private child?: ChildProcess;
    private pending?: { resolve: (reply: Reply) => void; reject: (error: Error) => void };

    constructor(private readonly directory: string, private readonly walLimit?: number) {}

    async save(request: Omit<IngestRequest, "kind">): Promise<IngestReceipt> {
        const reply = await this.send({ ...request, kind: "ingest" });
        if (reply.kind !== "ingest") throw new Error("Unexpected SQLite writer ingest reply");
        return reply.receipt;
    }

    async setKv(namespace: string, key: string, value: unknown): Promise<void> {
        const reply = await this.send({ kind: "setKv", namespace, key, value });
        if (reply.kind !== "setKv") throw new Error("Unexpected SQLite writer KV reply");
    }

    private send(request: Request): Promise<Reply> {
        if (this.pending) throw new Error("SQLite writer already has a pending save");
        const child = this.child ?? this.spawn();
        child.ref();
        child.channel?.ref();
        const { promise, resolve, reject } = Promise.withResolvers<Reply>();
        const pending = { resolve, reject };
        this.pending = pending;
        try {
            child.send(request, error => {
                if (error && this.child === child && this.pending === pending) this.fail(error);
            });
        } catch (error) {
            this.fail(error instanceof Error ? error : new Error(String(error)));
        }
        return promise;
    }

    async close(): Promise<void> {
        const child = this.child;
        if (!child || child.exitCode !== null || child.signalCode !== null) return;
        child.ref();
        child.channel?.ref();
        const exited = once(child, "exit");
        if (child.connected) child.disconnect();
        await exited;
    }

    private fail(error: Error): void {
        const pending = this.pending;
        this.pending = undefined;
        this.child?.unref();
        this.child?.channel?.unref();
        pending?.reject(error);
    }

    private spawn(): ChildProcess {
        const entry = fileURLToPath(new URL(import.meta.url.endsWith(".ts")
            ? "./writer-process.ts" : "./storage/writer-process.js", import.meta.url));
        const child = fork(entry, ["--bili-sqlite-writer", this.directory, String(this.walLimit ?? "")], {
            serialization: "advanced",
            stdio: ["ignore", "ignore", "inherit", "ipc"],
        });
        this.child = child;
        child.on("message", (reply: Reply) => {
            if (this.child !== child) return;
            const pending = this.pending;
            this.pending = undefined;
            child.unref();
            child.channel?.unref();
            if (reply.kind === "error") pending?.reject(new Error(reply.message));
            else pending?.resolve(reply);
        });
        child.on("error", error => { if (this.child === child) this.fail(error); });
        child.on("exit", (code, signal) => {
            if (this.child !== child) return;
            this.fail(new Error(`SQLite writer exited (${code ?? signal})`));
            this.child = undefined;
        });
        return child;
    }
}

function runWriter(directory: string, walLimit: string): void {
    const opened = Promise.all([import("./shadow.js"), import("../persist.js")]).then(([{ UnifiedShadow }, { recordToLegacyView }]) => {
        const shadow = UnifiedShadow.openWriter(directory, recordToLegacyView, walLimit ? Number(walLimit) : undefined);
        console.error(`[storage] SQLite writer ready pid=${process.pid}`);
        return shadow;
    });
    void opened.catch(error => {
        console.error("[storage] SQLite writer failed:", error);
        process.exitCode = 1;
        if (process.connected) process.disconnect?.();
    });
    process.on("disconnect", () => { void opened.then(shadow => shadow.close(), () => undefined); });
    process.on("message", (request: Request) => {
        void opened.then(shadow => {
            if (!process.connected) return;
            if (request.kind === "setKv") {
                let reply: Reply;
                try {
                    shadow.setKv(request.namespace, request.key, request.value);
                    reply = { kind: "setKv" };
                } catch (error) {
                    reply = { kind: "error", message: error instanceof Error ? error.message : String(error) };
                }
                process.send!(reply);
                return;
            }
            if (request.resetTrust) shadow.forgetIngestTrust(request.id);
            const before = { ...shadow.stats };
            shadow.record(request.record, request.rawTail, request.contentStore);
            const after = shadow.stats;
            const receipt: IngestReceipt = {
                stats: {
                    ingests: after.ingests - before.ingests,
                    deltaIngests: after.deltaIngests - before.deltaIngests,
                    verifySamples: after.verifySamples - before.verifySamples,
                    verifyOk: after.verifyOk - before.verifyOk,
                    verifyDrift: after.verifyDrift - before.verifyDrift,
                    lastError: after.lastError,
                },
                drifted: shadow.driftedSessions.has(request.id),
                frozenSince: shadow.envelopeFrozenInfo(request.id)?.sinceMs,
                committed: after.ingests > before.ingests,
            };
            process.send!({ kind: "ingest", receipt } satisfies Reply);
        }, () => undefined).catch(error => {
            console.error("[storage] SQLite writer IPC failed:", error);
            process.exitCode = 1;
            if (process.connected) process.disconnect?.();
        });
    });
}

if (process.send && process.argv[2] === "--bili-sqlite-writer") {
    runWriter(process.argv[3], process.argv[4]);
}
