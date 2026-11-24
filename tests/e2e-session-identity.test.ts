import assert from "node:assert";
import http from "node:http";
import { once } from "node:events";
import path from "node:path";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import test, { after } from "node:test";
import { recordToolUseId } from "../src/tool-ring.ts";

process.env.NODE_ENV = "test";

// Hermetic state dir per repo convention (91 sibling suites): without it this
// suite's anonymous-chain writes land in the machine-global
// prefix-affinity.json and cross-contaminate other suites running in parallel.
const xdgRoot = mkdtempSync(path.join(tmpdir(), "bc-e2e-session-identity-"));
process.env.XDG_STATE_HOME = path.join(xdgRoot, "state");
process.env.XDG_CACHE_HOME = path.join(xdgRoot, "cache");
process.env.XDG_DATA_HOME = path.join(xdgRoot, "data");
process.env.XDG_CONFIG_HOME = path.join(xdgRoot, "config");

const { defaultConfig } = await import("acp-kernel");
const { startServer } = await import("../src/server.ts");
type ProxyOptions = import("../src/config.ts").ProxyOptions;
const { SessionStore, _setStoreForTest } = await import("../src/persist.ts");
const { _setForTest: setRegistryForTest } = await import("../src/registry.ts");
const { _resetSessionsForTest } = await import("../src/session.ts");
// These modules read persistence config on import; use the test XDG roots above.
const { resolveConversation } = await import("../src/plugin.ts");
const { ccrEnabled } = await import("../src/store.ts");

after(() => {
    delete process.env.XDG_STATE_HOME;
    delete process.env.XDG_CACHE_HOME;
    delete process.env.XDG_DATA_HOME;
    delete process.env.XDG_CONFIG_HOME;
});

// #286: the session ID is the client-provided conversation value VERBATIM —
// no hash, no other dimensions. Credential rotation (ChatGPT OAuth bearer),
// upstream/relay switches, and even wire-protocol switches must not orphan
// the session and its compression state. Requests WITHOUT a client-provided
// identity are rejected with 400 (content-fingerprint sessions disabled).

function responsesSse(): string {
    const block = (type: string, data: Record<string, unknown>): string => `event: ${type}\ndata: ${JSON.stringify({ type, ...data })}\n\n`;
    const item = { type: "message", id: "msg_1", role: "assistant", status: "completed", content: [{ type: "output_text", text: "ok" }] };
    return (
        block("response.created", { response: { id: "resp_1", status: "in_progress" } }) +
        block("response.in_progress", { response: { id: "resp_1", status: "in_progress" } }) +
        block("response.output_item.added", { output_index: 0, item: { ...item, status: "in_progress", content: [] } }) +
        block("response.content_part.added", { item_id: "msg_1", output_index: 0, content_index: 0, part: { type: "output_text", text: "" } }) +
        block("response.output_text.delta", { item_id: "msg_1", output_index: 0, content_index: 0, delta: "ok" }) +
        block("response.output_text.done", { item_id: "msg_1", output_index: 0, text: "ok" }) +
        block("response.content_part.done", { item_id: "msg_1", output_index: 0, content_index: 0 }) +
        block("response.output_item.done", { output_index: 0, item }) +
        block("response.completed", { response: { id: "resp_1", status: "completed", output: [item], usage: { input_tokens: 10, output_tokens: 3 } } })
    );
}

function anthropicSse(): string {
    const block = (event: string, data: Record<string, unknown>): string => `event: ${event}\ndata: ${JSON.stringify(data)}\n\n`;
    return (
        block("message_start", { type: "message_start", message: { id: "msg_1", role: "assistant", usage: { input_tokens: 10 } } }) +
        block("content_block_start", { type: "content_block_start", index: 0, content_block: { type: "text", text: "" } }) +
        block("content_block_delta", { type: "content_block_delta", index: 0, delta: { type: "text_delta", text: "ok" } }) +
        block("content_block_stop", { type: "content_block_stop", index: 0 }) +
        block("message_delta", { type: "message_delta", delta: { stop_reason: "end_turn", stop_sequence: null }, usage: { output_tokens: 3 } }) +
        block("message_stop", { type: "message_stop" })
    );
}

function chatSse(): string {
    const chunk = (choices: unknown): string =>
        `data: ${JSON.stringify({ id: "c1", object: "chat.completion.chunk", created: 1, model: "gpt", choices })}\n\n`;
    return (
        chunk([{ index: 0, delta: { role: "assistant" }, finish_reason: null }]) +
        chunk([{ index: 0, delta: { content: "ok" }, finish_reason: null }]) +
        chunk([{ index: 0, delta: {}, finish_reason: "stop" }]) +
        "data: [DONE]\n\n"
    );
}

interface MockUpstream {
    server: http.Server;
    port: number;
    /** Headers of the most recent POST, lowercased — for passthrough assertions (#2779). */
    lastHeaders?: Record<string, string>;
    lastBody?: string;
    close(): Promise<void>;
}

async function startMockUpstream(): Promise<MockUpstream> {
    let lastHeaders: Record<string, string> | undefined;
    let lastBody: string | undefined;
    const server = http.createServer((req, res) => {
        const chunks: Buffer[] = [];
        req.on("data", (c: Buffer) => chunks.push(c));
        req.on("end", () => {
            if (req.method === "POST") {
                lastBody = Buffer.concat(chunks).toString("utf8");
                lastHeaders = {};
                for (const [k, v] of Object.entries(req.headers)) {
                    if (v !== undefined) lastHeaders[k.toLowerCase()] = Array.isArray(v) ? v.join(", ") : v;
                }
            }
            const url = req.url ?? "";
            if (req.method !== "POST") {
                res.writeHead(404, { "content-type": "application/json" });
                res.end(JSON.stringify({ error: { message: "mock upstream: unsupported" } }));
                return;
            }
            res.writeHead(200, { "content-type": "text/event-stream", "cache-control": "no-cache" });
            if (url.endsWith("/v1/messages")) res.end(anthropicSse());
            else if (url.endsWith("/responses")) res.end(responsesSse());
            else if (url.endsWith("/chat/completions")) res.end(chatSse());
            else res.end(responsesSse());
        });
    });
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const port = (server.address() as { port: number }).port;
    return {
        server,
        port,
        get lastHeaders() { return lastHeaders; },
        get lastBody() { return lastBody; },
        close: () => new Promise<void>((resolve, reject) => server.close((e) => (e ? reject(e) : resolve()))),
    };
}

interface Harness {
    proxyPort: number;
    upstreams: MockUpstream[];
    close(): Promise<void>;
}

async function startHarness(upstreamCount = 1, trial?: "tools" | "marker" | "no-inject"): Promise<Harness> {
    const upstreams: MockUpstream[] = [];
    for (let i = 0; i < upstreamCount; i++) upstreams.push(await startMockUpstream());
    _resetSessionsForTest();
    _setStoreForTest(new SessionStore({ enabled: false }));
    setRegistryForTest({});
    const routes: ProxyOptions["routes"] = {};
    for (const u of upstreams) {
        routes[`http://127.0.0.1:${u.port}`] = { models: { "gpt-test": { context: 100_000 }, "claude-test": { context: 100_000 } }, ...(trial === "marker" ? { compressProtocol: "marker" } : {}) };
    }
    const proxy = await startServer({
        port: 0,
        host: "127.0.0.1",
        upstream: "http://127.0.0.1",
        routes,
        modelContextLimit: 100_000,
        kernelConfig: defaultConfig(100_000),
        compress: { injectTool: trial !== "no-inject", injectNudge: true, ...(trial ? { ccr: { enabled: true, jevTrial: true } } : {}) },
        promptCache: { routing: "auto" },
        log: false,
        sessionHeader: "x-acp-session",
        debug: false,
        passthrough: false,
        autoUpdate: false,
        mitm: { enabled: false, domains: [] },
        compat: { roles: {} }, streamErrorShape: "protocol", passthroughSource: null, autoRestartOnUpdate: false, updateTag: "latest", advisoryCheck: false, releaseNotesCheck: false,
    });
    await once(proxy, "listening");
    const proxyPort = (proxy.address() as { port: number }).port;
    return {
        proxyPort,
        upstreams,
        close: async () => {
            proxy.close();
            proxy.closeIdleConnections?.();
            await once(proxy, "close");
            await Promise.all(upstreams.map((u) => u.close()));
        },
    };
}

interface StatsSession {
    id: string;
    protocol: string;
    upstream: string;
    requests: number;
}

async function getSessions(h: Harness): Promise<StatsSession[]> {
    const resp = await fetch(`http://127.0.0.1:${h.proxyPort}/__bili/stats`);
    assert.equal(resp.status, 200);
    const json = (await resp.json()) as { sessions: StatsSession[] };
    return json.sessions;
}

async function post(h: Harness, upstreamIdx: number, path: string, body: Record<string, unknown>, headers: Record<string, string> = {}): Promise<void> {
    const resp = await postRaw(h, upstreamIdx, path, body, headers);
    assert.equal(resp.status, 200, `proxy returned ${resp.status}: ${await resp.text()}`);
}

async function postRaw(h: Harness, upstreamIdx: number, path: string, body: Record<string, unknown>, headers: Record<string, string> = {}): Promise<Response> {
    const up = h.upstreams[upstreamIdx]!;
    return fetch(`http://127.0.0.1:${h.proxyPort}/bili/http://127.0.0.1:${up.port}${path}`, {
        method: "POST",
        headers: { "content-type": "application/json", ...headers },
        body: JSON.stringify(body),
    });
}

function responsesBody(extra: Record<string, unknown> = {}): Record<string, unknown> {
    return {
        model: "gpt-test",
        stream: true,
        instructions: "You are a test assistant.",
        input: [{ type: "message", role: "user", content: [{ type: "input_text", text: "hello" }] }],
        ...extra,
    };
}

function anthropicBody(): Record<string, unknown> {
    return { model: "claude-test", max_tokens: 1024, stream: true, system: "You are a test assistant.", messages: [{ role: "user", content: "hello" }] };
}

function chatBody(extra: Record<string, unknown> = {}): Record<string, unknown> {
    return { model: "gpt-test", stream: true, messages: [{ role: "user", content: "hello" }], ...extra };
}

test("e2e session identity: responses body.session_id survives bearer rotation; id is the value verbatim (#286, #280)", async () => {
    const h = await startHarness();
    try {
        const body = responsesBody({ session_id: "019fdc81-a420-7a00-bbd1-0a64e3eb772c" });
        await post(h, 0, "/v1/responses", body, { authorization: "Bearer bearer-old" });
        await post(h, 0, "/v1/responses", body, { authorization: "Bearer bearer-new" });
        const sessions = await getSessions(h);
        assert.equal(sessions.length, 1, `expected 1 session, got ${JSON.stringify(sessions)}`);
        assert.equal(sessions[0]!.id, "019fdc81-a420-7a00-bbd1-0a64e3eb772c");
        assert.equal(sessions[0]!.protocol, "responses");
        assert.equal(sessions[0]!.requests, 2);
    } finally {
        await h.close();
    }
});

test("e2e session identity: responses session-id header survives bearer rotation; id is the value verbatim (#286)", async () => {
    const h = await startHarness();
    try {
        const body = responsesBody();
        await post(h, 0, "/v1/responses", body, { authorization: "Bearer bearer-old", "session-id": "019fdc81-a420-7a00-bbd1-0a64e3eb772c" });
        await post(h, 0, "/v1/responses", body, { authorization: "Bearer bearer-new", "session-id": "019fdc81-a420-7a00-bbd1-0a64e3eb772c" });
        const sessions = await getSessions(h);
        assert.equal(sessions.length, 1, `expected 1 session, got ${JSON.stringify(sessions)}`);
        assert.equal(sessions[0]!.id, "019fdc81-a420-7a00-bbd1-0a64e3eb772c");
        assert.equal(sessions[0]!.protocol, "responses");
        assert.equal(sessions[0]!.requests, 2);
    } finally {
        await h.close();
    }
});

test("e2e session identity: responses body.session_id survives upstream/relay switch (#286)", async () => {
    const h = await startHarness(2);
    try {
        const body = responsesBody({ session_id: "019fdc81-a420-7a00-bbd1-0a64e3eb772c" });
        await post(h, 0, "/v1/responses", body, { authorization: "Bearer keyA" });
        await post(h, 1, "/v1/responses", body, { authorization: "Bearer keyA" });
        const sessions = await getSessions(h);
        assert.equal(sessions.length, 1, `expected 1 session, got ${JSON.stringify(sessions)}`);
        assert.equal(sessions[0]!.id, "019fdc81-a420-7a00-bbd1-0a64e3eb772c");
        assert.equal(sessions[0]!.requests, 2);
    } finally {
        await h.close();
    }
});

test("e2e session identity: anthropic x-claude-code-session-id survives x-api-key rotation (#286)", async () => {
    const h = await startHarness();
    try {
        const body = anthropicBody();
        await post(h, 0, "/v1/messages", body, { "x-api-key": "sk-ant-old", "x-claude-code-session-id": "claude-sess-1" });
        await post(h, 0, "/v1/messages", body, { "x-api-key": "sk-ant-new", "x-claude-code-session-id": "claude-sess-1" });
        const sessions = await getSessions(h);
        assert.equal(sessions.length, 1, `expected 1 session, got ${JSON.stringify(sessions)}`);
        assert.equal(sessions[0]!.id, "claude-sess-1");
        assert.equal(sessions[0]!.protocol, "anthropic");
        assert.equal(sessions[0]!.requests, 2);
    } finally {
        await h.close();
    }
});

test("e2e session identity: anthropic x-mavis-session-id survives x-api-key rotation (#1050)", async () => {
    const h = await startHarness();
    try {
        // #6884: a real mcode main turn carries the agent's tool surface; a
        // tools-less single-user body on the mcode lane is exactly the
        // one-shot auxiliary shape bili now demotes, so the fixture keeps the
        // (identity-irrelevant) tool to stay a main turn.
        const body = { ...anthropicBody(), tools: [{ type: "function", function: { name: "bash", description: "run a command", parameters: { type: "object", properties: {} } } }] };
        await post(h, 0, "/v1/messages", body, { "x-api-key": "sk-mm-old", "x-mavis-session-id": "mcode-sess-1" });
        await post(h, 0, "/v1/messages", body, { "x-api-key": "sk-mm-new", "x-mavis-session-id": "mcode-sess-1" });
        const sessions = await getSessions(h);
        assert.equal(sessions.length, 1, `expected 1 session, got ${JSON.stringify(sessions)}`);
        assert.equal(sessions[0]!.id, "mcode-sess-1");
        assert.equal(sessions[0]!.protocol, "anthropic");
        assert.equal(sessions[0]!.requests, 2);
    } finally {
        await h.close();
    }
});

test("e2e session identity: openai prompt_cache_key survives bearer rotation (#286)", async () => {
    const h = await startHarness();
    try {
        const body = chatBody({ prompt_cache_key: "pck-omp-1" });
        await post(h, 0, "/v1/chat/completions", body, { authorization: "Bearer bearer-old" });
        await post(h, 0, "/v1/chat/completions", body, { authorization: "Bearer bearer-new" });
        const sessions = await getSessions(h);
        assert.equal(sessions.length, 1, `expected 1 session, got ${JSON.stringify(sessions)}`);
        assert.equal(sessions[0]!.id, "pck-omp-1");
        assert.equal(sessions[0]!.protocol, "openai");
        assert.equal(sessions[0]!.requests, 2);
    } finally {
        await h.close();
    }
});

test("native OMP Responses isolates parent and siblings sharing a provider cache key", async () => {
    const h = await startHarness();
    const parent = "omp-parent-affinity";
    const children = ["omp-first-child", "omp-second-child"];
    const headers = { "x-bili-host-recovery-owner": "omp", "session-id": parent };
    try {
        for (const id of [parent, ...children]) {
            const registered = await fetch(`http://127.0.0.1:${h.proxyPort}/__bili/plugin/register`, {
                method: "POST", headers: { "content-type": "application/json" },
                body: JSON.stringify({ conversationId: id, agent: "omp", identity: true, ...(id !== parent ? { parentConversationId: parent } : {}) }),
            });
            assert.equal(registered.status, 200);
            await registered.text();
            if (id === parent) {
                // Before any fork exists, a legacy root using only its PCK
                // still records a model turn and can use its existing tools.
                await post(h, 0, "/v1/responses", responsesBody({ prompt_cache_key: parent }), { "x-bili-host-recovery-owner": "omp" });
            } else if (id === children[1]) {
                // An independently identified, registered legacy child may
                // establish its own ledger on its very first request.
                await post(h, 0, "/v1/responses", responsesBody({ prompt_cache_key: parent, session_id: id }), { "x-bili-host-recovery-owner": "omp" });
            }
            await post(h, 0, "/v1/responses", responsesBody({ prompt_cache_key: parent, biliJevHostSession: id }), headers);
            const sent = JSON.parse(h.upstreams[0]!.lastBody!);
            assert.equal(sent.prompt_cache_key, parent);
            assert.ok(!("biliJevHostSession" in sent));
        }
        // Both siblings share the key. An old header copied from the parent
        // or conflicting child headers cannot choose either child.
        await post(h, 0, "/v1/responses", responsesBody({ prompt_cache_key: parent }), headers);
        await post(h, 0, "/v1/responses", responsesBody({ prompt_cache_key: parent, session_id: children[1] }), { ...headers, "session-id": children[0]! });
        recordToolUseId("old-parent-call", parent);
        const tool = await fetch(`http://127.0.0.1:${h.proxyPort}/__bili/plugin/tool`, {
            method: "POST", headers: { "content-type": "application/json" },
            body: JSON.stringify({ conversationId: children[0], nativeCaller: true, toolCallId: "old-parent-call", tool: "compress", args: {} }),
        });
        assert.equal(tool.status, 409);
        assert.equal((await tool.json() as { code: string }).code, "TOOL_CONVERSATION_CONFLICT");
        const sessions = await getSessions(h);
        assert.deepEqual(Object.fromEntries(sessions.map(s => [s.id, s.requests])), {
            [parent]: 2, [children[0]!]: 1, [children[1]!]: 2,
        });
        for (const id of [parent, ...children]) {
            const status = await fetch(`http://127.0.0.1:${h.proxyPort}/__bili/plugin/status?conversationId=${encodeURIComponent(id)}`);
            assert.equal(status.status, 200);
            const state = await status.json() as { sessionId: string };
            assert.equal(state.sessionId, id);
        }
    } finally {
        await h.close();
    }
});

test("e2e session identity: same conversation value continues across a protocol switch (relay translation, #286)", async () => {
    const h = await startHarness();
    try {
        // The same client conversation id first speaks the Responses API, then
        // the OpenAI chat API (e.g. a relay that translates between them).
        // Session state is protocol-neutral, so the session must continue.
        await post(h, 0, "/v1/responses", responsesBody({ session_id: "cross-proto-1" }), { authorization: "Bearer keyA" });
        await post(h, 0, "/v1/chat/completions", chatBody(), { authorization: "Bearer keyA", "x-session-id": "cross-proto-1" });
        const sessions = await getSessions(h);
        assert.equal(sessions.length, 1, `expected 1 session, got ${JSON.stringify(sessions)}`);
        assert.equal(sessions[0]!.id, "cross-proto-1");
        assert.equal(sessions[0]!.requests, 2);
    } finally {
        await h.close();
    }
});

test("e2e session identity: anonymous responses request with history → prefix-affinity session, continuation sticks (#309)", async () => {
    const h = await startHarness();
    try {
        const body = responsesBody();
        await post(h, 0, "/v1/responses", body, { authorization: "Bearer anon-key" });
        const sessions1 = await getSessions(h);
        assert.equal(sessions1.length, 1);
        assert.match(sessions1[0]!.id, /^pfa-[0-9a-f]{16}$/);
        assert.equal(sessions1[0]!.requests, 1);
        // Replay the same history + one appended turn → same session.
        const extended = responsesBody();
        (extended.input as unknown[]).push({ type: "message", role: "user", content: [{ type: "input_text", text: "next turn" }] });
        await post(h, 0, "/v1/responses", extended, { authorization: "Bearer anon-key" });
        const sessions2 = await getSessions(h);
        assert.equal(sessions2.length, 1, `expected 1 session, got ${JSON.stringify(sessions2)}`);
        assert.equal(sessions2[0]!.id, sessions1[0]!.id);
        assert.equal(sessions2[0]!.requests, 2);
    } finally {
        await h.close();
    }
});

test("e2e session identity: anonymous anthropic request with history → prefix-affinity session (#309)", async () => {
    const h = await startHarness();
    try {
        const resp = await postRaw(h, 0, "/v1/messages", anthropicBody(), { "x-api-key": "sk-ant" });
        assert.equal(resp.status, 200, `proxy returned ${resp.status}: ${await resp.text()}`);
        const sessions = await getSessions(h);
        assert.equal(sessions.length, 1);
        assert.match(sessions[0]!.id, /^pfa-[0-9a-f]{16}$/);
        assert.equal(sessions[0]!.protocol, "anthropic");
    } finally {
        await h.close();
    }
});

test("e2e session identity: anonymous degenerate request (no history signal) → 400 (#286 remnant)", async () => {
    const h = await startHarness();
    try {
        // No messages at all: nothing to anchor prefix affinity on.
        const resp = await postRaw(h, 0, "/v1/messages", { model: "claude-test", max_tokens: 1024, stream: true, system: "system only", messages: [] }, { "x-api-key": "sk-ant" });
        assert.equal(resp.status, 400);
        const json = (await resp.json()) as { type?: string; error?: { message?: string } };
        assert.match(json.error?.message ?? "", /Missing stable conversation identity/);
        assert.equal((await getSessions(h)).length, 0);
    } finally {
        await h.close();
    }
});

test("e2e session identity: anonymous single-shot Responses server-tool call forwards verbatim, no session (#6548)", async () => {
    const h = await startHarness();
    try {
        // pi-web-access's xAI web-search backend: string input, web_search /
        // x_search tools only, no history, and no knob for a session id — the
        // client cannot carry an identity here. Pre-fix the #286 refusal
        // hard-400d it; post-fix it rides the verbatim forward like any
        // utility call and creates no session state.
        const body = { model: "gpt-test", stream: true, input: "latest news on context compression", tools: [{ type: "web_search" }, { type: "x_search" }] };
        const resp = await postRaw(h, 0, "/v1/responses", body, { authorization: "Bearer <redacted>" });
        assert.equal(resp.status, 200, `proxy returned ${resp.status}: ${await resp.text()}`);
        assert.equal((await getSessions(h)).length, 0, "verbatim forward must not create a session");
    } finally {
        await h.close();
    }
});

test("e2e session identity: anonymous Responses single-shot shape with a client tool still 400s (#6548 negative)", async () => {
    const h = await startHarness();
    try {
        // A function tool makes it an agent turn, not a server-tool utility
        // call — the #286 refusal still applies.
        const body = { model: "gpt-test", stream: true, input: "what is the weather", tools: [{ type: "function", name: "get_weather", description: "weather", parameters: { type: "object", properties: {} } }] };
        const resp = await postRaw(h, 0, "/v1/responses", body, { authorization: "Bearer <redacted>" });
        assert.equal(resp.status, 400);
        const json = (await resp.json()) as { error?: { message?: string } };
        assert.match(json.error?.message ?? "", /Missing stable conversation identity/);
        assert.equal((await getSessions(h)).length, 0);
    } finally {
        await h.close();
    }
});

test("e2e session identity: anonymous Responses search call chaining previous_response_id still 400s (#6548 negative)", async () => {
    const h = await startHarness();
    try {
        // previous_response_id chains the call onto a prior response — a
        // conversation continuation, not a single shot.
        const body = { model: "gpt-test", stream: true, input: "follow-up search", tools: [{ type: "web_search" }], previous_response_id: "resp_abc" };
        const resp = await postRaw(h, 0, "/v1/responses", body, { authorization: "Bearer <redacted>" });
        assert.equal(resp.status, 400);
        const json = (await resp.json()) as { error?: { message?: string } };
        assert.match(json.error?.message ?? "", /Missing stable conversation identity/);
        assert.equal((await getSessions(h)).length, 0);
    } finally {
        await h.close();
    }
});

test("e2e session identity: anonymous openai request → prefix-affinity session; distinct conversations split (#309)", async () => {
    const h = await startHarness();
    try {
        await post(h, 0, "/v1/chat/completions", chatBody(), { authorization: "Bearer keyX" });
        const different = { model: "gpt-test", stream: true, messages: [{ role: "user", content: "a completely different topic" }] };
        await post(h, 0, "/v1/chat/completions", different, { authorization: "Bearer keyX" });
        const sessions = await getSessions(h);
        assert.equal(sessions.length, 2, `expected 2 sessions, got ${JSON.stringify(sessions)}`);
        assert.ok(sessions.every((s) => /^pfa-[0-9a-f]{16}$/.test(s.id)));
    } finally {
        await h.close();
    }
});

test("e2e session identity: distinct session_ids stay separate (#286)", async () => {
    const h = await startHarness();
    try {
        await post(h, 0, "/v1/responses", responsesBody({ session_id: "sess-A" }), { authorization: "Bearer keyA" });
        await post(h, 0, "/v1/responses", responsesBody({ session_id: "sess-B" }), { authorization: "Bearer keyA" });
        const sessions = await getSessions(h);
        assert.equal(sessions.length, 2, `expected 2 sessions, got ${JSON.stringify(sessions)}`);
        const ids = sessions.map((s) => s.id).sort();
        assert.deepEqual(ids, ["sess-A", "sess-B"]);
    } finally {
        await h.close();
    }
});

// #2779: OpenCode 2.x child requests carry their own session id ONLY in
// x-opencode-session-id; x-session-affinity / X-Session-Id / x-opencode-session
// all carry the parent's cache-routing affinity (parentID ?? fork ?? id). The
// native header must win or siblings share the parent's mutable ACP state.
// The parent's own requests have affinity === own id (root session shape).
function opencodeChildHeaders(ownId: string, parentId: string): Record<string, string> {
    return {
        authorization: "Bearer oc-key",
        "user-agent": "opencode/2.0",
        "x-opencode-session-id": ownId,
        "x-session-affinity": parentId,
        "x-session-id": parentId,
        "x-opencode-session": parentId,
    };
}

async function expectOpencodeIsolation(h: Harness): Promise<void> {
    const sessions = await getSessions(h);
    assert.equal(sessions.length, 3, `expected 3 independent sessions, got ${JSON.stringify(sessions)}`);
    const byId = new Map(sessions.map((s) => [s.id, s]));
    for (const id of ["ses_parent_1", "ses_child_a", "ses_child_b"]) {
        const s = byId.get(id);
        assert.ok(s, `missing session ${id} in ${JSON.stringify(sessions)}`);
        assert.equal(s!.requests, 2, `session ${id} should see both interleaved turns, saw ${s!.requests}`);
    }
}

test("e2e session identity: opencode 2.x children isolate from parent under inherited affinity — chat completions (#2779)", async () => {
    const h = await startHarness();
    try {
        await post(h, 0, "/v1/chat/completions", chatBody(), opencodeChildHeaders("ses_parent_1", "ses_parent_1"));
        await post(h, 0, "/v1/chat/completions", chatBody(), opencodeChildHeaders("ses_child_a", "ses_parent_1"));
        await post(h, 0, "/v1/chat/completions", chatBody(), opencodeChildHeaders("ses_parent_1", "ses_parent_1"));
        await post(h, 0, "/v1/chat/completions", chatBody(), opencodeChildHeaders("ses_child_b", "ses_parent_1"));
        await post(h, 0, "/v1/chat/completions", chatBody(), opencodeChildHeaders("ses_child_a", "ses_parent_1"));
        await post(h, 0, "/v1/chat/completions", chatBody(), opencodeChildHeaders("ses_child_b", "ses_parent_1"));
        await expectOpencodeIsolation(h);
        // Upstream cache-affinity inputs are forwarded byte-unchanged.
        const up = h.upstreams[0]!;
        assert.equal(up.lastHeaders?.["x-session-affinity"], "ses_parent_1");
        assert.equal(up.lastHeaders?.["x-session-id"], "ses_parent_1");
        assert.equal(up.lastHeaders?.["x-opencode-session"], "ses_parent_1");
        assert.equal(up.lastHeaders?.["x-opencode-session-id"], "ses_child_b");
    } finally {
        await h.close();
    }
});

test("e2e session identity: opencode 2.x children isolate from parent under inherited affinity — responses (#2779)", async () => {
    const h = await startHarness();
    try {
        await post(h, 0, "/v1/responses", responsesBody(), opencodeChildHeaders("ses_parent_1", "ses_parent_1"));
        await post(h, 0, "/v1/responses", responsesBody(), opencodeChildHeaders("ses_child_a", "ses_parent_1"));
        await post(h, 0, "/v1/responses", responsesBody(), opencodeChildHeaders("ses_parent_1", "ses_parent_1"));
        await post(h, 0, "/v1/responses", responsesBody(), opencodeChildHeaders("ses_child_b", "ses_parent_1"));
        await post(h, 0, "/v1/responses", responsesBody(), opencodeChildHeaders("ses_child_a", "ses_parent_1"));
        await post(h, 0, "/v1/responses", responsesBody(), opencodeChildHeaders("ses_child_b", "ses_parent_1"));
        await expectOpencodeIsolation(h);
    } finally {
        await h.close();
    }
});

test("e2e session identity: opencode 2.x children isolate from parent under inherited affinity — anthropic (#2779)", async () => {
    const h = await startHarness();
    try {
        await post(h, 0, "/v1/messages", anthropicBody(), opencodeChildHeaders("ses_parent_1", "ses_parent_1"));
        await post(h, 0, "/v1/messages", anthropicBody(), opencodeChildHeaders("ses_child_a", "ses_parent_1"));
        await post(h, 0, "/v1/messages", anthropicBody(), opencodeChildHeaders("ses_parent_1", "ses_parent_1"));
        await post(h, 0, "/v1/messages", anthropicBody(), opencodeChildHeaders("ses_child_b", "ses_parent_1"));
        await post(h, 0, "/v1/messages", anthropicBody(), opencodeChildHeaders("ses_child_a", "ses_parent_1"));
        await post(h, 0, "/v1/messages", anthropicBody(), opencodeChildHeaders("ses_child_b", "ses_parent_1"));
        await expectOpencodeIsolation(h);
    } finally {
        await h.close();
    }
});

test("e2e session identity: blank x-opencode-session-id falls back to inherited affinity (#2779)", async () => {
    const h = await startHarness();
    try {
        await post(h, 0, "/v1/chat/completions", chatBody(), { authorization: "Bearer oc-key", "x-opencode-session-id": "   ", "x-session-affinity": "ses_parent_1" });
        await post(h, 0, "/v1/chat/completions", chatBody(), { authorization: "Bearer oc-key", "x-opencode-session-id": "  ", "x-session-affinity": "ses_parent_1" });
        const sessions = await getSessions(h);
        assert.equal(sessions.length, 1, `expected 1 session on the affinity fallback, got ${JSON.stringify(sessions)}`);
        assert.equal(sessions[0]!.id, "ses_parent_1");
        assert.equal(sessions[0]!.requests, 2);
    } finally {
        await h.close();
    }
});

test("e2e session identity: plugin-stamped identity still outranks the native opencode header (#2779)", async () => {
    const h = await startHarness();
    try {
        const headers = { ...opencodeChildHeaders("ses_child_a", "ses_parent_1"), "x-bili-plugin": "opencode", "x-bili-plugin-conversation": "plugin-conv-1" };
        await post(h, 0, "/v1/chat/completions", chatBody(), headers);
        await post(h, 0, "/v1/chat/completions", chatBody(), headers);
        const sessions = await getSessions(h);
        assert.equal(sessions.length, 1, `expected 1 plugin-keyed session, got ${JSON.stringify(sessions)}`);
        assert.equal(sessions[0]!.id, "plugin-conv-1");
        assert.equal(sessions[0]!.requests, 2);
    } finally {
        await h.close();
    }
});

const ompResponsesHeaders = { "x-bili-host-recovery-owner": "omp" };
const ompRetrieveTools = ["bash", "acp_retrieve"].map(name => ({ type: "function", name, parameters: { type: "object" } }));

test("OMP Responses verifies each owner before touching a ledger; siblings never claim the provider PCK", async () => {
    const h = await startHarness(1, "tools");
    try {
        const pck = "omp-owner-provider-key";
        const first = "omp-owner-first";
        const send = async (owner: string, headers: Record<string, string>, extra: Record<string, unknown> = {}, path = "/v1/responses") => {
            await post(h, 0, path, responsesBody({ prompt_cache_key: pck, biliJevHostSession: owner, tools: ompRetrieveTools, ...extra }), headers);
            const sent = JSON.parse(h.upstreams[0]!.lastBody!);
            assert.equal(sent.prompt_cache_key, pck);
            assert.ok(!("biliJevHostSession" in sent));
            assert.ok(!("biliJevCcr" in sent));
            assert.ok(!("x-bili-host-recovery-owner" in h.upstreams[0]!.lastHeaders!));
            return sent;
        };
        await send(first, {});
        await send(first, { "x-bili-plugin": "omp" }, { biliJevCcr: false });
        await send(first, { ...ompResponsesHeaders, "x-bili-plugin": "codex" });
        await send(first, { ...ompResponsesHeaders, "user-agent": "codex_cli_rs/1.0" });
        await send(first, { ...ompResponsesHeaders, "x-bili-plugin-conversation": "different" });
        assert.deepEqual(await getSessions(h), [], "an unverified stamp cannot open even the provider-key ledger");
        await send(first, ompResponsesHeaders);
        assert.ok(ccrEnabled(resolveConversation(first).session));
        await send(first, ompResponsesHeaders, { biliJevCcr: false });
        assert.equal(resolveConversation(first).session?.id, first);
        assert.ok(!ccrEnabled(resolveConversation(first).session), "an ordinary OMP turn disarms pruning on a trial listener");
        await send(first, ompResponsesHeaders);
        assert.ok(ccrEnabled(resolveConversation(first).session), "an opted-in turn with fresh proof can rearm pruning");
        const reg = await fetch(`http://127.0.0.1:${h.proxyPort}/__bili/plugin/register`, {
            method: "POST", body: JSON.stringify({ conversationId: "omp-owner-second", agent: "omp", identity: true }),
            headers: { "content-type": "application/json" },
        });
        assert.equal(reg.status, 200, await reg.text());
        await send("omp-owner-second", {}); // launcher lane: exact registration instead of the native transport header
        assert.equal(resolveConversation(first).session?.id, first);
        assert.equal(resolveConversation("omp-owner-second").session?.id, "omp-owner-second");
        assert.equal(resolveConversation(pck).session, undefined);
        const continuation = await send(first, ompResponsesHeaders, { previous_response_id: "resp-provider", input: [{ role: "user", content: "delta" }] });
        assert.equal(continuation.previous_response_id, "resp-provider");
        assert.deepEqual(continuation.input, [{ role: "user", content: "delta" }]);
        assert.ok(!ccrEnabled(resolveConversation(first).session));
        await send(first, ompResponsesHeaders, {}, "/v1/responses/compact");
        assert.ok(!ccrEnabled(resolveConversation(first).session));
        for (const extra of [
            { tools: [{ type: "tool_search" }, { ...ompRetrieveTools[1], defer_loading: true }] },
            { tool_choice: "none" },
            { context_management: [{ type: "compaction", compact_threshold: 1000 }] },
            { conversation: "conv_retained" },
            { input: [{ type: "item_reference", id: "msg_retained" }, { role: "user", content: "new" }] },
            { input: [{ role: "user", content: "new" }, { type: "function_call_output", call_id: "missing", output: "unpaired" }] },
        ]) {
            await send(first, ompResponsesHeaders, extra);
            assert.ok(!ccrEnabled(resolveConversation(first).session), JSON.stringify(extra));
        }
    } finally {
        await h.close();
    }
});

for (const mode of ["marker", "no-inject"] as const) test(`OMP Responses ${mode} never arms a retrieve channel`, async () => {
    const h = await startHarness(1, mode);
    try {
        const owner = `omp-owner-${mode}`;
        await post(h, 0, "/v1/responses", responsesBody({ biliJevHostSession: owner, tools: ompRetrieveTools }), ompResponsesHeaders);
        const session = resolveConversation(owner).session;
        assert.equal(session?.id, owner);
        assert.ok(!ccrEnabled(session));
        const sent = JSON.parse(h.upstreams[0]!.lastBody!);
        assert.ok(!("prompt_cache_key" in sent) && !("biliJevHostSession" in sent));
    } finally {
        await h.close();
    }
});
