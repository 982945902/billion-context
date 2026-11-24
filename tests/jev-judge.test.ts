import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { judgeJevCcr } from "../src/jev-judge.ts";

const candidates = Array.from({ length: 13 }, (_, index) => ({
    id: `id-${index}`, tool: "bash", input: "build", text: "x".repeat(5000),
}));

test("only explicit confident store answers authorize matching results in the bounded batch", async (t) => {
    const oldKey = process.env.BILI_JEV_API_KEY;
    const oldPath = process.env.BILI_JEV_CONFIG;
    const dir = await mkdtemp(join(tmpdir(), "jev-test-"));
    const path = join(dir, "judge.json");
    const config = { baseUrl: "https://judge.example.test/v1/systemone", apiKey: "test-file-key", model: "trial-model" };
    await writeFile(path, JSON.stringify(config));
    delete process.env.BILI_JEV_API_KEY;
    process.env.BILI_JEV_CONFIG = path;
    t.after(async () => {
        await rm(dir, { recursive: true });
        if (oldPath === undefined) delete process.env.BILI_JEV_CONFIG;
        else process.env.BILI_JEV_CONFIG = oldPath;
        if (oldKey === undefined) delete process.env.BILI_JEV_API_KEY;
        else process.env.BILI_JEV_API_KEY = oldKey;
    });
    const request = t.mock.method(globalThis, "fetch", async (url: URL, init: RequestInit) => {
        assert.equal(url.href, config.baseUrl);
        assert.equal(new Headers(init.headers).get("authorization"), `Bearer ${config.apiKey}`);
        const { state, questions, model } = JSON.parse(String(init.body));
        assert.equal(model, config.model);
        assert.equal(Object.keys(questions).length, 12);
        assert.equal(state.outputs.length, 12);
        assert.ok(state.outputs.every((r: { sample: string }) => r.sample.length < 2500));
        return Response.json({ answers: {
            result_0: { type: "choice", choice: "store", confidence: 0.9 },
            result_1: { type: "choice", choice: "store", confidence: 0.4 },
            result_2: { type: "choice", choice: "keep", confidence: 1 },
            result_3: { type: "choice", choice: "store", confidence: 9 },
            result_12: { type: "choice", choice: "store", confidence: 1 },
        } });
    });
    assert.deepEqual([...await judgeJevCcr("debug the build", candidates)], ["id-0"]);
    assert.equal(request.mock.callCount(), 1);
    process.env.BILI_JEV_API_KEY = "override";
    request.mock.mockImplementation(async (_url: URL, init: RequestInit) => {
        assert.equal(new Headers(init.headers).get("authorization"), "Bearer override");
        return Response.json({ answers: { result_1: { type: "choice", choice: "store", confidence: 1 } } });
    });
    assert.deepEqual([...await judgeJevCcr("debug the build", candidates)], ["id-1"]);
    request.mock.mockImplementation(async () => { throw new DOMException("expired", "TimeoutError"); });
    assert.deepEqual([...await judgeJevCcr("debug the build", candidates)], []);
    await writeFile(path, "{");
    assert.deepEqual([...await judgeJevCcr("debug the build", candidates)], []);
    assert.equal(request.mock.callCount(), 3);
});
