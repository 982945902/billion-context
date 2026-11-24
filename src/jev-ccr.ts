import { collectCcrCandidates, defaultCountTokens, type Config, type CoreMessage, type MessageContentStore } from "acp-kernel";
import { judgeJevCcr } from "./jev-judge.js";
import { getStore } from "./persist.js";
import type { Session } from "./session.js";

// Keys are host tool-call IDs, which survive a change of raw ID convention.
// A positive value also pins the raw ID that owns the recoverable original.
type Views = Record<string, string | false>;
const noIds = new Set<string>();
const verified = new WeakMap<Session, ReadonlySet<string>>();

export function jevCcrActive(session: Session | undefined): session is Session {
    return process.env.BILI_JEV_CCR === "1" && session?.metadata.pluginAgent === "omp";
}

export function jevReplayPolicy(session: Session): ReadonlySet<string> {
    return verified.get(session) ?? noIds;
}

function matchesOriginal(store: MessageContentStore | null, ref: string | undefined, message: CoreMessage): boolean {
    if (!store || !ref) return false;
    const entry = store.byRef[ref];
    return !!entry && entry.rawId === message.id && typeof message.text === "string" && store.byHash[entry.hash] === message.text;
}

interface JevPlan {
    views: Views;
    storeIds: Set<string>;
    replayIds: Set<string>;
    results: CoreMessage[];
}

export async function planJevCcr(session: Session, messages: CoreMessage[], config: Config, store: MessageContentStore): Promise<JevPlan> {
    verified.delete(session);
    const prior = session.metadata.jevCcrViews as Views | undefined;
    const views: Views = Object.assign(Object.create(null), prior);
    const results = messages.filter(m => m.contentType === "tool-result" && m.toolCallId);
    const seen = new Set<string>();
    const duplicates = new Set<string>();
    for (const result of results) {
        const call = result.toolCallId!;
        if (seen.has(call)) duplicates.add(call);
        seen.add(call);
    }
    const storage = getStore();
    let disk: MessageContentStore | null = null;
    let readable = true;
    if (Object.keys(store.byRef).length) {
        try { disk = storage.unifiedCcrFor(session.id) ?? storage.loadContentStore(session); }
        catch { readable = false; }
    }
    const replayIds = new Set<string>();
    for (const result of results) {
        const call = result.toolCallId!;
        const ref = session.state.messageRefs.byRaw[result.id];
        // An archive from before opt-in or adopted at a fork is usable only
        // if the current session can read the very same payload from disk.
        if (!duplicates.has(call) && (!Object.hasOwn(views, call) || views[call] === result.id) &&
            matchesOriginal(disk, ref, result) && matchesOriginal(store, ref, result)) {
            views[call] = result.id;
            replayIds.add(result.id);
        } else {
            views[call] = false;
        }
    }
    verified.set(session, replayIds);
    // Only the trailing tool response batch can still be awaiting its first
    // model view. A first request after enabling the feature may resend many
    // older batches under the very same user instruction.
    const fresh = new Set<string>();
    for (let i = messages.length - 1; i >= 0 && messages[i]!.contentType === "tool-result"; i--) {
        const call = messages[i]!.toolCallId;
        if (call && !duplicates.has(call) && !Object.hasOwn(prior ?? {}, call)) fresh.add(call);
    }
    const userIndex = messages.findLastIndex(m => m.role === "user");
    const user = messages[userIndex];
    const candidates = [];
    if (storage.enabled && readable && fresh.size && user?.contentType === "text" && user.text) {
        const currentCalls = new Set(messages.slice(userIndex + 1).filter(m => m.contentType === "tool-call").map(m => m.id));
        const inputIds = new Set(results.map(m => m.id));
        for (const { message, call } of collectCcrCandidates({ messages, state: session.state, config, store, countTokens: defaultCountTokens })) {
            if (call && currentCalls.has(call.id) && inputIds.has(message.id) && message.toolCallId && fresh.has(message.toolCallId)) {
                candidates.push({ id: message.id, tool: message.toolName ?? call.toolName ?? "", input: call.text ?? "", text: message.text! });
            }
        }
    }
    const storeIds = candidates.length ? await judgeJevCcr(user!.text!, candidates) : new Set<string>();
    for (const result of results) if (storeIds.has(result.id)) views[result.toolCallId!] = result.id;
    session.metadata.jevCcrViews = views;
    return { views, storeIds, replayIds, results };
}

/** Refresh the replay allowlist only after the session's ordinary reload
 *  sources contain the decision and exact original under the same ref. */
export function confirmJevCcr(session: Session, plan: JevPlan, landed: boolean): boolean {
    if (plan.storeIds.size === 0) return true;
    let confirmed = false;
    if (landed) {
        const storage = getStore();
        try {
            const views = storage.persistedJevViews(session);
            const disk = storage.unifiedCcrFor(session.id) ?? storage.loadContentStore(session);
            confirmed = plan.results.every(m => !plan.storeIds.has(m.id) || (views?.[m.toolCallId!] === m.id &&
                matchesOriginal(disk, session.state.messageRefs.byRaw[m.id], m) && matchesOriginal(session.contentStore ?? null, session.state.messageRefs.byRaw[m.id], m)));
        } catch { /* Failed read-back keeps the full batch. */ }
    }
    if (confirmed) {
        verified.set(session, new Set([...plan.replayIds, ...plan.storeIds]));
    } else {
        for (const m of plan.results) if (plan.storeIds.has(m.id)) plan.views[m.toolCallId!] = false;
        session.metadata.jevCcrViews = plan.views;
    }
    return confirmed;
}
