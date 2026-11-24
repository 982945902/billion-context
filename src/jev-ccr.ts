import { collectCcrCandidates, defaultCountTokens, parseStoredPlaceholder, type Config, type CoreMessage, type MessageContentStore } from "acp-kernel";
import type { BiliMessage, ResponseInputItem } from "acp-kernel/wire";
import { judgeJevCcr } from "./jev-judge.js";
import { getStore } from "./persist.js";
import type { Session } from "./session.js";

// Keys are host tool-call IDs, which survive a change of raw ID convention.
// A positive value also pins the raw ID that owns the recoverable original.
type Views = Record<string, string | false>;
const noIds = new Set<string>();
const verified = new WeakMap<Session, ReadonlySet<string>>();

export function jevCcrActive(session: Session | undefined): session is Session {
    const policy = session?.metadata.effectiveCcr as { jevTrial?: boolean } | null | undefined;
    return session?.metadata.pluginAgent === "omp" && (policy?.jevTrial === true || process.env.BILI_JEV_CCR === "1");
}

export function jevReplayPolicy(session: Session): ReadonlySet<string> {
    return verified.get(session) ?? noIds;
}

/** A verified owner can still be missing its tool or fall back to a raw
 * forward. Those results have now been shown in full, even if they used to
 * have a recoverable archive. Do not re-shrink that prefix on retry. */
export function rememberFullJevView(session: Session, messages: CoreMessage[]): void {
    verified.delete(session);
    const views: Views = Object.assign(Object.create(null), session.metadata.jevCcrViews);
    for (const m of messages) if (m.contentType === "tool-result" && m.toolCallId) views[m.toolCallId] = false;
    session.metadata.jevCcrViews = views;
}

function matchesOriginal(store: MessageContentStore | null, ref: string | undefined, message: CoreMessage): boolean {
    if (!store || !ref) return false;
    const entry = store.byRef[ref];
    return !!entry && entry.rawId === message.id && typeof message.text === "string" && store.byHash[entry.hash] === message.text;
}

/** Never send binary/opaque payloads to a text judge, or issue a text ref
 * for an image-bearing result whose complete original cannot be restored. */
function textualResult(m: BiliMessage): boolean {
    const raw = m.rawResponsesItem as ResponseInputItem | undefined;
    if (!raw) return true;
    if (raw.call_id !== m.toolCallId || (raw.type !== "function_call_output" && raw.type !== "custom_tool_call_output") ||
        (raw.status !== undefined && raw.status !== "completed") || raw.is_error === true) return false;
    return typeof raw.output === "string" || (Array.isArray(raw.output) && raw.output.length > 0 &&
        raw.output.every(part => part?.type === "input_text" && typeof part.text === "string"));
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
        // A fold also archives original bytes. On Responses it is not proof
        // that the model saw a placeholder: require its first-view decision
        // or a matching view latch inherited with a fork's prefix.
        const priorView = Object.hasOwn(views, call) ? views[call] === result.id :
            (session.metadata.nativeRetrieveOnly !== true || (!!ref && parseStoredPlaceholder(session.state.viewLatches?.[ref]?.text ?? "")?.ref === ref));
        if (!duplicates.has(call) && textualResult(result) && priorView &&
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
        if (call && textualResult(messages[i]!) && !duplicates.has(call) && !Object.hasOwn(prior ?? {}, call)) fresh.add(call);
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
