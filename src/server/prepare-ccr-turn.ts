import type { CompressionCore, CoreMessage, ProcessTurnInput } from "acp-kernel";
import { confirmJevCcr, planJevCcr } from "../jev-ccr.js";
import { SessionStore } from "../persist.js";
import { restoreNudgeStamps, snapshotMessages, type NudgeStamps, type Session } from "../session.js";
import { adoptContentStore, contentStoreOf } from "../store.js";

/** The first provider view is not committed until both its decision and exact
 * original can be read from the session's ordinary reload sources. Chat and
 * Responses must make the same decision on retries and disk failures. */
export async function processPreparedCcrTurn(
    core: CompressionCore, session: Session, input: ProcessTurnInput, jev: boolean,
    transcript: CoreMessage[], windowed: boolean, stamps: NudgeStamps, log: (level: string, msg: string) => void,
) {
    const contentStore = contentStoreOf(session);
    const turnInput = { ...input, contentStore };
    const plan = jev && input.config.ccr ? await planJevCcr(session, input.messages, input.config, contentStore) : undefined;
    const previousStats = plan?.storeIds.size ? { storedBytes: session.stats.storedBytes, storeBytesSaved: session.stats.storeBytesSaved } : undefined;
    let turn = core.processTurn(plan && input.config.ccr ? { ...turnInput, config: { ...input.config, ccr: { ...input.config.ccr, storePolicy: plan.storeIds, replayPolicy: plan.replayIds } } } : turnInput);
    session.state = turn.state;
    const nudgeHeldAfterReject = session.transient?.nudgeHeldAfterReject === true;
    if (nudgeHeldAfterReject) restoreNudgeStamps(session, stamps);
    adoptContentStore(session, turn.contentStore);
    snapshotMessages(session, transcript);
    if (windowed) session.lastMessagesFolded = true;
    let flush = await SessionStore.flushOrSkip(session);
    if (plan && !confirmJevCcr(session, plan, flush.ok)) {
        // Neither the provisional placeholder nor its latch has left the proxy.
        // Re-render from the pre-turn state with only verified replay allowed.
        session.contentStore = contentStore;
        session.contentStoreDirty = true;
        if (previousStats) Object.assign(session.stats, previousStats);
        turn = core.processTurn({ ...turnInput, config: { ...input.config, ccr: { ...input.config.ccr!, storePolicy: new Set(), replayPolicy: plan.replayIds } } });
        session.state = turn.state;
        if (nudgeHeldAfterReject) restoreNudgeStamps(session, stamps);
        flush = await SessionStore.flushOrSkip(session);
        log("warn", `[${session.id}] Jev CCR original or decision could not be verified on disk; forwarded full tool results`);
    }
    return { turn, flush, nudgeHeldAfterReject };
}
