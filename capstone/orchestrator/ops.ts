import { emitHop, newId } from "../shared/trace";
import { compensatedSteps, completedSteps, pendingPublish, recordOutcome, sagas, save } from "./state";
import { publishEvent } from "./events";
import { callUndo } from "./steps";

const running = new Set<string>();

export function stuckList() {
  return Object.entries(sagas)
    .filter(([, s]) => s.state === "needs_attention" || s.state === "compensation_failed")
    .map(([cid, s]) => ({
      correlationId: cid,
      businessKey: s.businessKey,
      state: s.state,
      completedSteps: completedSteps(s),
      compensatedSteps: compensatedSteps(s),
      since: s.updatedAt,
    }));
}

export async function operatorCompensate(cid: string): Promise<{ status: number; body: unknown }> {
  const saga = sagas[cid];
  if (!saga) return { status: 404, body: { code: "not_found", message: "no saga with this correlationId" } };
  if (running.has(cid)) return { status: 409, body: { code: "compensation_in_progress", message: "a compensation is already running" } };
  if (saga.state === "compensated" && saga.operatorCompensation) return { status: 200, body: saga.operatorCompensation.body };
  if (saga.state !== "needs_attention" && saga.state !== "compensation_failed") {
    return { status: 409, body: { code: "saga_not_stuck", message: `the saga is ${saga.state}, not stuck` } };
  }

  running.add(cid);
  try {
    const wasStuck = saga.state === "needs_attention";
    const cause = saga.lastHop ?? null;
    const opHop = newId();
    const t0 = Date.now();
    const done = new Set(compensatedSteps(saga));
    const todo = completedSteps(saga)
      .filter((s) => s === "payment" || s === "inventory")
      .reverse()
      .map((s) => (s === "payment" ? "payment_refund" : "inventory_release"))
      .filter((u) => !done.has(u));

    let prev = opHop;
    let failCode: string | undefined;
    for (const undo of todo) {
      const hopId = newId();
      const startedAt = Date.now();
      const r = await callUndo(undo, saga.orderId, saga.businessKey, cid, hopId);
      emitHop({ hopId, correlationId: cid, causationId: prev, service: "orchestrator", operation: undo, status: r.status, startedAt, finishedAt: Date.now(), ...(r.ok ? {} : { detail: { code: r.code } }) });
      saga.compensations.push({ step: undo, status: r.ok ? "success" : "failed", at: new Date().toISOString() });
      saga.lastHop = hopId;
      save();
      if (!r.ok) {
        failCode = r.code;
        break;
      }
      prev = hopId;
    }

    const state = failCode ? "compensation_failed" : "compensated";
    const code = failCode ?? (wasStuck ? "operator_compensated" : saga.failedCode ?? "operator_compensated");
    recordOutcome(cid, saga, state, code);
    const body = { correlationId: cid, state };
    saga.operatorCompensation = { state, code, at: new Date().toISOString(), body };
    save();
    emitHop({ hopId: opHop, correlationId: cid, causationId: cause, service: "orchestrator", operation: "saga.operator_compensate", status: failCode ? "failed" : "success", startedAt: t0, finishedAt: Date.now(), detail: { state } });
    try {
      await publishEvent(cid, saga.outcome!.routingKey, saga.outcome!.envelope, opHop);
    } catch {
      pendingPublish.add(cid); // the flush loop in server.ts publishes it when the broker is back
    }
    return { status: 200, body };
  } finally {
    running.delete(cid);
  }
}
