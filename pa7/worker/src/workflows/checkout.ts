import { proxyActivities, ActivityFailure, ApplicationFailure, TimeoutFailure } from "@temporalio/workflow";
import type * as activities from "../activities";
import type { CanonicalOrder, CheckoutResult, TraceItem } from "../types";

const act = proxyActivities<typeof activities>({
  startToCloseTimeout: "2500 ms",
  retry: {
    maximumAttempts: 3,
    initialInterval: "200 ms",
    backoffCoefficient: 2,
  },
});

// Whole cents as integers, formatted once at the end. Never a float.
function toCents(price: string): number {
  const negative = price.startsWith("-");
  const [whole = "0", frac = "00"] = price.replace("-", "").split(".");
  const cents = Number(whole) * 100 + Number(frac.padEnd(2, "0").slice(0, 2));
  return negative ? -cents : cents;
}

function computeAmount(items: CanonicalOrder["items"]): string {
  let total = 0;
  for (const item of items) {
    total += toCents(String(item.unitPrice)) * Number(item.quantity);
  }
  const sign = total < 0 ? "-" : "";
  const abs = Math.abs(total);
  return `${sign}${Math.floor(abs / 100)}.${String(abs % 100).padStart(2, "0")}`;
}

type StepOutcome = { ok: true } | { ok: false; code: string; timeout: boolean };

async function runStep(step: string, trace: TraceItem[], call: () => Promise<void>): Promise<StepOutcome> {
  const started = new Date();
  let outcome: StepOutcome = { ok: true };
  try {
    await call();
  } catch (err) {
    if (err instanceof ActivityFailure && err.cause instanceof TimeoutFailure) {
      outcome = { ok: false, code: "timeout", timeout: true };
    } else if (err instanceof ActivityFailure && err.cause instanceof ApplicationFailure) {
      outcome = { ok: false, code: err.cause.type ?? "step_failed", timeout: false };
    } else {
      outcome = { ok: false, code: "step_failed", timeout: false };
    }
  }
  const finished = new Date();
  trace.push({
    step,
    status: outcome.ok ? "success" : outcome.timeout ? "timeout" : "failed",
    startedAt: started.toISOString(),
    finishedAt: finished.toISOString(),
    durationMs: finished.getTime() - started.getTime(),
  });
  return outcome;
}

export async function checkout(order: CanonicalOrder): Promise<CheckoutResult> {
  const orderId = order.orderId;
  const trace: TraceItem[] = [];
  const amount = computeAmount(order.items);

  const forward: Array<{ step: string; call: () => Promise<void> }> = [
    { step: "payment", call: () => act.authorizePayment(orderId, amount) },
    { step: "inventory", call: () => act.reserveInventory(orderId, order.items) },
    { step: "shipping", call: () => act.createShipment(orderId) },
    { step: "notification", call: () => act.sendNotification(orderId, order.customer.email) },
  ];
  const undo: Record<string, { step: string; call: () => Promise<void> }> = {
    inventory: { step: "inventory_release", call: () => act.releaseInventory(orderId) },
    payment: { step: "payment_refund", call: () => act.refundPayment(orderId) },
  };

  const completed: string[] = [];
  let failure: { code: string; timeout: boolean } | null = null;

  for (const s of forward) {
    const r = await runStep(s.step, trace, s.call);
    if (!r.ok) {
      failure = r;
      break;
    }
    completed.push(s.step);
  }

  if (!failure) {
    return { orderId, status: "completed", trace };
  }

  // Compensate what completed, newest first. Shipping and notification have no undo.
  let compensated = false;
  let compensationFailed = false;
  for (const done of [...completed].reverse()) {
    const u = undo[done];
    if (!u) continue;
    compensated = true;
    const r = await runStep(u.step, trace, u.call);
    if (!r.ok) compensationFailed = true;
  }

  if (compensationFailed) {
    return { orderId, status: "failed", code: "compensation_failed", trace };
  }
  return { orderId, status: compensated ? "compensated" : "failed", code: failure.code, trace };
}
