import { randomUUID } from "node:crypto";
import { loadJson, saveJsonAtomic } from "../shared/store";

/** Drawn at every start. A saga still "running" under another RUN_ID was cut off by a crash. */
export const RUN_ID = randomUUID();

export type State =
  | "processing"
  | "parked"
  | "completed"
  | "failed"
  | "compensated"
  | "compensation_failed"
  | "needs_attention"
  | "duplicate";

export interface Envelope {
  correlationId: string;
  businessKey: string;
  orderId: string;
  source: string;
  orderType: string;
  recipient: string;
  state: State;
  code?: string;
  duplicateOf?: string;

cat > orchestrator/steps.ts <<'EOF'
const PAYMENT = process.env.PAYMENT_URL ?? "";
const INVENTORY = process.env.INVENTORY_URL ?? "";
const SHIPPING = process.env.SHIPPING_URL ?? "";
const NOTIFICATION = process.env.NOTIFICATION_URL ?? "";
const TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS ?? "2500");

export interface Order {
  orderId: string;
  orderType: string;
  source: string;
  customer: { email: string };
  items: Array<{ unitPrice: string; quantity: number }>;
}

export interface CallResult {
  ok: boolean;
  /** a 4xx: it will say no again */
  permanent: boolean;
  status: "success" | "failed" | "timeout";
  code: string;
  http: number;
}

function cents(price: string): number {
  const m = /^(-?)(\d+)\.(\d{2})$/.exec(price);
  if (!m) throw new Error(`bad unitPrice: ${price}`);
  const value = Number(m[2]) * 100 + Number(m[3]);
  return m[1] ? -value : value;
}

/** Whole cents as integers, formatted once at the end: "63.90", never a float. */
export function amountOf(items: Array<{ unitPrice: string; quantity: number }>): string {
  let total = 0;
  for (const item of items) total += cents(item.unitPrice) * item.quantity;
  const sign = total < 0 ? "-" : "";
  const abs = Math.abs(total);
  return `${sign}${Math.floor(abs / 100)}.${String(abs % 100).padStart(2, "0")}`;
}

export function planFor(orderType: string): string[] {
  // b2b is invoiced: no payment. Route on orderType, never on source.
  return orderType === "b2b"
    ? ["inventory", "shipping", "notification"]
    : ["payment", "inventory", "shipping", "notification"];
}


cat > orchestrator/events.ts <<'EOF'
import { randomUUID } from "node:crypto";
import { publishConfirmed } from "../shared/service";
import { emitHop, newId } from "../shared/trace";
import type { Envelope } from "./state";

/** Publish a lifecycle or outcome event to orders.outcome, with its saga.outcome hop (R-4.5.1, R-4.5.2). */
export async function publishEvent(cid: string, routingKey: string, envelope: Envelope, cause: string | null): Promise<string> {
  const hopId = newId();
  const startedAt = Date.now();
  let status: "success" | "failed" = "success";
  try {
    await publishConfirmed("orders.outcome", routingKey, Buffer.from(JSON.stringify(envelope)), {
      persistent: true,
      contentType: "application/json",
      messageId: randomUUID(),
      headers: { correlationId: cid, causationId: hopId },
    });
  } catch (err) {
    status = "failed";
    throw err;
  } finally {
    emitHop({
      hopId,
      correlationId: cid,
      causationId: cause,
      service: "orchestrator",
      operation: "saga.outcome",
      status,
      startedAt,
      finishedAt: Date.now(),
    });
  }
  return hopId;
}
