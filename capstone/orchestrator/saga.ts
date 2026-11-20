import fs from "node:fs";
import { randomUUID } from "node:crypto";
import type { ConfirmChannel, ConsumeMessage } from "amqplib";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import { publishConfirmed } from "../shared/service";
import { getRetryCount } from "../shared/retry";
import { emitHop, newId } from "../shared/trace";
import { RUN_ID, TERMINAL, businessKeys, envelopeOf, recordOutcome, sagas, save } from "./state";
import type { Saga } from "./state";
import { publishEvent } from "./events";
import { runSaga, sendAndAck } from "./run";
import type { Order } from "./steps";

const CONCURRENCY = Number(process.env.SAGA_CONCURRENCY ?? "5");
const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const validateOrder = ajv.compile(JSON.parse(fs.readFileSync("/canonical/order.schema.json", "utf8")));

// A counting semaphore: at most SAGA_CONCURRENCY sagas run at once (R-4.10.1).
let active = 0;
const waiters: Array<() => void> = [];
async function acquire(): Promise<void> {
  if (active < CONCURRENCY) {
    active += 1;
    return;
  }
  await new Promise<void>((resolve) => waiters.push(resolve));
}
function release(): void {
  const next = waiters.shift();
  if (next) next();
  else active -= 1;
}

export async function onMessage(msg: ConsumeMessage, channel: ConfirmChannel): Promise<void> {
  await acquire();
  try {
    await handle(msg, channel);
  } finally {
    release();
  }
}

async function quarantine(msg: ConsumeMessage, channel: ConfirmChannel, cid: string | undefined, cause: string | null, t0: number, why: string): Promise<void> {
  await publishConfirmed("orders.invalid.exchange", "", msg.content, {
    persistent: true,
    messageId: randomUUID(),
    headers: msg.properties.headers ?? {},
  });
  if (cid) {
    emitHop({ hopId: newId(), correlationId: cid, causationId: cause, service: "orchestrator", operation: "invalid.quarantine", status: "quarantined", startedAt: t0, finishedAt: Date.now(), detail: { reason: why } });
  }
  channel.ack(msg);
}

function newSaga(order: Order & { customer: { email: string } }, bk: string, hop: string, running: boolean): Saga {
  return {
    businessKey: bk,
    orderId: order.orderId,
    source: order.source,
    orderType: order.orderType,
    recipient: order.customer.email,
    state: "processing",
    steps: [],
    compensations: [],
    running,
    owner: RUN_ID,
    lastHop: hop,
    updatedAt: new Date().toISOString(),
  };
}

async function handle(msg: ConsumeMessage, channel: ConfirmChannel): Promise<void> {
  const t0 = Date.now();
  const headers = msg.properties.headers ?? {};
  const rawCid = headers["correlationId"];
  const cid = typeof rawCid === "string" ? rawCid : undefined;
  const rawCause = headers["causationId"];
  const header = typeof rawCause === "string" ? rawCause : null;
  const attempt = getRetryCount(msg) + 1;

  let order: Order;
  try {
    if (!cid) throw new Error("no correlationId header");
    const parsed: unknown = JSON.parse(msg.content.toString("utf8"));
    if (!validateOrder(parsed)) throw new Error("not a canonical order");
    order = parsed as Order;
  } catch (err) {
    await quarantine(msg, channel, cid, header, t0, (err as Error).message);
    return;
  }
  const correlationId = cid as string;
  const bk = `${order.source}:${order.orderId}`;
  const consumeHop = newId();
  emitHop({ hopId: consumeHop, correlationId, causationId: header, service: "orchestrator", operation: "saga.consume", status: "success", attempt, startedAt: t0, finishedAt: Date.now(), ...(msg.properties.messageId ? { messageId: msg.properties.messageId } : {}) });

  let saga = sagas[correlationId];
  const owner = businessKeys[bk];

  if (!saga && owner && owner !== correlationId) {
    // The same business key under another correlationId: a duplicate. No step runs.
    saga = newSaga(order, bk, consumeHop, false);
    sagas[correlationId] = saga;
    recordOutcome(correlationId, saga, "duplicate", undefined, owner);
    const hop = newId();
    emitHop({ hopId: hop, correlationId, causationId: consumeHop, service: "orchestrator", operation: "saga.dedupe", status: "duplicate", attempt, startedAt: t0, finishedAt: Date.now(), detail: { duplicateOf: owner } });
    await sendAndAck({ cid: correlationId, saga, msg, channel }, hop);
    return;
  }

  if (saga && TERMINAL.has(saga.state)) {
    // A redelivery of a saga that has an outcome: republish it, run nothing (R-4.2.4).
    const hop = newId();
    emitHop({ hopId: hop, correlationId, causationId: consumeHop, service: "orchestrator", operation: "saga.dedupe", status: "skipped", attempt, startedAt: t0, finishedAt: Date.now() });
    await sendAndAck({ cid: correlationId, saga, msg, channel }, hop);
    return;
  }

  if (saga && saga.running) {
    if (saga.owner === RUN_ID) {
      channel.ack(msg); // this run is already on it
      return;
    }
    recordOutcome(correlationId, saga, "needs_attention"); // steps under a run that no longer exists: stuck
    const hop = newId();
    emitHop({ hopId: hop, correlationId, causationId: saga.lastHop ?? consumeHop, service: "orchestrator", operation: "saga.stuck", status: "success", attempt, startedAt: t0, finishedAt: Date.now() });
    saga.stuckHop = hop;
    save();
    await sendAndAck({ cid: correlationId, saga, msg, channel }, hop);
    return;
  }

  const wasProcessing = saga?.state === "processing";
  if (!saga) {
    saga = newSaga(order, bk, consumeHop, true);
    sagas[correlationId] = saga;
    businessKeys[bk] = correlationId;
  }
  saga.state = "processing";
  saga.running = true;
  saga.owner = RUN_ID;
  saga.lastHop = consumeHop;
  saga.updatedAt = new Date().toISOString();
  save();

  if (!wasProcessing) {
    try {
      await publishEvent(correlationId, "order.processing", envelopeOf(correlationId, saga, "processing"), consumeHop);
    } catch (err) {
      console.error("[orchestrator] order.processing not published:", (err as Error).message);
    }
  }

  try {
    await runSaga({ cid: correlationId, order, saga, bk, msg, channel, consumeHop, attempt });
  } catch (err) {
    saga.running = false;
    save();
    throw err;
  }
}
