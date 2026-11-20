import { randomUUID } from "node:crypto";
import type { ConfirmChannel, ConsumeMessage } from "amqplib";
import { publishConfirmed } from "../shared/service";
import { emitHop, newId } from "../shared/trace";
import { envelopeOf, recordOutcome, save } from "./state";
import type { Saga, State } from "./state";
import { publishEvent } from "./events";
import { callForward, callUndo, planFor } from "./steps";
import type { CallResult, Order } from "./steps";

const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");

export interface Ctx {
  cid: string;
  order: Order;
  saga: Saga;
  bk: string;
  msg: ConsumeMessage;
  channel: ConfirmChannel;
  consumeHop: string;
  attempt: number;
}

const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const iso = () => new Date().toISOString();

/** Publish the saga's stored outcome, then ack. If the publish fails the message goes back: a redelivery republishes. */
export async function sendAndAck(
  ctx: { cid: string; saga: Saga; msg: ConsumeMessage; channel: ConfirmChannel },
  cause: string,
): Promise<void> {
  const outcome = ctx.saga.outcome;
  if (!outcome) {
    ctx.channel.ack(ctx.msg);
    return;
  }
  try {
    await publishEvent(ctx.cid, outcome.routingKey, outcome.envelope, cause);
  } catch (err) {
    console.error("[orchestrator] outcome not published, putting the message back:", (err as Error).message);
    await sleep(1000);
    try {
      ctx.channel.nack(ctx.msg, false, true);
    } catch {
      // the channel is gone: the broker redelivers anyway
    }
    return;
  }
  ctx.channel.ack(ctx.msg);
}

async function conclude(ctx: Ctx, state: State, code: string | undefined, cause: string, failedCode?: string): Promise<void> {
  recordOutcome(ctx.cid, ctx.saga, state, code);
  if (failedCode) {
    ctx.saga.failedCode = failedCode;
    save();
  }
  await sendAndAck(ctx, cause);
}

async function firstStepFailed(ctx: Ctx, name: string, r: CallResult): Promise<void> {
  const { cid, saga, msg, channel, consumeHop, attempt } = ctx;
  saga.running = false;
  save();
  const lastError = r.http ? `${name} ${r.http}` : `${name} ${r.code}`;
  const hopId = newId();
  const startedAt = Date.now();
  if (attempt < MAX_RETRIES) {
    emitHop({ hopId, correlationId: cid, causationId: consumeHop, service: "orchestrator", operation: "saga.retry", status: "retried", attempt, startedAt, finishedAt: Date.now(), detail: { code: r.code } });
    channel.nack(msg, false, false);
    return;
  }
  await publishConfirmed("orders.dlq.exchange", "", msg.content, {
    persistent: true,
    contentType: "application/json",
    messageId: randomUUID(),
    headers: { ...(msg.properties.headers ?? {}), originQueue: "saga.queue", lastError, causationId: hopId },
  });
  emitHop({ hopId, correlationId: cid, causationId: consumeHop, service: "orchestrator", operation: "saga.dead_letter", status: "dead_lettered", attempt, startedAt, finishedAt: Date.now(), detail: { lastError } });
  saga.state = "parked";
  saga.updatedAt = iso();
  save();
  try {
    await publishEvent(cid, "order.parked", envelopeOf(cid, saga, "parked"), hopId);
  } catch (err) {
    console.error("[orchestrator] order.parked not published:", (err as Error).message);
  }
  channel.ack(msg);
}

async function compensate(ctx: Ctx, completed: string[], failedHop: string, failCode: string): Promise<void> {
  const { cid, saga, bk, order, attempt } = ctx;
  let prev = failedHop;
  let undone = 0;
  for (const step of [...completed].reverse()) {
    const undo = step === "payment" ? "payment_refund" : step === "inventory" ? "inventory_release" : null;
    if (!undo) continue; // shipping and notification have no undo
    const hopId = newId();
    const startedAt = Date.now();
    const r = await callUndo(undo, order.orderId, bk, cid, hopId);
    emitHop({ hopId, correlationId: cid, causationId: prev, service: "orchestrator", operation: undo, status: r.status, attempt, startedAt, finishedAt: Date.now(), ...(r.ok ? {} : { detail: { code: r.code } }) });
    saga.compensations.push({ step: undo, status: r.ok ? "success" : "failed", at: iso() });
    saga.lastHop = hopId;
    save();
    if (!r.ok) {
      await conclude(ctx, "compensation_failed", r.code, hopId, failCode);
      return;
    }
    prev = hopId;
    undone += 1;
  }
  await conclude(ctx, undone > 0 ? "compensated" : "failed", failCode, prev);
}

export async function runSaga(ctx: Ctx): Promise<void> {
  const { cid, order, saga, bk, consumeHop, attempt } = ctx;
  const steps = planFor(order.orderType);
  const completed: string[] = [];
  let prev = consumeHop;
  for (const name of steps) {
    const hopId = newId();
    const startedAt = Date.now();
    const r = await callForward(name, order, bk, cid, hopId);
    emitHop({ hopId, correlationId: cid, causationId: prev, service: "orchestrator", operation: name, status: r.status, attempt, startedAt, finishedAt: Date.now(), ...(r.ok ? {} : { detail: { code: r.code } }) });
    if (r.ok) {
      saga.steps.push({ step: name, status: "success", at: iso() });
      saga.lastHop = hopId;
      save();
      completed.push(name);
      prev = hopId;
      continue;
    }
    if (name === steps[0] && !r.permanent) {
      await firstStepFailed(ctx, name, r); // nothing is known: the retry path
      return;
    }
    saga.steps.push({ step: name, status: "failed", at: iso() });
    saga.lastHop = hopId;
    save();
    if (name === steps[0]) {
      await conclude(ctx, "failed", r.code, hopId); // a 4xx on the first step: nothing to undo
      return;
    }
    await compensate(ctx, completed, hopId, r.code);
    return;
  }
  await conclude(ctx, "completed", undefined, prev);
}
