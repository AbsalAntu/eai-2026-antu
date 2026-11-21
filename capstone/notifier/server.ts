import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { ConfirmChannel, ConsumeMessage } from "amqplib";
import { startService, publishConfirmed } from "../shared/service";
import { getRetryCount } from "../shared/retry";
import { DATA_DIR, loadJson, saveJsonAtomic } from "../shared/store";
import { emitHop, newId } from "../shared/trace";

const QUEUE = "notifications.queue";
const NOTIFICATION_URL = process.env.NOTIFICATION_URL ?? "";
const TIMEOUT_MS = Number(process.env.REQUEST_TIMEOUT_MS ?? "2500");
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");

// The notified correlationIds live in a file: after a restart a redelivered event is still a duplicate (R-4.5.3).
const processed = new Set<string>(loadJson<string[]>("processed-ids.json", []));
saveJsonAtomic("processed-ids.json", [...processed]);

interface SendResult {
  ok: boolean;
  status: "success" | "failed" | "timeout";
  lastError: string;
}

interface Outcome {
  orderId: string;
  recipient: string;
  code?: string;
}

async function sendNotice(body: unknown, cid: string, hop: string): Promise<SendResult> {
  try {
    const res = await fetch(`${NOTIFICATION_URL}/notification/send`, {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Correlation-Id": cid, "X-Causation-Id": hop },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(TIMEOUT_MS),
    });
    if (res.ok) return { ok: true, status: "success", lastError: "" };
    return { ok: false, status: "failed", lastError: `notification ${res.status}` };
  } catch (err) {
    const name = (err as Error).name;
    if (name === "TimeoutError" || name === "AbortError") {
      return { ok: false, status: "timeout", lastError: "notification timeout" };
    }
    return { ok: false, status: "failed", lastError: "notification unreachable" };
  }
}

async function handle(msg: ConsumeMessage, channel: ConfirmChannel): Promise<void> {
  const t0 = Date.now();
  const headers = msg.properties.headers ?? {};
  const rawCid = headers["correlationId"];
  const cid = typeof rawCid === "string" ? rawCid : undefined;
  const rawCause = headers["causationId"];
  const header = typeof rawCause === "string" ? rawCause : null;
  const attempt = getRetryCount(msg) + 1;

  let event: Outcome;
  try {
    if (!cid) throw new Error("no correlationId header");
    const parsed = JSON.parse(msg.content.toString("utf8")) as Partial<Outcome>;
    if (typeof parsed?.orderId !== "string" || typeof parsed?.recipient !== "string") {
      throw new Error("not an outcome event");
    }
    event = parsed as Outcome;
  } catch (err) {
    // A malformed message is permanent: original bytes and headers to the invalid channel, never retried.
    await publishConfirmed("orders.invalid.exchange", "", msg.content, {
      persistent: true,
      messageId: randomUUID(),
      headers,
    });
    if (cid) {
      emitHop({ hopId: newId(), correlationId: cid, causationId: header, service: "notifier", operation: "invalid.quarantine", status: "quarantined", startedAt: t0, finishedAt: Date.now(), detail: { reason: (err as Error).message } });
    }
    channel.ack(msg);
    return;
  }
  const correlationId = cid as string;

  const consumeHop = newId();
  emitHop({ hopId: consumeHop, correlationId, causationId: header, service: "notifier", operation: "notify.consume", status: "success", attempt, startedAt: t0, finishedAt: Date.now(), ...(msg.properties.messageId ? { messageId: msg.properties.messageId } : {}) });

  if (processed.has(correlationId)) {
    const now = Date.now();
    emitHop({ hopId: newId(), correlationId, causationId: consumeHop, service: "notifier", operation: "notify.duplicate", status: "duplicate", attempt, startedAt: now, finishedAt: now });
    channel.ack(msg);
    return;
  }

  const sendHop = newId();
  const tSend = Date.now();
  const r = await sendNotice(
    { orderId: event.orderId, recipient: event.recipient, kind: "order_not_completed", reason: event.code ?? "unknown" },
    correlationId,
    sendHop,
  );
  emitHop({ hopId: sendHop, correlationId, causationId: consumeHop, service: "notifier", operation: "notify.send", status: r.status, attempt, startedAt: tSend, finishedAt: Date.now(), ...(r.ok ? {} : { detail: { lastError: r.lastError } }) });

  if (r.ok) {
    fs.appendFileSync(
      path.join(DATA_DIR, "notification.log"),
      JSON.stringify({
        correlationId,
        orderId: event.orderId,
        customerEmail: event.recipient,
        reason: event.code ?? "unknown",
        timestamp: new Date().toISOString(),
        message: "Order not completed",
      }) + "\n",
    );
    processed.add(correlationId); // recorded after a successful send
    saveJsonAtomic("processed-ids.json", [...processed]);
    channel.ack(msg);
    return;
  }

  const hopId = newId();
  const tFail = Date.now();
  if (attempt < MAX_RETRIES) {
    emitHop({ hopId, correlationId, causationId: consumeHop, service: "notifier", operation: "notify.retry", status: "retried", attempt, startedAt: tFail, finishedAt: Date.now(), detail: { lastError: r.lastError } });
    channel.nack(msg, false, false); // into the retry path, back to this queue only
    return;
  }
  await publishConfirmed("orders.dlq.exchange", "", msg.content, {
    persistent: true,
    contentType: "application/json",
    messageId: randomUUID(),
    headers: { ...headers, originQueue: QUEUE, lastError: r.lastError, deadLetteredAt: new Date().toISOString(), causationId: hopId },
  });
  emitHop({ hopId, correlationId, causationId: consumeHop, service: "notifier", operation: "notify.dead_letter", status: "dead_lettered", attempt, startedAt: tFail, finishedAt: Date.now(), detail: { lastError: r.lastError } });
  channel.ack(msg);
}

// One message at a time: two copies of the same event can never both pass the duplicate check.
let chain: Promise<void> = Promise.resolve();
function onMessage(msg: ConsumeMessage, channel: ConfirmChannel): Promise<void> {
  const run = chain.then(() => handle(msg, channel));
  chain = run.catch(() => undefined);
  return run;
}

startService({ name: "notifier", consume: { [QUEUE]: onMessage } });
