import { randomUUID } from "node:crypto";
import type { ConsumeMessage, GetMessage } from "amqplib";
import express from "express";
import { getChannel, publishConfirmed } from "../shared/service";
import { getRetryCount, withoutRetryHistory } from "../shared/retry";
import { emitHop, newId } from "../shared/trace";

const DLQ = "orders.dlq";

// AMQP cannot look at a queue without taking from it. So: take everything, answer, put back what stays.
// One operation at a time, or the second would find an empty queue while the first holds its messages.
let busy: Promise<unknown> = Promise.resolve();
function exclusive<T>(fn: () => Promise<T>): Promise<T> {
  const run = busy.then(fn);
  busy = run.catch(() => undefined);
  return run;
}

async function takeAll(): Promise<GetMessage[]> {
  const channel = getChannel();
  if (!channel) throw new Error("broker_unavailable");
  const taken: GetMessage[] = [];
  for (;;) {
    const m = await channel.get(DLQ, { noAck: false });
    if (!m) break;
    taken.push(m);
  }
  return taken;
}

function text(value: unknown, fallback: string): string {
  return typeof value === "string" && value !== "" ? value : fallback;
}

function entryOf(m: GetMessage) {
  const h = (m.properties.headers ?? {}) as Record<string, unknown>;
  const stamp = typeof h["deadLetteredAt"] === "string" ? Date.parse(h["deadLetteredAt"]) : Number.NaN;
  return {
    messageId: text(m.properties.messageId, ""),
    correlationId: text(h["correlationId"], ""),
    originQueue: text(h["originQueue"], "unknown"),
    attempts: Math.max(1, getRetryCount(m as unknown as ConsumeMessage) + 1),
    lastError: text(h["lastError"], "unknown"),
    deadLetteredAt: new Date(Number.isNaN(stamp) ? Date.now() : stamp).toISOString(),
  };
}

export function registerDlq(app: express.Express): void {
  // Non-destructive (R-4.6.2): the messages are taken, listed and put back.
  app.get("/dlq", async (_req, res) => {
    try {
      const body = await exclusive(async () => {
        const channel = getChannel();
        if (!channel) throw new Error("broker_unavailable");
        const taken = await takeAll();
        const entries = taken.map(entryOf);
        for (const m of taken) channel.nack(m, false, true);
        return { count: entries.length, entries };
      });
      res.status(200).json(body);
    } catch (err) {
      res.status(503).json({ code: "broker_unavailable", message: (err as Error).message });
    }
  });

  app.post("/dlq/replay", express.json(), async (req, res) => {
    const wanted = typeof req.body?.correlationId === "string" ? (req.body.correlationId as string) : undefined;
    try {
      const body = await exclusive(async () => {
        const channel = getChannel();
        if (!channel) throw new Error("broker_unavailable");
        const taken = await takeAll();
        const keep: GetMessage[] = [];
        let replayed = 0;
        for (const m of taken) {
          const e = entryOf(m);
          const h = (m.properties.headers ?? {}) as Record<string, unknown>;
          if ((wanted && e.correlationId !== wanted) || e.originQueue === "unknown") {
            keep.push(m);
            continue;
          }
          const hop = newId();
          const t0 = Date.now();
          try {
            // Back to the queue it failed on, with a fresh retry history (R-4.6.4).
            await publishConfirmed("orders.return.exchange", e.originQueue, m.content, {
              persistent: true,
              contentType: "application/json",
              messageId: randomUUID(),
              headers: { ...withoutRetryHistory(h), causationId: hop },
            });
          } catch {
            keep.push(m);
            continue;
          }
          channel.ack(m); // only after the broker confirmed the republish (R-4.6.5)
          replayed += 1;
          if (e.correlationId) {
            emitHop({ hopId: hop, correlationId: e.correlationId, causationId: typeof h["causationId"] === "string" ? h["causationId"] : null, service: "intake", operation: "dlq.replay", status: "success", startedAt: t0, finishedAt: Date.now() });
          }
        }
        for (const m of keep) channel.nack(m, false, true);
        const q = await channel.checkQueue(DLQ);
        return { replayed, remaining: q.messageCount };
      });
      res.status(200).json(body);
    } catch (err) {
      res.status(503).json({ code: "broker_unavailable", message: (err as Error).message });
    }
  });
}
