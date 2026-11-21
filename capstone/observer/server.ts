import fs from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import type { ConfirmChannel, ConsumeMessage } from "amqplib";
import { startService, getChannel, publishConfirmed } from "../shared/service";
import { DATA_DIR } from "../shared/store";

const FILE = path.join(DATA_DIR, "trace-store.jsonl");
const MANAGEMENT_URL = (process.env.MANAGEMENT_URL ?? "").replace(/\/+$/, "");
const MANAGEMENT_USER = process.env.MANAGEMENT_USER ?? "";
const MANAGEMENT_PASSWORD = process.env.MANAGEMENT_PASSWORD ?? "";
const ORCHESTRATOR_URL = (process.env.ORCHESTRATOR_URL ?? "").replace(/\/+$/, "");

// The two lag thresholds (R-4.9.8, R-4.9.9): the runbook justifies them.
const SAGA_LAG_THRESHOLD = 5;
const NOTIFIER_LAG_THRESHOLD = 5;

interface Hop {
  hopId: string;
  correlationId: string;
  operation: string;
  startedAt: string;
  [key: string]: unknown;
}

const byCorrelation = new Map<string, Hop[]>();
const seen = new Set<string>();

/** Keeps a hop once: the wire tap is at-least-once, so the same hopId may arrive twice. */
function remember(hop: Hop): boolean {
  if (seen.has(hop.hopId)) return false;
  seen.add(hop.hopId);
  const list = byCorrelation.get(hop.correlationId) ?? [];
  list.push(hop);
  byCorrelation.set(hop.correlationId, list);
  return true;
}

// The trace survives a restart: the JSONL file is read back at start (R-3.5.4).
// A line torn by a kill is skipped.
function loadFile(): void {
  fs.mkdirSync(DATA_DIR, { recursive: true });
  if (!fs.existsSync(FILE)) return;
  for (const line of fs.readFileSync(FILE, "utf8").split("\n")) {
    if (!line.trim()) continue;
    try {
      const hop = JSON.parse(line) as Hop;
      if (typeof hop.hopId === "string" && typeof hop.correlationId === "string") remember(hop);
    } catch {
      // a torn line
    }
  }
}
loadFile();

async function onHop(msg: ConsumeMessage, channel: ConfirmChannel): Promise<void> {
  let hop: Hop;
  try {
    const parsed = JSON.parse(msg.content.toString("utf8")) as Partial<Hop>;
    if (
      typeof parsed.hopId !== "string" ||
      typeof parsed.correlationId !== "string" ||
      typeof parsed.operation !== "string" ||
      typeof parsed.startedAt !== "string"
    ) {
      throw new Error("not a hop record");
    }
    hop = parsed as Hop;
  } catch {
    // A malformed record goes to the invalid channel with its original bytes and headers, and is acked.
    await publishConfirmed("orders.invalid.exchange", "", msg.content, {
      persistent: true,
      messageId: randomUUID(),
      headers: msg.properties.headers ?? {},
    });
    channel.ack(msg);
    return;
  }
  if (remember(hop)) fs.appendFileSync(FILE, JSON.stringify(hop) + "\n");
  channel.ack(msg);
}

const QUEUES = ["saga.queue", "notifications.queue", "orders.dlq", "orders.invalid", "trace.store"];

/** Only the management API knows `unacked`. It may report an idle queue late; null means it did not answer. */
async function unackedOf(name: string): Promise<number | null> {
  try {
    const res = await fetch(`${MANAGEMENT_URL}/api/queues/%2F/${encodeURIComponent(name)}`, {
      headers: { Authorization: "Basic " + Buffer.from(`${MANAGEMENT_USER}:${MANAGEMENT_PASSWORD}`).toString("base64") },
      signal: AbortSignal.timeout(2000),
    });
    if (!res.ok) return null;
    const body = (await res.json()) as { messages_unacknowledged?: number };
    return Number(body.messages_unacknowledged ?? 0);
  } catch {
    return null;
  }
}

async function stuckCount(): Promise<number> {
  try {
    const res = await fetch(`${ORCHESTRATOR_URL}/sagas/stuck`, { signal: AbortSignal.timeout(2000) });
    if (!res.ok) return 0;
    const body = (await res.json()) as { sagas?: unknown[] };
    return Array.isArray(body.sagas) ? body.sagas.length : 0;
  } catch {
    return 0;
  }
}

startService({
  name: "observer",
  consume: { "trace.store": onHop },
  routes: (app) => {
    app.get("/trace/:correlationId", (req, res) => {
      const cid = req.params.correlationId ?? "";
      const hops = byCorrelation.get(cid);
      if (!hops || hops.length === 0) {
        res.status(404).json({ code: "not_found", message: "no hops for this correlationId" });
        return;
      }
      const sorted = [...hops].sort((a, b) => Date.parse(a.startedAt) - Date.parse(b.startedAt));
      res.status(200).json({ correlationId: cid, hops: sorted });
    });

    app.get("/ops/metrics", async (_req, res) => {
      const channel = getChannel();
      if (!channel) {
        res.status(503).json({ code: "broker_unavailable", message: "the observer has no broker connection" });
        return;
      }
      try {
        const queues: Record<string, { ready: number; unacked: number; consumers: number }> = {};
        // A passive declare gives ready and consumers exactly, now.
        await Promise.all(
          QUEUES.map(async (name) => {
            const q = await channel.checkQueue(name);
            const unacked = await unackedOf(name);
            queues[name] = { ready: q.messageCount, unacked: unacked ?? 0, consumers: q.consumerCount };
          }),
        );
        const dlqDepth = (queues["orders.dlq"]?.ready ?? 0) + (queues["orders.dlq"]?.unacked ?? 0);
        const invalidDepth = (queues["orders.invalid"]?.ready ?? 0) + (queues["orders.invalid"]?.unacked ?? 0);
        const stuckSagas = await stuckCount();
        const sagaReady = queues["saga.queue"]?.ready ?? 0;
        const notifierReady = queues["notifications.queue"]?.ready ?? 0;
        const sagaConsumers = queues["saga.queue"]?.consumers ?? 0;
        const notifierConsumers = queues["notifications.queue"]?.consumers ?? 0;
        const alerts = [
          { key: "dlq_not_empty", value: dlqDepth, threshold: null, firing: dlqDepth > 0 },
          { key: "invalid_not_empty", value: invalidDepth, threshold: null, firing: invalidDepth > 0 },
          { key: "saga_consumer_missing", value: sagaConsumers, threshold: null, firing: sagaConsumers === 0 },
          { key: "notifier_consumer_missing", value: notifierConsumers, threshold: null, firing: notifierConsumers === 0 },
          { key: "saga_consumer_lag", value: sagaReady, threshold: SAGA_LAG_THRESHOLD, firing: sagaReady > SAGA_LAG_THRESHOLD },
          { key: "notifier_consumer_lag", value: notifierReady, threshold: NOTIFIER_LAG_THRESHOLD, firing: notifierReady > NOTIFIER_LAG_THRESHOLD },
          { key: "stuck_sagas", value: stuckSagas, threshold: null, firing: stuckSagas > 0 },
        ];
        res.status(200).json({ generatedAt: new Date().toISOString(), queues, dlqDepth, invalidDepth, stuckSagas, alerts });
      } catch (err) {
        res.status(503).json({ code: "metrics_unavailable", message: (err as Error).message });
      }
    });
  },
});
