import express from "express";
import { randomUUID } from "node:crypto";
import type { Channel } from "amqplib";
import { connectWithRetry, withoutRetryHistory } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const PORT = process.env.PORT ? Number(process.env.PORT) : 3000;
const ORDERS_EXCHANGE = "orders.exchange";
const RETURN_EXCHANGE = "orders.return.exchange";
const DLQ_EXCHANGE = "orders.dlq.exchange";
const DLQ_QUEUE = "orders.dlq";

const orders = new Map<string, CanonicalOrder>();

function looksLikeOrder(body: unknown): body is CanonicalOrder {
  if (typeof body !== "object" || body === null) return false;
  const o = body as Record<string, unknown>;
  const customer = o["customer"] as Record<string, unknown> | undefined;
  return (
    typeof o["orderId"] === "string" &&
    typeof customer === "object" &&
    customer !== null &&
    typeof customer["email"] === "string" &&
    Array.isArray(o["items"]) &&
    o["items"].length > 0
  );
}

async function main(): Promise<void> {
  const { channel } = (await connectWithRetry(process.env.RABBITMQ_URL!)) as { channel: Channel };

  // Replay reads from the DLQ, so it must exist (same properties as everywhere).
  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue(DLQ_QUEUE, { durable: true });
  await channel.bindQueue(DLQ_QUEUE, DLQ_EXCHANGE, "");

  const app = express();
  app.use(express.json());

  app.get("/health", (_req, res) => {
    res.status(200).json({ status: "ok" });
  });

  app.post("/orders", (req, res) => {
    if (!looksLikeOrder(req.body)) {
      res.status(400).json({ error: "body is not a canonical order" });
      return;
    }
    const order = req.body;
    const correlationId = randomUUID();
    orders.set(correlationId, order);
    channel.publish(ORDERS_EXCHANGE, "", Buffer.from(JSON.stringify(order)), {
      headers: { correlationId },
      contentType: "application/json",
      persistent: true,
    });
    res.status(201).json({ correlationId, status: "accepted" });
  });

  app.get("/orders/:correlationId", (req, res) => {
    const order = orders.get(req.params.correlationId);
    if (!order) {
      res.status(404).json({ error: "not found" });
      return;
    }
    res.status(200).json({ correlationId: req.params.correlationId, order });
  });

  app.post("/dlq/replay", async (_req, res) => {
    let replayed = 0;
    try {
      for (;;) {
        const msg = await channel.get(DLQ_QUEUE, { noAck: false });
        if (!msg) break;
        const headers = msg.properties.headers;
        const originQueue = headers?.["originQueue"];
        if (typeof originQueue !== "string") {
          // Cannot tell who failed it: put it back and stop instead of looping.
          channel.nack(msg, false, true);
          break;
        }
        channel.publish(RETURN_EXCHANGE, originQueue, msg.content, {
          headers: withoutRetryHistory(headers),
          contentType: "application/json",
          persistent: true,
        });
        channel.ack(msg);
        replayed += 1;
      }
      res.status(200).json({ replayed });
    } catch (err) {
      console.error("[order-service] replay failed:", err);
      res.status(500).json({ error: "replay failed", replayed });
    }
  });

  app.listen(PORT, () => console.log(`[order-service] listening on ${PORT}`));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
