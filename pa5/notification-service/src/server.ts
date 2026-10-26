import { appendFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import type { ConsumeMessage } from "amqplib";
import { connectWithRetry, getRetryCount } from "../../shared/rabbit";
import type { CanonicalOrder } from "../../shared/canonical-order";

const QUEUE = "notifications.queue";
const RESULTS_EXCHANGE = "results.notification";
const DLQ_EXCHANGE = process.env.DLQ_EXCHANGE ?? "orders.dlq.exchange";
const INVALID_EXCHANGE = "orders.invalid.exchange";
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? "3");
const DATA_DIR = "/data";
const IDS_FILE = `${DATA_DIR}/processed-ids.json`;
const LOG_FILE = `${DATA_DIR}/notification.log`;

class PermanentError extends Error {}

function loadProcessed(): Set<string> {
  try {
    if (existsSync(IDS_FILE)) {
      const parsed = JSON.parse(readFileSync(IDS_FILE, "utf8")) as unknown;
      if (Array.isArray(parsed)) return new Set(parsed.map(String));
    }
  } catch {
    // unreadable file: start empty rather than crash the consumer
  }
  return new Set();
}

function persistProcessed(ids: Set<string>): void {
  writeFileSync(IDS_FILE, JSON.stringify([...ids]));
}

async function main(): Promise<void> {
  mkdirSync(DATA_DIR, { recursive: true });
  const { channel } = await connectWithRetry(process.env.RABBITMQ_URL!);
  await channel.prefetch(1);

  await channel.assertExchange(DLQ_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.dlq", { durable: true });
  await channel.bindQueue("orders.dlq", DLQ_EXCHANGE, "");
  await channel.assertExchange(INVALID_EXCHANGE, "fanout", { durable: true });
  await channel.assertQueue("orders.invalid", { durable: true });
  await channel.bindQueue("orders.invalid", INVALID_EXCHANGE, "");

  console.log(`[Notification] Consuming from ${QUEUE}`);

  await channel.consume(QUEUE, async (msg: ConsumeMessage | null) => {
    if (!msg) return;

    const correlationId = msg.properties.headers?.["correlationId"] as string | undefined;
    const retryCount = getRetryCount(msg);

    console.log(`[Notification] Processing ${correlationId} (attempt ${retryCount + 1})`);

    try {
      const order = JSON.parse(msg.content.toString()) as CanonicalOrder;
      if (!correlationId) throw new PermanentError("missing correlationId header");

      const processed = loadProcessed();
      if (processed.has(correlationId)) {
        console.log(`[Notification] duplicate skipped ${correlationId}`);
        channel.ack(msg);
        return;
      }

      appendFileSync(
        LOG_FILE,
        JSON.stringify({
          correlationId,
          orderId: order.orderId,
          customerEmail: order.customer.email,
          timestamp: new Date().toISOString(),
          message: "Order received",
        }) + "\n",
      );
      processed.add(correlationId);
      persistProcessed(processed);

      channel.publish(
        RESULTS_EXCHANGE,
        "",
        Buffer.from(
          JSON.stringify({
            correlationId,
            source: "notification",
            status: "success",
            timestamp: new Date().toISOString(),
            details: { message: "Order received" },
          }),
        ),
        { headers: { correlationId }, contentType: "application/json", persistent: true },
      );
      channel.ack(msg);
      console.log(`[Notification] OK ${correlationId}`);
    } catch (err) {
      if (err instanceof SyntaxError || err instanceof PermanentError) {
        channel.publish(INVALID_EXCHANGE, "", msg.content, {
          headers: msg.properties.headers,
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Notification] -> invalid: ${(err as Error).message}`);
        return;
      }

      if (retryCount >= MAX_RETRIES - 1) {
        channel.publish(DLQ_EXCHANGE, "", msg.content, {
          headers: { ...msg.properties.headers, originQueue: QUEUE },
          persistent: true,
        });
        channel.ack(msg);
        console.log(`[Notification] -> DLQ after ${retryCount + 1} attempts: ${(err as Error).message}`);
      } else {
        channel.nack(msg, false, false);
        console.log(`[Notification] -> Retry (attempt ${retryCount + 1}): ${(err as Error).message}`);
      }
    }
  });
}

main().catch(console.error);
