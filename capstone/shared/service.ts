import amqp from "amqplib";
import type { ConfirmChannel, ConsumeMessage } from "amqplib";
import express from "express";

export type Handler = (msg: ConsumeMessage, channel: ConfirmChannel) => Promise<void>;

export interface ServiceOptions {
  name: string;
  consume: Record<string, Handler>;
  routes?: (app: express.Express) => void;
  /** Runs after every (re)connect, before consuming: declare or bind what you need. */
  onConnect?: (channel: ConfirmChannel) => Promise<void>;
}

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "";
const PORT = Number(process.env.PORT ?? "3000");

let currentChannel: ConfirmChannel | null = null;
let brokerUp = false;

export function getChannel(): ConfirmChannel | null {
  return currentChannel;
}

export function isBrokerUp(): boolean {
  return brokerUp;
}

/** Step 0 handler: take the message off the queue and do nothing else. */
export const ackOnly: Handler = async (msg, channel) => {
  channel.ack(msg);
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Publish and wait for the broker's confirm. Rejects with no channel, a nack, or no confirm in 5 s. */
export function publishConfirmed(
  exchange: string,
  routingKey: string,
  content: Buffer,
  options: amqp.Options.Publish,
): Promise<void> {
  const channel = currentChannel;
  if (!channel) return Promise.reject(new Error("broker_unavailable"));
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("publish not confirmed in time")), 5000);
    try {
      channel.publish(exchange, routingKey, content, options, (err) => {
        clearTimeout(timer);
        if (err) reject(err instanceof Error ? err : new Error("publish nacked"));
        else resolve();
      });
    } catch (err) {
      clearTimeout(timer);
      reject(err);
    }
  });
}

/** Fire and forget, for trace hops: never blocks and never throws. */
export function publishNoWait(
  exchange: string,
  routingKey: string,
  content: Buffer,
  options: amqp.Options.Publish,
): void {
  const channel = currentChannel;
  if (!channel) return;
  try {
    channel.publish(exchange, routingKey, content, options, () => undefined);
  } catch {
    // a lost hop must never fail the business operation
  }
}

export function startService(opts: ServiceOptions): express.Express {
  const consumers: Record<string, number> = {};
  for (const queue of Object.keys(opts.consume)) consumers[queue] = 0;

  process.on("unhandledRejection", (err) => console.error(`[${opts.name}] unhandledRejection`, err));
  process.on("uncaughtException", (err) => console.error(`[${opts.name}] uncaughtException`, err));

  // No global body parser: a route that needs the raw bytes parses them itself.
  const app = express();

  app.get("/health", (_req, res) => {
    const consuming = Object.values(consumers).every((n) => n > 0);
    const ok = brokerUp && consuming;
    res.status(ok ? 200 : 503).json({
      status: ok ? "ok" : "degraded",
      checks: { broker: brokerUp ? "up" : "down", consumers: { ...consumers } },
    });
  });

  opts.routes?.(app);
  app.listen(PORT, () => console.log(`[${opts.name}] listening on ${PORT}`));

  // Connect, consume, and when the connection or channel drops, start again.
  // The process never exits: a broker restart is not our restart.
  void (async () => {
    for (;;) {
      try {
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createConfirmChannel();
        await channel.prefetch(10);

        const closed = new Promise<void>((resolve) => {
          connection.on("close", () => resolve());
          connection.on("error", (err: Error) => console.error(`[${opts.name}] connection error: ${err.message}`));
          channel.on("error", (err: Error) => console.error(`[${opts.name}] channel error: ${err.message}`));
          channel.on("close", () => {
            connection.close().catch(() => undefined);
            resolve();
          });
        });

        if (opts.onConnect) await opts.onConnect(channel);

        for (const [queue, handler] of Object.entries(opts.consume)) {
          await channel.consume(queue, async (msg) => {
            if (!msg) {
              channel.close().catch(() => undefined);
              return;
            }
            try {
              await handler(msg, channel);
            } catch (err) {
              console.error(`[${opts.name}] handler failed on ${queue}`, err);
              try {
                channel.nack(msg, false, false);
              } catch {
                // channel already gone
              }
            }
          });
          consumers[queue] = 1;
        }

        currentChannel = channel;
        brokerUp = true;
        console.log(`[${opts.name}] connected to the broker`);
        await closed;
      } catch (err) {
        console.error(`[${opts.name}] broker not available: ${(err as Error).message}`);
      }
      brokerUp = false;
      currentChannel = null;
      for (const queue of Object.keys(consumers)) consumers[queue] = 0;
      await sleep(1000);
    }
  })();

  return app;
}
