import amqp from "amqplib";
import type { Channel, ConsumeMessage } from "amqplib";
import express from "express";

export type Handler = (msg: ConsumeMessage, channel: Channel) => Promise<void>;

export interface ServiceOptions {
  name: string;
  consume: Record<string, Handler>;
  routes?: (app: express.Express) => void;
}

const RABBITMQ_URL = process.env.RABBITMQ_URL ?? "";
const PORT = Number(process.env.PORT ?? "3000");

let currentChannel: Channel | null = null;

export function getChannel(): Channel | null {
  return currentChannel;
}

/** Step 0 handler: take the message off the queue and do nothing else. */
export const ackOnly: Handler = async (msg, channel) => {
  channel.ack(msg);
};

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export function startService(opts: ServiceOptions): express.Express {
  const state = { broker: false, consumers: {} as Record<string, number> };
  for (const queue of Object.keys(opts.consume)) state.consumers[queue] = 0;

  process.on("unhandledRejection", (err) => console.error(`[${opts.name}] unhandledRejection`, err));
  process.on("uncaughtException", (err) => console.error(`[${opts.name}] uncaughtException`, err));

  const app = express();
  app.use(express.json());

  app.get("/health", (_req, res) => {
    const consuming = Object.values(state.consumers).every((n) => n > 0);
    const ok = state.broker && consuming;
    res.status(ok ? 200 : 503).json({
      status: ok ? "ok" : "degraded",
      checks: { broker: state.broker ? "up" : "down", consumers: { ...state.consumers } },
    });
  });

  opts.routes?.(app);
  app.listen(PORT, () => console.log(`[${opts.name}] listening on ${PORT}`));

  // Connect, consume, and when the connection or the channel drops, do it all
  // again. The process never exits: a restart of the broker is not our restart.
  void (async () => {
    for (;;) {
      try {
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
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

        for (const [queue, handler] of Object.entries(opts.consume)) {
          await channel.consume(queue, async (msg) => {
            if (!msg) {
              // the broker cancelled our consumer: start over
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
          state.consumers[queue] = 1;
        }

        currentChannel = channel;
        state.broker = true;
        console.log(`[${opts.name}] connected to the broker`);
        await closed;
      } catch (err) {
        console.error(`[${opts.name}] broker not available: ${(err as Error).message}`);
      }
      state.broker = false;
      currentChannel = null;
      for (const queue of Object.keys(state.consumers)) state.consumers[queue] = 0;
      await sleep(1000);
    }
  })();

  return app;
}
