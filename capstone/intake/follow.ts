import type { ConfirmChannel, ConsumeMessage } from "amqplib";

const ALL = ["processing", "parked", "completed", "failed", "compensated", "compensation_failed", "needs_attention", "duplicate"];

// R-4.4.4 to R-4.4.9: the allowed moves. An event for any other move is ignored.
const ALLOWED: Record<string, string[]> = {
  accepted: ALL,
  processing: ["parked", "completed", "failed", "compensated", "compensation_failed", "needs_attention"],
  parked: ["processing", "completed", "failed", "compensated", "compensation_failed", "needs_attention"],
  compensation_failed: ["compensated", "compensation_failed"],
  needs_attention: ["compensated", "compensation_failed"],
  completed: [],
  failed: [],
  compensated: [],
  duplicate: [],
};

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export function makeFollower(orders: Record<string, any>, save: () => void) {
  return {
    declare: async (channel: ConfirmChannel): Promise<void> => {
      await channel.assertQueue("intake.state.queue", { durable: true });
      await channel.bindQueue("intake.state.queue", "orders.outcome", "#");
    },
    handler: async (msg: ConsumeMessage, channel: ConfirmChannel): Promise<void> => {
      try {
        const event = JSON.parse(msg.content.toString("utf8"));
        const view = orders[event.correlationId];
        if (view && typeof event.state === "string" && (ALLOWED[view.state] ?? []).includes(event.state)) {
          view.state = event.state;
          if (event.code) view.code = event.code;
          else delete view.code;
          if (event.duplicateOf) view.duplicateOf = event.duplicateOf;
          view.updatedAt = new Date().toISOString();
          save();
        }
      } catch {
        // a malformed event is ignored
      }
      channel.ack(msg);
    },
  };
}
