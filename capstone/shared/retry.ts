import type { ConsumeMessage } from "amqplib";

/** How many times this message was rejected into the retry path (PA5's getRetryCount). */
export function getRetryCount(msg: ConsumeMessage): number {
  const xDeath = msg.properties.headers?.["x-death"] as Array<{ count?: number; reason?: string }> | undefined;
  if (!Array.isArray(xDeath)) return 0;
  return xDeath
    .filter((entry) => entry?.reason === "rejected")
    .reduce((sum, entry) => sum + (Number(entry?.count) || 0), 0);
}

/** A copy of the headers without RabbitMQ's dead-lettering history, for a replay. */
export function withoutRetryHistory(headers: Record<string, unknown> | undefined): Record<string, unknown> {
  const kept: Record<string, unknown> = {};
  for (const [key, value] of Object.entries(headers ?? {})) {
    if (key === "x-death" || key.startsWith("x-first-death-") || key.startsWith("x-last-death-")) continue;
    kept[key] = value;
  }
  return kept;
}
