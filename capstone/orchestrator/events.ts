import { randomUUID } from "node:crypto";
import { publishConfirmed } from "../shared/service";
import { emitHop, newId } from "../shared/trace";
import type { Envelope } from "./state";

/** Publish a lifecycle or outcome event to orders.outcome, with its saga.outcome hop (R-4.5.1, R-4.5.2). */
export async function publishEvent(cid: string, routingKey: string, envelope: Envelope, cause: string | null): Promise<string> {
  const hopId = newId();
  const startedAt = Date.now();
  let status: "success" | "failed" = "success";
  try {
    await publishConfirmed("orders.outcome", routingKey, Buffer.from(JSON.stringify(envelope)), {
      persistent: true,
      contentType: "application/json",
      messageId: randomUUID(),
      headers: { correlationId: cid, causationId: hopId },
    });
  } catch (err) {
    status = "failed";
    throw err;
  } finally {
    emitHop({
      hopId,
      correlationId: cid,
      causationId: cause,
      service: "orchestrator",
      operation: "saga.outcome",
      status,
      startedAt,
      finishedAt: Date.now(),
    });
  }
  return hopId;
}
