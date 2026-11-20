import { randomUUID } from "node:crypto";
import { publishNoWait } from "./service";

export interface HopInput {
  hopId: string;
  correlationId: string;
  causationId: string | null;
  service: "intake" | "orchestrator" | "notifier" | "observer" | "worker";
  operation: string;
  status: "success" | "failed" | "timeout" | "retried" | "dead_lettered" | "quarantined" | "skipped" | "duplicate";
  attempt?: number;
  messageId?: string;
  startedAt: number;
  finishedAt: number;
  detail?: Record<string, unknown>;
}

export function newId(): string {
  return randomUUID();
}

/** Publish one hop record to the wire tap. Fire and forget: it never blocks or fails the caller. */
export function emitHop(h: HopInput): void {
  try {
    const record: Record<string, unknown> = {
      hopId: h.hopId,
      correlationId: h.correlationId,
      causationId: h.causationId,
      service: h.service,
      operation: h.operation,
      status: h.status,
      attempt: h.attempt ?? 1,
      startedAt: new Date(h.startedAt).toISOString(),
      finishedAt: new Date(h.finishedAt).toISOString(),
      durationMs: Math.max(0, Math.round(h.finishedAt - h.startedAt)),
    };
    if (h.messageId) record.messageId = h.messageId;
    if (h.detail) record.detail = h.detail;
    publishNoWait("trace.events", "", Buffer.from(JSON.stringify(record)), {
      persistent: true,
      contentType: "application/json",
      messageId: randomUUID(),
    });
  } catch {
    // a lost hop must never fail the business operation
  }
}
