import { randomUUID } from "node:crypto";
import { loadJson, saveJsonAtomic } from "../shared/store";

/** Drawn at every start. A saga still "running" under another RUN_ID was cut off by a crash. */
export const RUN_ID = randomUUID();

export type State =
  | "processing"
  | "parked"
  | "completed"
  | "failed"
  | "compensated"
  | "compensation_failed"
  | "needs_attention"
  | "duplicate";

export interface Envelope {
  correlationId: string;
  businessKey: string;
  orderId: string;
  source: string;
  orderType: string;
  recipient: string;
  state: State;
  code?: string;
  duplicateOf?: string;
  occurredAt: string;
}

export interface Saga {
  businessKey: string;
  orderId: string;
  source: string;
  orderType: string;
  recipient: string;
  state: State;
  code?: string;
  failedCode?: string;
  duplicateOf?: string;
  steps: Array<{ step: string; status: string; at: string }>;
  compensations: Array<{ step: string; status: string; at: string }>;
  operatorCompensation?: { state: string; code?: string; at: string; body: unknown };
  outcome?: { routingKey: string; envelope: Envelope };
  running: boolean;
  owner: string;
  lastHop?: string;
  stuckHop?: string;
  updatedAt: string;
}

interface Store {
  sagas: Record<string, Saga>;
  businessKeys: Record<string, string>;
}

const store = loadJson<Store>("saga-store.json", { sagas: {}, businessKeys: {} });
if (!store.sagas) store.sagas = {};
if (!store.businessKeys) store.businessKeys = {};

export const sagas = store.sagas;
export const businessKeys = store.businessKeys;
/** Outcomes that still have to be published: after a crash, or when the broker was away. */
export const pendingPublish = new Set<string>();

export const TERMINAL: ReadonlySet<string> = new Set([
  "completed",
  "failed",
  "compensated",
  "compensation_failed",
  "needs_attention",
  "duplicate",
]);

export function save(): void {
  saveJsonAtomic("saga-store.json", store);
}

export function envelopeOf(cid: string, saga: Saga, state: State, code?: string, duplicateOf?: string): Envelope {
  const envelope: Envelope = {
    correlationId: cid,
    businessKey: saga.businessKey,
    orderId: saga.orderId,
    source: saga.source,
    orderType: saga.orderType,
    recipient: saga.recipient,
    state,
    occurredAt: new Date().toISOString(),
  };
  if (code) envelope.code = code;
  if (duplicateOf) envelope.duplicateOf = duplicateOf;
  return envelope;
}

/** Persist a new state together with the outcome event a redelivery would republish (R-4.2.18). */
export function recordOutcome(cid: string, saga: Saga, state: State, code?: string, duplicateOf?: string): void {
  saga.state = state;
  if (code) saga.code = code;
  else delete saga.code;
  if (duplicateOf) saga.duplicateOf = duplicateOf;
  saga.running = false;
  const envelope = envelopeOf(cid, saga, state, code, duplicateOf);
  saga.outcome = { routingKey: `order.${state}`, envelope };
  saga.updatedAt = envelope.occurredAt;
  save();
}

export function completedSteps(saga: Saga): string[] {
  return saga.steps.filter((s) => s.status === "success").map((s) => s.step);
}

export function compensatedSteps(saga: Saga): string[] {
  return saga.compensations.filter((s) => s.status === "success").map((s) => s.step);
}

// Stuck detection (R-4.7.1): at start, a saga whose steps were running under a
// previous run is marked needs_attention. Nothing of it is ever re-run.
for (const [cid, saga] of Object.entries(sagas)) {
  if (saga.state === "processing" && saga.running && saga.owner !== RUN_ID) {
    recordOutcome(cid, saga, "needs_attention");
    pendingPublish.add(cid);
  }
}
