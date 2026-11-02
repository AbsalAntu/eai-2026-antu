/**
 * PA6 orchestrator scaffold — ported from the practice-03-orchestration JS lab.
 *
 * This scaffold is intentionally incomplete. `GET /health`, request validation
 * against the canonical order schema, the Idempotency-Key contract shell, and
 * restart-safe file persistence are already wired up. What is NOT implemented
 * is the actual saga:
 *
 *   1) payment authorize
 *   2) inventory reserve
 *   3) shipping create
 *   4) notification send
 *
 * with strict sequencing, trace recording, timeout handling, compensation
 * (in reverse order of completed steps), and the full idempotent-replay
 * policy. See the TODO block inside the POST /checkout handler.
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import express, { type Request, type Response } from "express";
import Ajv from "ajv";
import addFormats from "ajv-formats";
import type { CanonicalOrder } from "./canonical-order";

const app = express();
app.use(express.json());
app.use(express.static(path.join(process.cwd(), "public")));

interface Config {
  port: number;
  paymentUrl: string;
  inventoryUrl: string;
  shippingUrl: string;
  notificationUrl: string;
  requestTimeoutMs: number;
}

function readRequiredEnv(name: string): string {
  const value = process.env[name];
  if (!value) {
    throw new Error(`Missing required environment variable: ${name}`);
  }
  return value;
}

function loadConfig(): Config {
  return {
    port: Number(process.env.ORCHESTRATOR_PORT || 3000),
    paymentUrl: readRequiredEnv("PAYMENT_URL"),
    inventoryUrl: readRequiredEnv("INVENTORY_URL"),
    shippingUrl: readRequiredEnv("SHIPPING_URL"),
    notificationUrl: readRequiredEnv("NOTIFICATION_URL"),
    requestTimeoutMs: Number(process.env.REQUEST_TIMEOUT_MS || 2500),
  };
}

const config = loadConfig();

const DATA_DIR = "/data";
const IDEMPOTENCY_STORE_PATH = path.join(DATA_DIR, "idempotency-store.json");
const SAGA_STORE_PATH = path.join(DATA_DIR, "saga-store.json");

// ---------------------------------------------------------------- contract --
// These shapes mirror grading/schema/*.json. Keep them in sync if you change
// either side — the tests validate persisted files against those schemas.

export interface TraceItem {
  step: string;
  status: string;
  startedAt: string;
  finishedAt: string;
  durationMs: number;
}

export type SagaState = "completed" | "failed" | "compensated";
export type IdempotencyState = "in_progress" | "completed" | "failed" | "compensated";

interface IdempotencyRecord {
  requestHash: string;
  state: IdempotencyState;
  httpStatus: number;
  response: unknown;
  updatedAt: string;
}

interface IdempotencyStore {
  records: Record<string, IdempotencyRecord>;
}

interface SagaRecord {
  idempotencyKey: string;
  state: SagaState;
  steps: TraceItem[];
  updatedAt: string;
}

interface SagaStore {
  sagas: Record<string, SagaRecord>;
}

function ensureJsonFile(filePath: string, initialData: unknown): void {
  const dirPath = path.dirname(filePath);
  if (!fs.existsSync(dirPath)) {
    fs.mkdirSync(dirPath, { recursive: true });
  }
  if (!fs.existsSync(filePath)) {
    fs.writeFileSync(filePath, JSON.stringify(initialData, null, 2), "utf8");
  }
}

function readJsonFile<T>(filePath: string): T {
  ensureJsonFile(filePath, {});
  const raw = fs.readFileSync(filePath, "utf8");
  return JSON.parse(raw || "{}") as T;
}

function writeJsonFile(filePath: string, value: unknown): void {
  fs.writeFileSync(filePath, JSON.stringify(value, null, 2), "utf8");
}

function nowIso(): string {
  return new Date().toISOString();
}

function payloadHash(payload: unknown): string {
  const normalized = JSON.stringify(payload);
  const hash = crypto.createHash("sha256").update(normalized).digest("hex");
  return `sha256:${hash}`;
}

// The request body is a canonical order. docker-compose.yml mounts the
// course's canonical/ folder read-only at /canonical, so the orchestrator
// validates against the very file PA4's output was validated against.
const CANONICAL_SCHEMA_PATH = "/canonical/order.schema.json";

if (!fs.existsSync(CANONICAL_SCHEMA_PATH)) {
  throw new Error(
    `${CANONICAL_SCHEMA_PATH} not found. docker-compose.yml mounts ../canonical ` +
      "from your repository root: copy canonical/ from eai-2026 next to pa6/ " +
      '(root README, "Starting an assignment"), then run docker compose up again.',
  );
}

const ajv = new Ajv({ allErrors: true });
addFormats(ajv);
const validateCanonicalOrder = ajv.compile(JSON.parse(fs.readFileSync(CANONICAL_SCHEMA_PATH, "utf8")));

function validateCheckoutPayload(payload: unknown): string | null {
  if (validateCanonicalOrder(payload)) {
    return null;
  }
  return `Request body is not a canonical order: ${ajv.errorsText(validateCanonicalOrder.errors, { dataVar: "body" })}`;
}

function bootstrapStores(): void {
  ensureJsonFile(IDEMPOTENCY_STORE_PATH, { records: {} });
  ensureJsonFile(SAGA_STORE_PATH, { sagas: {} });
}

app.get("/health", (_req: Request, res: Response) => {
  res.status(200).json({ status: "ok" });
});

app.get("/debug/trace/:orderId", (req: Request, res: Response) => {
  const orderIdParam = req.params.orderId ?? "";
  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  const saga = sagaStore?.sagas?.[orderIdParam];
  if (!saga) {
    res.status(404).json({ code: "not_found", message: "No saga found for this orderId" });
    return;
  }
  res.status(200).json(saga);
});

const MAX_ITEMS = 25; // must match maxItems in openapi.yaml

function toCents(price: string): bigint {
  const negative = price.startsWith("-");
  const [whole = "0", frac = "00"] = price.replace("-", "").split(".");
  const cents = BigInt(whole) * 100n + BigInt(frac.padEnd(2, "0").slice(0, 2));
  return negative ? -cents : cents;
}

function formatCents(cents: bigint): string {
  const negative = cents < 0n;
  const abs = negative ? -cents : cents;
  const frac = (abs % 100n).toString().padStart(2, "0");
  return `${negative ? "-" : ""}${abs / 100n}.${frac}`;
}

function computeAmount(items: CanonicalOrder["items"]): string {
  let total = 0n;
  for (const item of items) {
    total += toCents(String(item.unitPrice)) * BigInt(item.quantity);
  }
  return formatCents(total);
}

interface StepResult {
  ok: boolean;
  timeout: boolean;
  code: string;
}

async function callStep(step: string, url: string, body: unknown, trace: TraceItem[]): Promise<StepResult> {
  const started = Date.now();
  const startedAt = new Date(started).toISOString();
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), config.requestTimeoutMs);
  let result: StepResult;
  try {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
    let code = `${step}_failed`;
    try {
      const parsed = (await res.json()) as { code?: unknown };
      if (res.ok) code = "";
      else if (typeof parsed?.code === "string") code = parsed.code;
    } catch {
      if (res.ok) code = "";
    }
    result = { ok: res.ok, timeout: false, code };
  } catch (err) {
    const aborted = (err as Error)?.name === "AbortError";
    result = { ok: false, timeout: aborted, code: aborted ? "timeout" : `${step}_unreachable` };
  } finally {
    clearTimeout(timer);
  }
  const finished = Date.now();
  trace.push({
    step,
    status: result.ok ? "success" : result.timeout ? "timeout" : "failed",
    startedAt,
    finishedAt: new Date(finished).toISOString(),
    durationMs: finished - started,
  });
  return result;
}

app.post("/checkout", async (req: Request, res: Response) => {
  const idempotencyKey = req.header("Idempotency-Key");
  if (!idempotencyKey) {
    res.status(400).json({ code: "validation_error", message: "Idempotency-Key header is required" });
    return;
  }

  const validationError = validateCheckoutPayload(req.body);
  if (validationError) {
    res.status(400).json({ code: "validation_error", message: validationError });
    return;
  }

  const order = req.body as CanonicalOrder;
  if (order.items.length > MAX_ITEMS) {
    res.status(400).json({
      code: "validation_error",
      message: `Order has ${order.items.length} items; the contract allows at most ${MAX_ITEMS}`,
    });
    return;
  }

  const requestHash = payloadHash(req.body);
  const idempotencyStore = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!idempotencyStore.records) idempotencyStore.records = {};

  const existing = idempotencyStore.records[idempotencyKey];
  if (existing) {
    if (existing.requestHash !== requestHash) {
      res.status(409).json({
        code: "idempotency_payload_mismatch",
        message: "This Idempotency-Key is already used for a different payload",
      });
      return;
    }
    if (existing.state === "in_progress") {
      res.status(409).json({
        code: "idempotency_conflict",
        message: "A request with this Idempotency-Key is still in progress",
      });
      return;
    }
    res.status(existing.httpStatus).json(existing.response);
    return;
  }

  // No await between the check above and this write, so two requests with the
  // same key cannot both pass the check.
  const orderId = order.orderId;
  idempotencyStore.records[idempotencyKey] = {
    requestHash,
    state: "in_progress",
    httpStatus: 202,
    response: { orderId, status: "in_progress" },
    updatedAt: nowIso(),
  };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, idempotencyStore);

  const trace: TraceItem[] = [];
  let state: SagaState = "completed";
  let httpStatus = 200;
  let code: string | undefined;

  try {
    const amount = computeAmount(order.items);
    const forward = [
      { step: "payment", url: `${config.paymentUrl}/payment/authorize`, body: { orderId, amount } },
      { step: "inventory", url: `${config.inventoryUrl}/inventory/reserve`, body: { orderId, items: order.items } },
      { step: "shipping", url: `${config.shippingUrl}/shipping/create`, body: { orderId } },
      { step: "notification", url: `${config.notificationUrl}/notification/send`, body: { orderId, recipient: order.customer.email } },
    ];
    const undo: Record<string, { step: string; url: string }> = {
      inventory: { step: "inventory_release", url: `${config.inventoryUrl}/inventory/release` },
      payment: { step: "payment_refund", url: `${config.paymentUrl}/payment/refund` },
    };

    const completed: string[] = [];
    let failure: StepResult | null = null;
    for (const s of forward) {
      const r = await callStep(s.step, s.url, s.body, trace);
      if (!r.ok) {
        failure = r;
        break;
      }
      completed.push(s.step);
    }

    if (failure) {
      code = failure.code;
      httpStatus = failure.timeout ? 504 : 422;
      state = "failed";
      let compensationFailed = false;
      let compensated = false;
      for (const done of [...completed].reverse()) {
        const u = undo[done];
        if (!u) continue; // shipping and notification have no undo
        compensated = true;
        const r = await callStep(u.step, u.url, { orderId }, trace);
        if (!r.ok) compensationFailed = true;
      }
      if (compensationFailed) {
        state = "failed";
        code = "compensation_failed";
        httpStatus = 422;
      } else if (compensated) {
        state = "compensated";
      }
    }
  } catch (err) {
    console.error("[orchestrator] unexpected error", err);
    state = "failed";
    httpStatus = 422;
    code = "internal_error";
  }

  const response: Record<string, unknown> = { orderId, status: state, trace };
  if (code) response.code = code;

  const sagaStore = readJsonFile<SagaStore>(SAGA_STORE_PATH);
  if (!sagaStore.sagas) sagaStore.sagas = {};
  sagaStore.sagas[orderId] = { idempotencyKey, state, steps: trace, updatedAt: nowIso() };
  writeJsonFile(SAGA_STORE_PATH, sagaStore);

  const store = readJsonFile<IdempotencyStore>(IDEMPOTENCY_STORE_PATH);
  if (!store.records) store.records = {};
  store.records[idempotencyKey] = { requestHash, state, httpStatus, response, updatedAt: nowIso() };
  writeJsonFile(IDEMPOTENCY_STORE_PATH, store);

  res.status(httpStatus).json(response);
});

bootstrapStores();

app.listen(config.port, () => {
  console.log(`[orchestrator] listening on port ${config.port}`);
  console.log("[orchestrator] downstream targets loaded from env", {
    paymentUrl: config.paymentUrl,
    inventoryUrl: config.inventoryUrl,
    shippingUrl: config.shippingUrl,
    notificationUrl: config.notificationUrl,
    requestTimeoutMs: config.requestTimeoutMs,
  });
});
